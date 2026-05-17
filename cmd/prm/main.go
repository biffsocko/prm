// Command prm is the PRM reference TUI client.
//
// Usage:
//
//	prm [flags] <server-addr> <tenant> <username> <channel>
//
// Example:
//
//	prm --insecure localhost:6697 acme alex general
//
// The password is read from the terminal (or PRM_PASSWORD env var for
// non-interactive testing).
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	"github.com/charmbracelet/lipgloss"
	"golang.org/x/term"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/proto"
)

func main() {
	fs := flag.NewFlagSet("prm", flag.ExitOnError)
	insecure := fs.Bool("insecure", false, "skip TLS verification (for dev self-signed certs)")
	token := fs.String("token", "", "use token-method auth instead of password (overrides password)")
	displayName := fs.String("display-name", "", "(unused in slice 1; reserved)")
	_ = displayName
	_ = fs.Parse(os.Args[1:])

	if fs.NArg() != 4 {
		fmt.Fprintln(os.Stderr, "usage: prm [--insecure] [--token TOKEN] <server-addr> <tenant> <username> <channel>")
		os.Exit(2)
	}
	addr := fs.Arg(0)
	tenant := fs.Arg(1)
	username := fs.Arg(2)
	channel := fs.Arg(3)

	creds := credentials{
		addr:     addr,
		tenant:   tenant,
		username: username,
		channel:  channel,
		token:    *token,
		insecure: *insecure,
	}
	if creds.token == "" {
		password := os.Getenv("PRM_PASSWORD")
		if password == "" {
			fmt.Printf("Password for %s@%s: ", username, tenant)
			pw, err := term.ReadPassword(int(os.Stdin.Fd()))
			fmt.Println()
			if err != nil {
				fmt.Fprintln(os.Stderr, "read password:", err)
				os.Exit(1)
			}
			password = string(pw)
		}
		creds.password = password
	}

	cli, err := dialAndAuth(creds)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}

	model := newModel(cli, creds)
	p := tea.NewProgram(model, tea.WithAltScreen())
	model.prog = p

	// Run a reader goroutine that pushes incoming frames as tea messages.
	go cli.pumpInto(p)

	if _, err := p.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "tui:", err)
		os.Exit(1)
	}
	cli.close()
}

// credentials carries everything needed to (re)establish a session.
type credentials struct {
	addr     string
	tenant   string
	username string
	channel  string
	password string // empty if using token
	token    string // empty if using password
	insecure bool
}

func (c credentials) tlsConfig() *tls.Config {
	return &tls.Config{
		ServerName:         hostFromAddr(c.addr),
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: c.insecure,
	}
}

func hostFromAddr(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}

// ---------- client wrapper ----------

type prmClient struct {
	conn      net.Conn
	dec       *proto.Decoder
	mu        sync.Mutex // serializes writes
	accountID string

	closed chan struct{}
}

func dialAndAuth(creds credentials) (*prmClient, error) {
	d := &net.Dialer{Timeout: 5 * time.Second}
	c, err := tls.DialWithDialer(d, "tcp", creds.addr, creds.tlsConfig())
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	cli := &prmClient{conn: c, dec: proto.NewDecoder(c), closed: make(chan struct{})}

	if err := cli.send(proto.Hello{ClientName: "prm-tui", ClientVersion: "0.1.0", CapVersion: "0.1"}); err != nil {
		return nil, err
	}
	if err := cli.expectType(proto.TypeWelcome); err != nil {
		return nil, err
	}

	if creds.token != "" {
		// Token-method auth: one-shot, no challenge.
		if err := cli.send(proto.AuthRequest{Method: proto.AuthMethodToken, Token: creds.token}); err != nil {
			return nil, err
		}
	} else {
		// Password-method auth: 3-frame handshake.
		if err := cli.send(proto.AuthRequest{Method: proto.AuthMethodPassword, Tenant: creds.tenant, Username: creds.username}); err != nil {
			return nil, err
		}
		chalF, err := cli.expectAny(proto.TypeAuthChallenge, proto.TypeAuthErr)
		if err != nil {
			return nil, err
		}
		if errFrame, ok := chalF.(proto.AuthErr); ok {
			return nil, fmt.Errorf("auth failed: %s %s", errFrame.Reason, errFrame.Detail)
		}
		chal := chalF.(proto.AuthChallenge)
		saltBytes, err := auth.DecodeBase64(chal.Salt)
		if err != nil {
			return nil, fmt.Errorf("bad challenge salt: %w", err)
		}
		proof, err := auth.ComputeClientProof(creds.password, saltBytes, chal.Params)
		if err != nil {
			return nil, fmt.Errorf("compute proof: %w", err)
		}
		if err := cli.send(proto.AuthResponse{Proof: auth.EncodeBase64(proof)}); err != nil {
			return nil, err
		}
	}

	okF, err := cli.expectAny(proto.TypeAuthOK, proto.TypeAuthErr)
	if err != nil {
		return nil, err
	}
	if errFrame, ok := okF.(proto.AuthErr); ok {
		return nil, fmt.Errorf("auth failed: %s %s", errFrame.Reason, errFrame.Detail)
	}
	ok := okF.(proto.AuthOK)
	cli.accountID = ok.AccountID
	return cli, nil
}

func (c *prmClient) send(f proto.Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return proto.Encode(c.conn, f)
}

func (c *prmClient) expectType(want string) error {
	f, err := c.dec.Decode()
	if err != nil {
		return err
	}
	if f.FrameType() != want {
		return fmt.Errorf("unexpected frame: got %q want %q", f.FrameType(), want)
	}
	return nil
}

func (c *prmClient) expectAny(want ...string) (proto.Frame, error) {
	f, err := c.dec.Decode()
	if err != nil {
		return nil, err
	}
	for _, w := range want {
		if f.FrameType() == w {
			return f, nil
		}
	}
	return nil, fmt.Errorf("unexpected frame: got %q want one of %v", f.FrameType(), want)
}

func (c *prmClient) close() {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	_ = c.conn.Close()
}

// pumpInto reads frames continuously and pushes them as tea messages.
// It also auto-replies to Pings so the connection stays alive.
func (c *prmClient) pumpInto(p *tea.Program) {
	for {
		f, err := c.dec.Decode()
		if err != nil {
			p.Send(disconnectMsg{err: err})
			return
		}
		switch v := f.(type) {
		case proto.Ping:
			_ = c.send(proto.Pong{Token: v.Token})
		case proto.Msg:
			p.Send(chatMsg{from: v.From, body: v.Body, ts: v.TS})
		case proto.Presence:
			p.Send(presenceMsg{kind: v.Kind, displayName: v.DisplayName, accountID: v.AccountID})
		case proto.Error:
			p.Send(serverErrorMsg{reason: v.Reason, detail: v.Detail})
		}
	}
}

// ---------- tea messages ----------

type chatMsg struct {
	from string
	body string
	ts   time.Time
}

type presenceMsg struct {
	kind        string
	displayName string
	accountID   string
}

type serverErrorMsg struct {
	reason string
	detail string
}

type disconnectMsg struct{ err error }

// ---------- tea model ----------

type model struct {
	cli       *prmClient
	creds     credentials
	myName    string
	input     textinput.Model
	view      viewport.Model
	lines     []string
	width     int
	height    int
	err       error
	joined    bool

	// Reconnect bookkeeping.
	reconnecting bool
	reconnectN   int // attempt number, starts at 1
	prog         *tea.Program // set after newProgram so reconnect goroutines can Send
}

var (
	systemStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("242"))
	selfStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("39")).Bold(true)
	otherStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	tsStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	errStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Bold(true)
)

func newModel(c *prmClient, creds credentials) *model {
	ti := textinput.New()
	ti.Placeholder = "type a message and press Enter (or /quit)"
	ti.Focus()
	ti.CharLimit = 4096
	vp := viewport.New(80, 20)
	return &model{cli: c, creds: creds, myName: creds.username, input: ti, view: vp}
}

func (m *model) Init() tea.Cmd {
	// Send Join on first frame. Returned as a tea.Cmd so it happens after the
	// program starts and any size events have settled.
	return func() tea.Msg {
		_ = m.cli.send(proto.Join{Channel: m.creds.channel})
		return joinedMsg{}
	}
}

type joinedMsg struct{}

// reconnectedMsg is sent when a reconnect attempt succeeds.
type reconnectedMsg struct{ cli *prmClient }

// reconnectFailedMsg is sent when a reconnect attempt fails; carries the
// next backoff so the model can schedule a retry.
type reconnectFailedMsg struct {
	attempt int
	err     error
	nextIn  time.Duration
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.view.Width = msg.Width
		m.view.Height = msg.Height - 3 // leave room for input + status
		m.input.Width = msg.Width
		m.view.SetContent(strings.Join(m.lines, "\n"))
		m.view.GotoBottom()

	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC, tea.KeyEsc:
			return m, tea.Quit
		case tea.KeyEnter:
			text := strings.TrimSpace(m.input.Value())
			m.input.SetValue("")
			if text == "/quit" || text == "/exit" {
				return m, tea.Quit
			}
			if text != "" {
				if err := m.cli.send(proto.Msg{Channel: m.creds.channel, Body: text}); err != nil {
					m.appendLine(errStyle.Render("send: " + err.Error()))
				}
			}
		}

	case joinedMsg:
		m.joined = true
		m.appendLine(systemStyle.Render(fmt.Sprintf("** joined #%s **", m.creds.channel)))

	case chatMsg:
		ts := msg.ts.Local().Format("15:04:05")
		name := msg.from
		if name == m.cli.accountID {
			name = m.myName
		}
		style := otherStyle
		if msg.from == m.cli.accountID {
			style = selfStyle
		}
		m.appendLine(fmt.Sprintf("%s %s: %s",
			tsStyle.Render(ts),
			style.Render(displayShort(name)),
			msg.body))

	case presenceMsg:
		kind := "joined"
		if msg.kind == proto.PresencePart {
			kind = "left"
		}
		who := msg.displayName
		if who == "" {
			who = displayShort(msg.accountID)
		}
		m.appendLine(systemStyle.Render(fmt.Sprintf("** %s %s **", who, kind)))

	case serverErrorMsg:
		m.appendLine(errStyle.Render(fmt.Sprintf("server error: %s %s", msg.reason, msg.detail)))

	case disconnectMsg:
		// Don't quit on disconnect -- start reconnect with backoff.
		if m.reconnecting {
			break
		}
		m.reconnecting = true
		m.reconnectN = 1
		m.appendLine(errStyle.Render("disconnected: " + msg.err.Error() + " (reconnecting...)"))
		return m, m.scheduleReconnect(0)

	case reconnectFailedMsg:
		m.appendLine(errStyle.Render(fmt.Sprintf("reconnect attempt %d failed: %v -- retrying in %s",
			msg.attempt, msg.err, msg.nextIn)))
		m.reconnectN = msg.attempt + 1
		return m, m.scheduleReconnect(msg.nextIn)

	case reconnectedMsg:
		m.cli = msg.cli
		m.reconnecting = false
		m.reconnectN = 0
		m.appendLine(systemStyle.Render(fmt.Sprintf("** reconnected; rejoining #%s **", m.creds.channel)))
		go m.cli.pumpInto(m.prog)
		// Re-join the channel after reconnect.
		return m, func() tea.Msg {
			_ = m.cli.send(proto.Join{Channel: m.creds.channel})
			return nil
		}
	}

	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	cmds = append(cmds, cmd)
	m.view, cmd = m.view.Update(msg)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

// scheduleReconnect returns a tea.Cmd that sleeps for delay, then attempts a
// new dialAndAuth. On success emits reconnectedMsg; on failure emits
// reconnectFailedMsg with the next backoff.
func (m *model) scheduleReconnect(delay time.Duration) tea.Cmd {
	attempt := m.reconnectN
	creds := m.creds
	return func() tea.Msg {
		if delay > 0 {
			time.Sleep(delay)
		}
		cli, err := dialAndAuth(creds)
		if err != nil {
			return reconnectFailedMsg{
				attempt: attempt,
				err:     err,
				nextIn:  backoffFor(attempt),
			}
		}
		return reconnectedMsg{cli: cli}
	}
}

// backoffFor returns the delay before the next reconnect attempt.
// Exponential with a 30s cap: 1s, 2s, 4s, 8s, 16s, 30s, 30s, ...
func backoffFor(attempt int) time.Duration {
	d := time.Second << attempt // 1s, 2s, 4s, 8s, 16s, 32s, ...
	if d > 30*time.Second || d < 0 {
		return 30 * time.Second
	}
	return d
}

func (m *model) appendLine(line string) {
	m.lines = append(m.lines, line)
	if len(m.lines) > 2000 {
		m.lines = m.lines[len(m.lines)-2000:]
	}
	m.view.SetContent(strings.Join(m.lines, "\n"))
	m.view.GotoBottom()
}

func (m *model) View() string {
	connState := ""
	if m.reconnecting {
		connState = errStyle.Render(fmt.Sprintf(" -- RECONNECTING (attempt %d)", m.reconnectN))
	}
	status := systemStyle.Render(fmt.Sprintf("#%s -- %s -- Ctrl-C to quit", m.creds.channel, m.myName)) + connState
	return fmt.Sprintf("%s\n%s\n%s", m.view.View(), m.input.View(), status)
}

func displayShort(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
