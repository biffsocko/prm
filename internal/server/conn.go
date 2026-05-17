package server

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/channels"
	"github.com/biffsocko/prm/internal/proto"
)

// silence unused-import lint when channels.Member is only referenced via
// interface satisfaction below.
var _ channels.Member = (*Conn)(nil)

// outboundQueueSize is the per-connection outbound buffer (in frames).
// Under sustained backpressure, additional frames are dropped and the
// connection is tagged as lagging.
const outboundQueueSize = 1024

// laggingThreshold is the number of consecutive drops after which a
// connection is forcibly closed.
const laggingThreshold = 256

// pingInterval is how often the server sends a Ping. The client must
// respond with a Pong within (pingInterval + pingTimeout) or be disconnected.
const (
	pingInterval = 30 * time.Second
	pingTimeout  = 30 * time.Second
)

// Conn represents one client connection. It implements channels.Member so
// the channel registry can enqueue broadcast frames onto it.
type Conn struct {
	srv  *Server
	raw  net.Conn          // typically *tls.Conn
	dec  *proto.Decoder
	w    *bufio.Writer     // wraps raw for the write goroutine
	wmu  sync.Mutex        // serializes Flush + Write since we may call from multiple goroutines (ping)

	id          uuid.UUID
	tenantID    uuid.UUID
	accountID   uuid.UUID
	displayName string
	authed      atomic.Bool

	out      chan []byte
	closed   atomic.Bool
	closeOnce sync.Once

	lastPongToken atomic.Value // string
	pongCh        chan string

	dropCount int64 // atomic

	// pendingChal is set during the password-auth handshake between
	// AuthRequest (server returns AuthChallenge) and AuthResponse
	// (server validates + returns AuthOK/AuthErr). nil at all other times.
	// Accessed only from the read goroutine.
	pendingChal *auth.Challenge

	// joinedChannels caches (channel_name -> channel_id) for channels this
	// connection has successfully joined. Looked up by handleMsg and
	// handlePart so the hot path doesn't touch storage. Accessed only from
	// the read goroutine (single-threaded per conn), so no lock needed.
	joinedChannels map[string]uuid.UUID

	log *slog.Logger
}

// newConn wraps a freshly-accepted net.Conn.
func newConn(srv *Server, raw net.Conn) *Conn {
	id := uuid.Must(uuid.NewV7())
	return &Conn{
		srv:            srv,
		raw:            raw,
		dec:            proto.NewDecoder(raw),
		w:              bufio.NewWriterSize(raw, 8*1024),
		id:             id,
		out:            make(chan []byte, outboundQueueSize),
		pongCh:         make(chan string, 4),
		joinedChannels: make(map[string]uuid.UUID, 4),
		log:            srv.log.With("conn", id.String()[:8]),
	}
}

// --- channels.Member implementation ---

func (c *Conn) ConnID() uuid.UUID    { return c.id }
func (c *Conn) AccountID() uuid.UUID { return c.accountID }
func (c *Conn) DisplayName() string  { return c.displayName }

// Enqueue is the hot-path fan-out target. Nonblocking: if the outbound
// queue is full, drop the frame and bump the drop counter. The write
// goroutine notices the drop count crossing laggingThreshold and closes
// the connection.
func (c *Conn) Enqueue(frame []byte) {
	if c.closed.Load() {
		return
	}
	select {
	case c.out <- frame:
	default:
		atomic.AddInt64(&c.dropCount, 1)
	}
}

// --- lifecycle ---

// serve runs the read goroutine + spawns the write goroutine + the ping
// goroutine. Blocks until the connection closes. Caller cleans up after.
func (c *Conn) serve(ctx context.Context) {
	defer c.close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); c.writeLoop(ctx) }()
	go func() { defer wg.Done(); c.pingLoop(ctx) }()

	// Close the underlying conn when ctx cancels. Without this, the read
	// loop's blocking Decode() never wakes up on shutdown and serve()
	// deadlocks. The close itself is idempotent via closeOnce.
	go func() {
		<-ctx.Done()
		c.closed.Store(true)
		_ = c.raw.Close()
	}()

	// Read loop runs on the goroutine that called serve; when it returns,
	// we cancel the ctx and wait for the write/ping goroutines.
	c.readLoop(ctx)
	cancel()
	wg.Wait()
}

func (c *Conn) close() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		_ = c.raw.Close()
		// Remove from all channels on the way out.
		c.srv.channels.RemoveMemberFromAll(c.id)
		c.log.Info("connection closed")
	})
}

// --- read loop ---

func (c *Conn) readLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		frame, err := c.dec.Decode()
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.log.Info("client closed")
			} else {
				c.log.Info("read error", "err", err)
			}
			return
		}
		c.dispatch(ctx, frame)
	}
}

// dispatch routes a single inbound frame.
func (c *Conn) dispatch(ctx context.Context, f proto.Frame) {
	// Hello / Auth pre-auth path
	if !c.authed.Load() {
		switch v := f.(type) {
		case proto.Hello:
			c.handleHello(v)
		case proto.AuthRequest:
			c.handleAuthRequest(ctx, v)
		case proto.AuthResponse:
			c.handleAuthResponse(ctx, v)
		case proto.Pong:
			c.handlePong(v)
		case proto.Ping:
			c.handlePing(v)
		default:
			c.sendError("not_authenticated", "complete auth before sending other frames", "")
		}
		return
	}

	// Post-auth dispatch
	switch v := f.(type) {
	case proto.Join:
		c.handleJoin(ctx, v)
	case proto.Part:
		c.handlePart(ctx, v)
	case proto.Msg:
		c.handleMsg(ctx, v)
	case proto.Ping:
		c.handlePing(v)
	case proto.Pong:
		c.handlePong(v)
	default:
		c.sendError("unsupported", fmt.Sprintf("verb %q not supported in slice 1", f.FrameType()), "")
	}
}

// --- write loop ---

func (c *Conn) writeLoop(ctx context.Context) {
	flushTicker := time.NewTicker(5 * time.Millisecond)
	defer flushTicker.Stop()
	dirty := false

	flush := func() {
		if !dirty {
			return
		}
		c.wmu.Lock()
		_ = c.w.Flush()
		c.wmu.Unlock()
		dirty = false
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case bytes, ok := <-c.out:
			if !ok {
				flush()
				return
			}
			c.wmu.Lock()
			if _, err := c.w.Write(bytes); err != nil {
				c.wmu.Unlock()
				c.log.Info("write error", "err", err)
				return
			}
			c.wmu.Unlock()
			dirty = true
			// If many small writes pile up, flush right away to keep
			// latency tight. The ticker is a safety net for when the
			// channel goes quiet mid-flush.
			if len(c.out) == 0 {
				flush()
			}
			if atomic.LoadInt64(&c.dropCount) > laggingThreshold {
				c.log.Warn("slow consumer; closing connection", "drops", atomic.LoadInt64(&c.dropCount))
				return
			}
		case <-flushTicker.C:
			flush()
		}
	}
}

// --- ping loop ---

func (c *Conn) pingLoop(ctx context.Context) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tok := randToken(8)
			c.lastPongToken.Store(tok)
			c.sendFrame(proto.Ping{Token: tok})
			select {
			case got := <-c.pongCh:
				if got != tok {
					c.log.Warn("pong token mismatch; closing", "want", tok, "got", got)
					return
				}
			case <-time.After(pingTimeout):
				c.log.Warn("pong timeout; closing")
				return
			case <-ctx.Done():
				return
			}
		}
	}
}

func (c *Conn) handlePong(p proto.Pong) {
	select {
	case c.pongCh <- p.Token:
	default:
		// ping loop might not be reading; drop is OK
	}
}

func (c *Conn) handlePing(p proto.Ping) {
	// Echo back as a Pong.
	c.sendFrame(proto.Pong{Token: p.Token, ID: p.ID})
}

// --- hello / welcome / auth handlers ---

func (c *Conn) handleHello(h proto.Hello) {
	c.sendFrame(proto.Welcome{
		ID:            h.ID,
		ServerName:    c.srv.name,
		ServerVersion: c.srv.version,
		CapVersion:    "0.1",
		Capabilities:  []string{"presence", "ping"},
	})
}

func (c *Conn) handleAuthRequest(ctx context.Context, r proto.AuthRequest) {
	if r.Method != proto.AuthMethodPassword {
		// Token method comes in slice 2.
		c.sendFrame(proto.AuthErr{ID: r.ID, Reason: "unsupported_method", Detail: "only password method is supported in slice 1"})
		return
	}
	if r.Tenant == "" || r.Username == "" {
		c.sendFrame(proto.AuthErr{ID: r.ID, Reason: "invalid_request", Detail: "tenant and username are required for password method"})
		return
	}
	chal, _, err := c.srv.beginPasswordAuth(ctx, r.Tenant, r.Username)
	if err != nil {
		// Map errors to wire-safe reasons. Don't distinguish "no such user"
		// from "bad password"; both surface as invalid_credentials.
		reason := authErrorReason(err)
		c.sendFrame(proto.AuthErr{ID: r.ID, Reason: reason})
		return
	}
	// Stash the in-flight challenge on the conn for the matching response.
	c.pendingChal = chal
	c.sendFrame(proto.AuthChallenge{
		ID:     r.ID,
		Salt:   base64encode(chal.Salt),
		Nonce:  base64encode(chal.Nonce),
		Params: chal.Params,
	})
}

func (c *Conn) handleAuthResponse(ctx context.Context, r proto.AuthResponse) {
	if c.pendingChal == nil {
		c.sendFrame(proto.AuthErr{ID: r.ID, Reason: "no_challenge", Detail: "send AuthRequest before AuthResponse"})
		return
	}
	res, err := c.srv.completePasswordAuth(ctx, c.pendingChal, r.Proof)
	c.pendingChal = nil
	if err != nil {
		c.log.Error("complete password auth failed", "err", err)
		c.sendFrame(proto.AuthErr{ID: r.ID, Reason: "internal"})
		return
	}
	if !res.OK {
		c.sendFrame(proto.AuthErr{ID: r.ID, Reason: res.Reason})
		return
	}
	c.tenantID = res.Tenant.ID
	c.accountID = res.Account.ID
	c.displayName = res.Account.DisplayName
	c.authed.Store(true)
	c.log = c.log.With("tenant", res.Tenant.Slug, "account", res.Account.Username)
	c.sendFrame(proto.AuthOK{
		ID:          r.ID,
		AccountID:   res.Account.ID.String(),
		TenantID:    res.Tenant.ID.String(),
		AccountType: string(res.Account.Type),
		DisplayName: res.Account.DisplayName,
	})
	c.log.Info("authenticated")
}

// --- channel handlers ---

func (c *Conn) handleJoin(ctx context.Context, j proto.Join) {
	if j.Channel == "" {
		c.sendError("invalid_request", "channel is required", j.ID)
		return
	}
	// Look up the persisted channel by (tenant, name). Channels must be
	// explicitly created (no implicit creation on first JOIN) -- that's a
	// slice 2 change from slice 1's name-derived deterministic UUIDs.
	channel, err := c.srv.store.GetChannelByName(ctx, c.tenantID, j.Channel)
	if err != nil {
		c.sendError("channel_not_found", "no such channel in this tenant", j.ID)
		return
	}
	// ACL enforcement.
	allowed, reason := c.srv.canJoin(ctx, c.tenantID, channel, c.accountID)
	if !allowed {
		c.sendError(reason, "not permitted to join this channel", j.ID)
		return
	}
	// Cache the channel id on the connection for fast lookup in
	// handlePart / handleMsg.
	c.joinedChannels[j.Channel] = channel.ID

	ch := c.srv.channels.GetOrCreate(c.tenantID, channel.ID, j.Channel)
	added := ch.AddMember(c)
	if added {
		presence, _ := proto.EncodeBytes(proto.Presence{
			Channel:     j.Channel,
			Kind:        proto.PresenceJoin,
			AccountID:   c.accountID.String(),
			DisplayName: c.displayName,
		})
		ch.Broadcast(presence)
	}
}

func (c *Conn) handlePart(ctx context.Context, p proto.Part) {
	if p.Channel == "" {
		c.sendError("invalid_request", "channel is required", p.ID)
		return
	}
	chanID, ok := c.joinedChannels[p.Channel]
	if !ok {
		// Not joined; nothing to do.
		return
	}
	delete(c.joinedChannels, p.Channel)
	ch := c.srv.channels.Get(c.tenantID, chanID)
	if ch == nil {
		return
	}
	removed := ch.RemoveMember(c.id)
	if removed {
		presence, _ := proto.EncodeBytes(proto.Presence{
			Channel:     p.Channel,
			Kind:        proto.PresencePart,
			AccountID:   c.accountID.String(),
			DisplayName: c.displayName,
		})
		ch.Broadcast(presence)
	}
}

func (c *Conn) handleMsg(ctx context.Context, m proto.Msg) {
	if m.Channel == "" {
		c.sendError("invalid_request", "channel is required", m.ID)
		return
	}
	if m.Body == "" {
		return
	}
	// Hot path: look up channel by name in the per-connection cache (no
	// storage hit, no shared lock). If the user isn't joined, refuse.
	chanID, ok := c.joinedChannels[m.Channel]
	if !ok {
		c.sendError("not_in_channel", "join the channel before sending", m.ID)
		return
	}
	ch := c.srv.channels.Get(c.tenantID, chanID)
	if ch == nil {
		delete(c.joinedChannels, m.Channel)
		c.sendError("not_in_channel", "channel no longer in memory; rejoin", m.ID)
		return
	}
	// Server-stamp From + TS, encode the broadcast frame once, fan out.
	out := proto.Msg{
		Channel: m.Channel,
		From:    c.accountID.String(),
		TS:      time.Now().UTC(),
		Body:    m.Body,
	}
	bytes, err := proto.EncodeBytes(out)
	if err != nil {
		c.log.Error("encode broadcast msg", "err", err)
		c.sendError("internal", "", m.ID)
		return
	}
	ch.Broadcast(bytes)
}

// --- send helpers ---

func (c *Conn) sendFrame(f proto.Frame) {
	bytes, err := proto.EncodeBytes(f)
	if err != nil {
		c.log.Error("encode frame", "type", f.FrameType(), "err", err)
		return
	}
	c.Enqueue(bytes)
}

func (c *Conn) sendError(reason, detail, id string) {
	c.sendFrame(proto.Error{ID: id, Reason: reason, Detail: detail})
}

// --- misc ---

func randToken(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
