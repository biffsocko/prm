package server_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/proto"
	"github.com/biffsocko/prm/internal/server"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/sqlite"
)

// e2eFixture stands up an in-memory storage + a real TLS server on a
// random localhost port. Tests dial back in to exercise the full path.
type e2eFixture struct {
	t      *testing.T
	srv    *server.Server
	addr   string
	store  storage.Store
	tenant *storage.Tenant
	pwd    string
	user   *storage.Account
	cancel context.CancelFunc
	wg     sync.WaitGroup
	tlsCfg *tls.Config
}

func newFixture(t *testing.T) *e2eFixture {
	t.Helper()

	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	ten := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(context.Background(), ten); err != nil {
		t.Fatal(err)
	}
	hash, salt, params, _ := auth.HashPassword("hunter2")
	acc := &storage.Account{
		Username:       "alex",
		DisplayName:    "Alex",
		PasswordHash:   hash,
		PasswordSalt:   salt,
		PasswordParams: params,
	}
	if err := s.CreateAccount(context.Background(), ten.ID, acc); err != nil {
		t.Fatal(err)
	}
	// Slice 2: channels are explicit. Create a public #general for the tests.
	if err := s.CreateChannel(context.Background(), ten.ID, &storage.Channel{
		Name: "general", OwnerID: acc.ID, Visibility: storage.ChannelPublic,
	}); err != nil {
		t.Fatal(err)
	}

	// Listen on a random port so tests can run in parallel.
	addr := pickFreeAddr(t)
	tlsCfg, err := server.DevTLSConfig("localhost")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.New(server.Config{
		Addr:      addr,
		TLSConfig: tlsCfg,
		Store:     s,
		Name:      "prmd-test",
		Version:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	f := &e2eFixture{
		t: t, srv: srv, addr: addr, store: s, tenant: ten, user: acc,
		pwd: "hunter2", cancel: cancel,
		tlsCfg: &tls.Config{InsecureSkipVerify: true, ServerName: "localhost"},
	}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		_ = srv.Serve(ctx)
	}()

	// Wait until the server is actually accepting.
	if err := waitDialable(addr, 2*time.Second, f.tlsCfg); err != nil {
		cancel()
		f.wg.Wait()
		t.Fatalf("server didn't come up: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		f.wg.Wait()
	})
	return f
}

func pickFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func waitDialable(addr string, timeout time.Duration, tlsCfg *tls.Config) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := tls.Dial("tcp", addr, tlsCfg)
		if err == nil {
			_ = c.Close()
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("timeout")
}

// client is a small test-side wrapper.
type client struct {
	conn net.Conn
	dec  *proto.Decoder
}

func (f *e2eFixture) dial(t *testing.T) *client {
	t.Helper()
	c, err := tls.Dial("tcp", f.addr, f.tlsCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &client{conn: c, dec: proto.NewDecoder(c)}
}

func (c *client) send(t *testing.T, f proto.Frame) {
	t.Helper()
	if err := proto.Encode(c.conn, f); err != nil {
		t.Fatal(err)
	}
}

// recvType reads frames until one of the wanted types is seen, returning it.
// Discards anything else (such as Ping frames the server may send).
//
// Deadline is generous (10s) because Argon2id during auth takes ~5s under
// the race detector; this needs to cover the slow path comfortably.
func (c *client) recvType(t *testing.T, want ...string) proto.Frame {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer c.conn.SetReadDeadline(time.Time{})
	for {
		f, err := c.dec.Decode()
		if err != nil {
			if err == io.EOF {
				t.Fatal("EOF while waiting for", want)
			}
			t.Fatal(err)
		}
		for _, w := range want {
			if f.FrameType() == w {
				return f
			}
		}
		// echo pings to keep server happy
		if p, ok := f.(proto.Ping); ok {
			_ = proto.Encode(c.conn, proto.Pong{Token: p.Token})
			continue
		}
	}
}

func (f *e2eFixture) login(t *testing.T, c *client) proto.AuthOK {
	t.Helper()
	c.send(t, proto.Hello{ClientName: "test", CapVersion: "0.1"})
	if got := c.recvType(t, proto.TypeWelcome); got.FrameType() != proto.TypeWelcome {
		t.Fatal("expected welcome")
	}
	c.send(t, proto.AuthRequest{Method: proto.AuthMethodPassword, Tenant: f.tenant.Slug, Username: f.user.Username})
	chalF := c.recvType(t, proto.TypeAuthChallenge, proto.TypeAuthErr)
	chal, ok := chalF.(proto.AuthChallenge)
	if !ok {
		t.Fatalf("expected AuthChallenge, got %#v", chalF)
	}
	salt, err := auth.DecodeBase64(chal.Salt)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := auth.ComputeClientProof(f.pwd, salt, chal.Params)
	if err != nil {
		t.Fatal(err)
	}
	c.send(t, proto.AuthResponse{Proof: auth.EncodeBase64(proof)})
	okF := c.recvType(t, proto.TypeAuthOK, proto.TypeAuthErr)
	res, ok := okF.(proto.AuthOK)
	if !ok {
		t.Fatalf("expected AuthOK, got %#v", okF)
	}
	return res
}

func TestServerHelloWelcomeAuthOK(t *testing.T) {
	f := newFixture(t)
	c := f.dial(t)
	ok := f.login(t, c)
	if ok.AccountID != f.user.ID.String() {
		t.Errorf("account id mismatch: got %q want %q", ok.AccountID, f.user.ID.String())
	}
	if ok.DisplayName != f.user.DisplayName {
		t.Errorf("display name mismatch")
	}
}

func TestServerJoinAndBroadcast(t *testing.T) {
	f := newFixture(t)
	a := f.dial(t)
	b := f.dial(t)
	f.login(t, a)
	f.login(t, b)

	a.send(t, proto.Join{Channel: "general"})
	b.send(t, proto.Join{Channel: "general"})

	// Drain presence frames from both clients so the next recv is the actual msg.
	a.recvType(t, proto.TypePresence) // a sees own join
	a.recvType(t, proto.TypePresence) // a sees b join
	b.recvType(t, proto.TypePresence) // b sees own join

	a.send(t, proto.Msg{Channel: "general", Body: "hello"})

	gotA := a.recvType(t, proto.TypeMsg).(proto.Msg)
	gotB := b.recvType(t, proto.TypeMsg).(proto.Msg)

	if gotA.Body != "hello" || gotB.Body != "hello" {
		t.Errorf("body mismatch: a=%q b=%q", gotA.Body, gotB.Body)
	}
	if gotA.From == "" || gotB.From == "" {
		t.Errorf("server should stamp From: a=%q b=%q", gotA.From, gotB.From)
	}
	if gotA.TS.IsZero() || gotB.TS.IsZero() {
		t.Errorf("server should stamp TS")
	}
}

func TestServerRejectsUnauthenticatedAction(t *testing.T) {
	f := newFixture(t)
	c := f.dial(t)
	c.send(t, proto.Hello{CapVersion: "0.1"})
	c.recvType(t, proto.TypeWelcome)
	// Skip auth and try a Join. Should get an error.
	c.send(t, proto.Join{Channel: "general"})
	got := c.recvType(t, proto.TypeError).(proto.Error)
	if got.Reason != "not_authenticated" {
		t.Errorf("expected not_authenticated, got %q", got.Reason)
	}
}

func TestServerCrossTenantIsolation(t *testing.T) {
	// Two clients in different tenants should not see each other's broadcasts
	// even if both join a channel called "general".
	f := newFixture(t)
	// Add a second tenant + account + public #general channel.
	ctx := context.Background()
	t2 := &storage.Tenant{Slug: "globex", DisplayName: "Globex"}
	if err := f.store.CreateTenant(ctx, t2); err != nil {
		t.Fatal(err)
	}
	hash, salt, params, _ := auth.HashPassword("p2")
	a2 := &storage.Account{Username: "bob", DisplayName: "Bob", PasswordHash: hash, PasswordSalt: salt, PasswordParams: params}
	if err := f.store.CreateAccount(ctx, t2.ID, a2); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CreateChannel(ctx, t2.ID, &storage.Channel{
		Name: "general", OwnerID: a2.ID, Visibility: storage.ChannelPublic,
	}); err != nil {
		t.Fatal(err)
	}

	alex := f.dial(t)
	bob := f.dial(t)
	f.login(t, alex)
	// Manually log bob in to tenant globex
	bob.send(t, proto.Hello{CapVersion: "0.1"})
	bob.recvType(t, proto.TypeWelcome)
	bob.send(t, proto.AuthRequest{Method: proto.AuthMethodPassword, Tenant: "globex", Username: "bob"})
	chal := bob.recvType(t, proto.TypeAuthChallenge).(proto.AuthChallenge)
	salt2, _ := auth.DecodeBase64(chal.Salt)
	proof2, _ := auth.ComputeClientProof("p2", salt2, chal.Params)
	bob.send(t, proto.AuthResponse{Proof: auth.EncodeBase64(proof2)})
	bob.recvType(t, proto.TypeAuthOK)

	alex.send(t, proto.Join{Channel: "general"})
	alex.recvType(t, proto.TypePresence)
	bob.send(t, proto.Join{Channel: "general"})
	bob.recvType(t, proto.TypePresence)

	alex.send(t, proto.Msg{Channel: "general", Body: "alex-says-hi"})

	// Alex should see her own broadcast.
	gotAlex := alex.recvType(t, proto.TypeMsg).(proto.Msg)
	if gotAlex.Body != "alex-says-hi" {
		t.Fatal("alex didn't get her own msg")
	}

	// Bob should NOT see it. Set a short read deadline and expect timeout.
	_ = bob.conn.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	defer bob.conn.SetReadDeadline(time.Time{})
	for {
		f, err := bob.dec.Decode()
		if err != nil {
			// Expected: read deadline timeout or EOF. Pass.
			return
		}
		if m, ok := f.(proto.Msg); ok && m.Body == "alex-says-hi" {
			t.Fatalf("cross-tenant leak: bob received alex's message: %#v", m)
		}
	}
}
