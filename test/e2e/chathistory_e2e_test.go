package e2e_test

import (
	"context"
	"crypto/tls"
	"sync"
	"testing"
	"time"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/proto"
	"github.com/biffsocko/prm/internal/server"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/sqlite"
)

// TestEndToEndChatHistory proves slice 5's history persistence + the
// chathistory retrieval verb work together: send three messages over
// a real TLS connection, briefly wait for the async writer to drain,
// then issue a chathistory request and verify the messages come back
// oldest-first.
func TestEndToEndChatHistory(t *testing.T) {
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tenant := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	hash, salt, params, _ := auth.HashPassword("hunter2")
	alex := &storage.Account{
		Username: "alex", PasswordHash: hash, PasswordSalt: salt, PasswordParams: params,
	}
	if err := s.CreateAccount(ctx, tenant.ID, alex); err != nil {
		t.Fatal(err)
	}
	ch := &storage.Channel{Name: "ops", OwnerID: alex.ID, Visibility: storage.ChannelPublic}
	if err := s.CreateChannel(ctx, tenant.ID, ch); err != nil {
		t.Fatal(err)
	}

	tlsCfg, err := server.DevTLSConfig("localhost")
	if err != nil {
		t.Fatal(err)
	}
	addr := pickFreeAddr(t)
	rt, err := server.New(server.Config{
		Addr: addr, TLSConfig: tlsCfg, Store: s,
		Name: "prmd-e2e", Version: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = rt.Serve(runCtx) }()
	if err := waitDialable(addr, 3*time.Second); err != nil {
		cancel()
		wg.Wait()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); wg.Wait() })

	clientTLS := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost"}
	c, err := tls.Dial("tcp", addr, clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	dec := proto.NewDecoder(c)
	_ = proto.Encode(c, proto.Hello{CapVersion: "0.1"})
	_, _ = dec.Decode()
	_ = proto.Encode(c, proto.AuthRequest{Method: proto.AuthMethodPassword, Tenant: "acme", Username: "alex"})
	chalF, _ := dec.Decode()
	chal := chalF.(proto.AuthChallenge)
	saltBytes, _ := auth.DecodeBase64(chal.Salt)
	proof, _ := auth.ComputeClientProof("hunter2", saltBytes, chal.Params)
	_ = proto.Encode(c, proto.AuthResponse{Proof: auth.EncodeBase64(proof)})
	_, _ = dec.Decode()
	_ = proto.Encode(c, proto.Join{Channel: "ops"})
	_, _ = dec.Decode() // own presence

	// Send three messages with small spacing so timestamps order cleanly.
	bodies := []string{"first", "second", "third"}
	for _, b := range bodies {
		_ = proto.Encode(c, proto.Msg{Channel: "ops", Body: b})
		_, _ = dec.Decode() // echo
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for the async history writer to drain. Poll storage.
	deadline := time.Now().Add(3 * time.Second)
	var got []*storage.StoredMessage
	for time.Now().Before(deadline) {
		got, _ = s.ListMessages(ctx, tenant.ID, ch.ID, 10, time.Time{})
		if len(got) >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 persisted messages, got %d", len(got))
	}

	// Issue chathistory verb; verify response.
	_ = proto.Encode(c, proto.ChatHistory{Channel: "ops", Limit: 10})
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer c.SetReadDeadline(time.Time{})
	for {
		f, err := dec.Decode()
		if err != nil {
			t.Fatal(err)
		}
		resp, ok := f.(proto.ChatHistoryOK)
		if !ok {
			continue // skip pings etc.
		}
		if resp.Channel != "ops" {
			t.Errorf("channel mismatch: %q", resp.Channel)
		}
		if len(resp.Messages) != 3 {
			t.Fatalf("expected 3 in response, got %d", len(resp.Messages))
		}
		for i, want := range bodies {
			if resp.Messages[i].Body != want {
				t.Errorf("msg %d: got %q want %q", i, resp.Messages[i].Body, want)
			}
		}
		return
	}
}
