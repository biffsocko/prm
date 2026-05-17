// Package bench runs the fan-out latency benchmark for PRM.
//
// Goal: validate the sub-ms p50 target for the hot path (one msg arriving
// on one connection -> the same bytes written to all subscribed connections
// on the same node).
//
// Run:
//
//	go test ./test/bench/ -run TestFanoutLatency -v
//	go test ./test/bench/ -bench BenchmarkFanout -benchtime=3s -count=3
package bench_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/proto"
	"github.com/biffsocko/prm/internal/server"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/sqlite"
)

const benchTenant = "bench"
const benchChannel = "general"

// fixture stands up an in-process prmd + N pre-authenticated clients all
// joined to one channel. Returns the sender client and the receiver
// clients.
type fixture struct {
	addr    string
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	sender  *benchClient
	receivers []*benchClient
}

func setup(tb testing.TB, numClients int) *fixture {
	tb.Helper()
	st, err := sqlite.Open(":memory:")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(context.Background()); err != nil {
		tb.Fatal(err)
	}

	ten := &storage.Tenant{Slug: benchTenant, DisplayName: "Bench"}
	if err := st.CreateTenant(context.Background(), ten); err != nil {
		tb.Fatal(err)
	}
	// Single shared bench account: all clients log in as the same account
	// (PRM allows N concurrent connections per account). This avoids the
	// O(N) Argon2id setup cost so the benchmark measures the fan-out path,
	// not the auth path.
	hash, salt, params, _ := auth.HashPassword("pw")
	acc := &storage.Account{
		Username:       "bench",
		DisplayName:    "Bench",
		PasswordHash:   hash, PasswordSalt: salt, PasswordParams: params,
	}
	if err := st.CreateAccount(context.Background(), ten.ID, acc); err != nil {
		tb.Fatal(err)
	}
	// Pre-compute the client-side Argon2id proof once and reuse it for every
	// dial -- the salt/params are the same for the shared account. This
	// turns 100 dials' worth of Argon2id (~8s + 6GB memory) into one
	// (~80ms + 64MB).
	preProof, err := auth.ComputeClientProof("pw", salt, params)
	if err != nil {
		tb.Fatal(err)
	}
	benchProofB64 := auth.EncodeBase64(preProof)

	addr := pickFreeAddr(tb)
	tlsCfg, err := server.DevTLSConfig("localhost")
	if err != nil {
		tb.Fatal(err)
	}
	srv, err := server.New(server.Config{Addr: addr, TLSConfig: tlsCfg, Store: st, Name: "prmd-bench"})
	if err != nil {
		tb.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	f := &fixture{addr: addr, cancel: cancel}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		_ = srv.Serve(ctx)
	}()
	if err := waitDialable(addr, 2*time.Second); err != nil {
		cancel()
		f.wg.Wait()
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		cancel()
		f.wg.Wait()
	})

	// Dial sequentially -- the per-conn auth round-trip is fast (no per-dial
	// Argon2id, since we pre-computed the proof), and serial dial avoids
	// the TLS-handshake-storm + memory-pressure issues that bit earlier.
	cls := make([]*benchClient, numClients)
	for i := 0; i < numClients; i++ {
		c, err := dialBench(addr, benchTenant, "bench", benchProofB64)
		if err != nil {
			tb.Fatalf("client setup [%d/%d]: %v", i, numClients, err)
		}
		if err := c.send(proto.Join{Channel: benchChannel}); err != nil {
			tb.Fatalf("client join [%d/%d]: %v", i, numClients, err)
		}
		cls[i] = c
	}

	// We deliberately do NOT drain the presence backlog here. Each runRound
	// goroutine reads frames in a loop and ignores anything that isn't a
	// Msg, so leftover Presence frames are harmless. Latency is measured
	// from the t0 stamped right before the send, so any pre-Msg backlog
	// consumed by the receiver isn't included in the timing.
	//
	// (An earlier version used read deadlines to drain, but bufio.Scanner
	// inside the Decoder doesn't recover from timeout errors -- once Scan
	// returns false with an error, the Decoder is dead. Don't reintroduce
	// that pattern.)

	f.sender = cls[0]
	f.receivers = cls[1:]
	return f
}

// runRound sends one message from f.sender and returns the latency seen by
// each receiver (receive time minus pre-send time).
func (f *fixture) runRound(tb testing.TB) []time.Duration {
	tb.Helper()
	latencies := make([]time.Duration, len(f.receivers))
	var wg sync.WaitGroup
	wg.Add(len(f.receivers))
	for i, c := range f.receivers {
		i, c := i, c
		go func() {
			defer wg.Done()
			// Each receiver waits for the next Msg frame and records latency.
			for {
				f, err := c.dec.Decode()
				if err != nil {
					latencies[i] = -1
					return
				}
				if _, ok := f.(proto.Msg); ok {
					latencies[i] = time.Since(c.lastSend.Load().(time.Time))
					return
				}
			}
		}()
	}

	// Stamp t0 on every receiver so they can compute latency against it.
	t0 := time.Now()
	for _, c := range f.receivers {
		c.lastSend.Store(t0)
	}
	if err := f.sender.send(proto.Msg{Channel: benchChannel, Body: "x"}); err != nil {
		tb.Fatal(err)
	}
	// Sender will also receive its own broadcast; drain it so the next round is clean.
	go func() {
		f.sender.dec.Decode()
	}()

	wg.Wait()
	return latencies
}

// ---------- TestFanoutLatency: prints a clean stats summary ----------

func TestFanoutLatency(t *testing.T) {
	for _, n := range []int{10, 50, 100} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := setup(t, n)
			const rounds = 30
			var all []time.Duration
			for r := 0; r < rounds; r++ {
				lats := f.runRound(t)
				for _, l := range lats {
					if l > 0 {
						all = append(all, l)
					}
				}
			}
			report(t, n, all)
		})
	}
}

// ---------- BenchmarkFanout: standard `go test -bench` integration ----------

func BenchmarkFanout(b *testing.B) {
	for _, n := range []int{10, 50, 100} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			f := setup(b, n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lats := f.runRound(b)
				_ = lats
			}
		})
	}
}

// ---------- helpers ----------

func report(tb testing.TB, n int, all []time.Duration) {
	tb.Helper()
	if len(all) == 0 {
		tb.Fatal("no samples")
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	p := func(q float64) time.Duration {
		idx := int(float64(len(all)) * q)
		if idx >= len(all) {
			idx = len(all) - 1
		}
		return all[idx]
	}
	mean := time.Duration(0)
	for _, l := range all {
		mean += l
	}
	mean /= time.Duration(len(all))
	tb.Logf("n=%d samples=%d  mean=%v  p50=%v  p95=%v  p99=%v  max=%v",
		n, len(all), mean.Round(time.Microsecond), p(0.50).Round(time.Microsecond),
		p(0.95).Round(time.Microsecond), p(0.99).Round(time.Microsecond), all[len(all)-1].Round(time.Microsecond))
}

func pickFreeAddr(tb testing.TB) string {
	tb.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	a := l.Addr().String()
	_ = l.Close()
	return a
}

func waitDialable(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	for time.Now().Before(deadline) {
		c, err := tls.Dial("tcp", addr, tlsCfg)
		if err == nil {
			_ = c.Close()
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("server not dialable: %s", addr)
}

type benchClient struct {
	conn     net.Conn
	dec      *proto.Decoder
	mu       sync.Mutex
	lastSend atomic.Value // time.Time
}

func (c *benchClient) send(f proto.Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return proto.Encode(c.conn, f)
}

func dialBench(addr, tenant, username, proofB64 string) (*benchClient, error) {
	tlsCfg := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost"}
	c, err := tls.Dial("tcp", addr, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	bc := &benchClient{conn: c, dec: proto.NewDecoder(c)}
	bc.lastSend.Store(time.Now())
	if err := bc.send(proto.Hello{CapVersion: "0.1"}); err != nil {
		return nil, err
	}
	if _, err := bc.expect(proto.TypeWelcome); err != nil {
		return nil, err
	}
	if err := bc.send(proto.AuthRequest{Method: proto.AuthMethodPassword, Tenant: tenant, Username: username}); err != nil {
		return nil, err
	}
	chalF, err := bc.expect(proto.TypeAuthChallenge)
	if err != nil {
		return nil, err
	}
	_ = chalF // we don't need the salt/params; the proof is already computed
	if err := bc.send(proto.AuthResponse{Proof: proofB64}); err != nil {
		return nil, err
	}
	if _, err := bc.expect(proto.TypeAuthOK); err != nil {
		return nil, err
	}
	return bc, nil
}

func (c *benchClient) expect(want string) (proto.Frame, error) {
	f, err := c.dec.Decode()
	if err != nil {
		return nil, err
	}
	if f.FrameType() != want {
		return nil, fmt.Errorf("got %q want %q", f.FrameType(), want)
	}
	return f, nil
}
