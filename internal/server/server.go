// Package server is the PRM realtime server: TLS listener, per-connection
// goroutines, hello/auth handshake, channel fan-out.
//
// Architecture (slice 1):
//
//	tls.Listener.Accept() -> per-conn read goroutine + write goroutine + ping goroutine
//	read: proto.Decoder -> dispatch -> handle{Hello,AuthRequest,AuthResponse,Join,Part,Msg,Ping,Pong}
//	write: drain Conn.out chan -> bufio.Writer -> net.Conn
//	fan-out: channels.Channel.Broadcast(bytes) -> for each member m: m.Enqueue(bytes)
//
// The fan-out path never touches storage. ACLs are checked at JOIN (slice 2
// will enforce; slice 1 is one public channel per tenant).
package server

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/channels"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/tenants"
)

// Config is what a caller passes to New.
type Config struct {
	// Addr is the TCP address to listen on (e.g., ":6697").
	Addr string
	// TLSConfig must include at least one server certificate.
	TLSConfig *tls.Config
	// Storage backend.
	Store storage.Store
	// Logger. If nil, slog.Default() is used.
	Logger *slog.Logger
	// Name and Version surfaced in Welcome frames.
	Name    string
	Version string
}

// Server is the PRM realtime server.
type Server struct {
	addr      string
	tlsCfg    *tls.Config
	store     storage.Store
	tenants   *tenants.Service
	channels  *channels.Registry
	name      string
	version   string
	log       *slog.Logger

	listener net.Listener
	wg       sync.WaitGroup
}

// New constructs a Server. Doesn't start listening yet — call Serve(ctx).
func New(cfg Config) (*Server, error) {
	if cfg.Addr == "" {
		return nil, errors.New("server: Addr is required")
	}
	if cfg.TLSConfig == nil || len(cfg.TLSConfig.Certificates) == 0 {
		return nil, errors.New("server: TLSConfig with at least one certificate is required")
	}
	if cfg.Store == nil {
		return nil, errors.New("server: Store is required")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	if cfg.Name == "" {
		cfg.Name = "prmd"
	}
	if cfg.Version == "" {
		cfg.Version = "0.0.0-dev"
	}
	return &Server{
		addr:     cfg.Addr,
		tlsCfg:   cfg.TLSConfig,
		store:    cfg.Store,
		tenants:  tenants.New(cfg.Store),
		channels: channels.NewRegistry(),
		name:     cfg.Name,
		version:  cfg.Version,
		log:      log,
	}, nil
}

// Serve listens on the configured address and accepts connections until
// ctx is cancelled or Listen fails. Each accepted connection runs its own
// goroutines and is independent.
func (s *Server) Serve(ctx context.Context) error {
	l, err := tls.Listen("tcp", s.addr, s.tlsCfg)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", s.addr, err)
	}
	s.listener = l
	s.log.Info("prmd listening", "addr", s.addr)

	// Close the listener when ctx cancels so Accept unblocks.
	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()

	for {
		raw, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				s.log.Info("listener closed; shutting down")
				break
			}
			s.log.Warn("accept error", "err", err)
			continue
		}
		c := newConn(s, raw)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			c.serve(ctx)
		}()
	}
	s.wg.Wait()
	return nil
}

// --- internal helpers used by Conn ---

// pendingChalField is the on-Conn field used during the auth handshake.
// It's a free-floating field declared on Conn (in conn.go) but typed here
// so server.go owns the actual auth.Challenge type knowledge.

// beginPasswordAuth wraps auth.BeginPasswordAuth.
func (s *Server) beginPasswordAuth(ctx context.Context, tenantSlug, username string) (*auth.Challenge, *storage.Tenant, error) {
	return auth.BeginPasswordAuth(ctx, s.store, tenantSlug, username)
}

// completePasswordAuth wraps auth.CompletePasswordAuth.
func (s *Server) completePasswordAuth(ctx context.Context, ch *auth.Challenge, proofB64 string) (*auth.Result, error) {
	return auth.CompletePasswordAuth(ctx, s.store, ch, proofB64)
}

// channelIDForName maps (tenantID, channelName) to a deterministic UUID.
// Slice 1 derives channel IDs from name so JOIN("general") always lands on
// the same channel state across reconnects. Slice 2 will introduce real
// persisted channels with explicit IDs and ACLs.
func (s *Server) channelIDForName(tenantID uuid.UUID, name string) uuid.UUID {
	h := sha256.New()
	h.Write(tenantID[:])
	h.Write([]byte("/"))
	h.Write([]byte(name))
	sum := h.Sum(nil)
	var id uuid.UUID
	copy(id[:], sum[:16])
	// Stamp version 5 (name-based) in the version nibble.
	id[6] = (id[6] & 0x0F) | 0x50
	// Stamp variant.
	id[8] = (id[8] & 0x3F) | 0x80
	return id
}

// authErrorReason maps auth-layer errors to wire-safe reason codes.
func authErrorReason(err error) string {
	switch {
	case errors.Is(err, auth.ErrUnauthenticated):
		return "invalid_credentials"
	case errors.Is(err, auth.ErrTenantNotFound):
		return "invalid_credentials" // don't leak whether tenant exists
	case errors.Is(err, auth.ErrTenantSuspended):
		return "tenant_suspended"
	case errors.Is(err, auth.ErrUnsupported):
		return "unsupported_method"
	default:
		return "internal"
	}
}

// base64encode is a small helper so wire-bound byte fields encode/decode
// consistently as base64.
func base64encode(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// quiet the unused-import lints for binary; reserved for future use in
// length-prefixed extensions of the wire protocol.
var _ = binary.LittleEndian
