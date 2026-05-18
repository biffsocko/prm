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
	"crypto/tls"
	"encoding/base64"
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
	"github.com/biffsocko/prm/internal/webhook"
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
	// WebhookMgr, if set, is notified after every channel msg broadcast
	// so matching subscriptions can fire. Optional; nil disables webhook
	// dispatch.
	WebhookMgr *webhook.Manager
}

// Server is the PRM realtime server.
type Server struct {
	addr     string
	tlsCfg   *tls.Config
	store    storage.Store
	tenants  *tenants.Service
	channels *channels.Registry
	webhooks *webhook.Manager // optional; nil disables webhook dispatch
	history  *historyWriter   // async persister; nil disables history
	name     string
	version  string
	log      *slog.Logger

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
		webhooks: cfg.WebhookMgr,
		history:  newHistoryWriter(cfg.Store),
		name:     cfg.Name,
		version:  cfg.Version,
		log:      log,
	}, nil
}

// Serve listens on the configured address and accepts connections until
// ctx is cancelled or Listen fails. Each accepted connection runs its own
// goroutines and is independent.
//
// Serve also spins up the async history writer goroutine and tears it
// down before returning.
func (s *Server) Serve(ctx context.Context) error {
	// History writer runs for the lifetime of Serve.
	if s.history != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.history.run(ctx)
		}()
		defer s.history.Close()
	}
	return s.serveAcceptLoop(ctx)
}

func (s *Server) serveAcceptLoop(ctx context.Context) error {
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

// canJoin enforces channel ACLs on JOIN.
//
//	- ChannelPublic: any authenticated account in the tenant may join.
//	- ChannelPrivate: account must have an ACL entry with a role that
//	  satisfies ChannelRole.CanJoin() (owner / admin / member).
//
// Returns (allowed, wireReasonCode). wireReasonCode is one of
// "permission_denied" (in ACL but role doesn't allow it, e.g. banned)
// or "not_in_acl" (no ACL entry at all on a private channel).
func (s *Server) canJoin(ctx context.Context, tenantID uuid.UUID, channel *storage.Channel, accountID uuid.UUID) (bool, string) {
	switch channel.Visibility {
	case storage.ChannelPublic:
		return true, ""
	case storage.ChannelPrivate:
		entry, err := s.store.GetChannelACL(ctx, tenantID, channel.ID, accountID)
		if err != nil {
			return false, "not_in_acl"
		}
		if !entry.Role.CanJoin() {
			return false, "permission_denied"
		}
		return true, ""
	default:
		return false, "unknown_visibility"
	}
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

// uuidUnused suppresses unused-import warnings if uuid only appears in
// signatures of helpers further down. It's currently referenced; this is
// belt-and-suspenders against future refactors.
var _ = uuid.Nil
