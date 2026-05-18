// Package rest is the PRM HTTP control plane: subscription CRUD over a
// dedicated TLS listener. Bearer-token auth (a bot account's API token);
// all operations are scoped to the token's tenant.
//
// Endpoint summary:
//
//	POST   /v1/subscriptions       create (returns plaintext secret once)
//	GET    /v1/subscriptions       list (caller's bot account in caller's tenant)
//	GET    /v1/subscriptions/{id}  read
//	PATCH  /v1/subscriptions/{id}  update mutable fields
//	DELETE /v1/subscriptions/{id}  delete
//	GET    /healthz                liveness; intended for L4 LB health checks (see slice 2 HA)
//
// All responses are JSON. Errors come back with a stable shape:
//
//	{"error": {"code": "...", "message": "..."}}
package rest

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/subops"
	"github.com/biffsocko/prm/internal/webhook"
)

// Config tunes the REST server.
type Config struct {
	Addr       string      // e.g. ":8443"
	TLSConfig  *tls.Config // must have at least one cert
	Store      storage.Store
	Logger     *slog.Logger
	WebhookMgr *webhook.Manager
}

// Server is the PRM HTTP control plane.
type Server struct {
	cfg Config
	log *slog.Logger
	mux *http.ServeMux
	srv *http.Server
	wg  sync.WaitGroup

	// healthy is flipped to true once Serve has accepted at least one
	// connection. For Tier 2 HA, the L4 LB probes /healthz; we return
	// 200 only when this is true (proxy for "this prmd is the leader").
	// Slice 3 ships the endpoint without the leader check; slice 4 wires
	// the ha.Elector into the readiness signal.
	healthy bool
	hmu     sync.Mutex
}

// New constructs a Server. Doesn't start listening.
func New(cfg Config) (*Server, error) {
	if cfg.Addr == "" {
		return nil, errors.New("rest: Addr is required")
	}
	if cfg.TLSConfig == nil || len(cfg.TLSConfig.Certificates) == 0 {
		return nil, errors.New("rest: TLSConfig with at least one certificate is required")
	}
	if cfg.Store == nil {
		return nil, errors.New("rest: Store is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := &Server{cfg: cfg, log: cfg.Logger, mux: http.NewServeMux()}
	s.routes()
	s.srv = &http.Server{
		Addr:              cfg.Addr,
		Handler:           s.mux,
		TLSConfig:         cfg.TLSConfig,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return s, nil
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("POST /v1/subscriptions", s.auth(s.handleCreateSubscription))
	s.mux.HandleFunc("GET /v1/subscriptions", s.auth(s.handleListSubscriptions))
	s.mux.HandleFunc("GET /v1/subscriptions/{id}", s.auth(s.handleGetSubscription))
	s.mux.HandleFunc("PATCH /v1/subscriptions/{id}", s.auth(s.handleUpdateSubscription))
	s.mux.HandleFunc("DELETE /v1/subscriptions/{id}", s.auth(s.handleDeleteSubscription))
}

// Serve listens until ctx is cancelled. Closes the listener on ctx cancel.
func (s *Server) Serve(ctx context.Context) error {
	l, err := tls.Listen("tcp", s.cfg.Addr, s.cfg.TLSConfig)
	if err != nil {
		return fmt.Errorf("rest: listen %s: %w", s.cfg.Addr, err)
	}
	s.log.Info("rest control plane listening", "addr", s.cfg.Addr)
	s.markHealthy(true)

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(shutCtx)
	}()

	err = s.srv.Serve(l)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) markHealthy(v bool) {
	s.hmu.Lock()
	s.healthy = v
	s.hmu.Unlock()
}

// --- auth middleware ---

type ctxKey int

const (
	ctxAccount ctxKey = iota
	ctxTenant
)

// auth wraps a handler with bearer-token auth. Extracts Authorization,
// looks up the token by SHA-256 hash, resolves account + tenant, stashes
// them in request context. Rejects non-bot accounts (humans use the
// realtime protocol for management in slice 3b).
func (s *Server) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hdr := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(hdr, prefix) {
			writeError(w, http.StatusUnauthorized, "missing_bearer", "Authorization: Bearer <token> required")
			return
		}
		plaintext := strings.TrimSpace(strings.TrimPrefix(hdr, prefix))
		if plaintext == "" {
			writeError(w, http.StatusUnauthorized, "empty_token", "empty bearer token")
			return
		}
		sum := sha256.Sum256([]byte(plaintext))
		tok, err := s.cfg.Store.GetTokenByHash(r.Context(), sum[:])
		if err != nil {
			writeError(w, http.StatusUnauthorized, "invalid_token", "token is unknown, revoked, or malformed")
			return
		}
		tenant, err := s.cfg.Store.GetTenantByID(r.Context(), tok.TenantID)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "invalid_token", "token references unknown tenant")
			return
		}
		if tenant.Status == storage.TenantSuspended {
			writeError(w, http.StatusForbidden, "tenant_suspended", "tenant is suspended")
			return
		}
		acc, err := s.cfg.Store.GetAccountByID(r.Context(), tok.TenantID, tok.AccountID)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "invalid_token", "token references unknown account")
			return
		}
		if acc.Type != storage.AccountBot {
			writeError(w, http.StatusForbidden, "not_a_bot", "REST control plane is for bot accounts only")
			return
		}
		// Stash + opportunistically touch last-used (fire-and-forget).
		go func(id uuid.UUID) {
			_ = s.cfg.Store.TouchTokenLastUsed(context.Background(), id)
		}(tok.ID)

		ctx := context.WithValue(r.Context(), ctxAccount, acc)
		ctx = context.WithValue(ctx, ctxTenant, tenant)
		h(w, r.WithContext(ctx))
	}
}

func callerAccount(r *http.Request) *storage.Account {
	v, _ := r.Context().Value(ctxAccount).(*storage.Account)
	return v
}
func callerTenant(r *http.Request) *storage.Tenant {
	v, _ := r.Context().Value(ctxTenant).(*storage.Tenant)
	return v
}

// --- health ---

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	s.hmu.Lock()
	ok := s.healthy
	s.hmu.Unlock()
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "starting up or shutting down")
		return
	}
	_ = writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

// --- subscriptions ---

// createSubscriptionRequest is the POST body. ChannelName and ChannelID
// are mutually exclusive; channel_name is convenient when the caller
// knows the human name but not the UUID.
type createSubscriptionRequest struct {
	ChannelName  string          `json:"channel_name,omitempty"`
	ChannelID    string          `json:"channel_id,omitempty"`
	URL          string          `json:"url"`
	Match        json.RawMessage `json:"match"`
	Events       []string        `json:"events,omitempty"`
	ContextLines int             `json:"context_lines,omitempty"`
	DebounceMs   int             `json:"debounce_ms,omitempty"`
	CooldownMs   int             `json:"cooldown_ms,omitempty"`
	Budget       json.RawMessage `json:"budget,omitempty"`
}

// subscriptionResponse is what the API returns. On create the Secret
// field is populated (plaintext, shown once); on every other read it's
// omitted.
type subscriptionResponse struct {
	ID           string          `json:"id"`
	TenantID     string          `json:"tenant_id"`
	AccountID    string          `json:"account_id"`
	ChannelID    string          `json:"channel_id"`
	URL          string          `json:"url"`
	Secret       string          `json:"secret,omitempty"`
	Match        json.RawMessage `json:"match"`
	Events       []string        `json:"events"`
	ContextLines int             `json:"context_lines"`
	DebounceMs   int             `json:"debounce_ms"`
	CooldownMs   int             `json:"cooldown_ms"`
	Budget       json.RawMessage `json:"budget,omitempty"`
	DisabledAt   *time.Time      `json:"disabled_at,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
}

func toResponse(sub *storage.Subscription, includeSecret bool) subscriptionResponse {
	resp := subscriptionResponse{
		ID:           sub.ID.String(),
		TenantID:     sub.TenantID.String(),
		AccountID:    sub.AccountID.String(),
		ChannelID:    sub.ChannelID.String(),
		URL:          sub.URL,
		Match:        json.RawMessage(sub.MatchJSON),
		Events:       sub.Events,
		ContextLines: sub.ContextLines,
		DebounceMs:   sub.DebounceMs,
		CooldownMs:   sub.CooldownMs,
		Budget:       json.RawMessage(sub.BudgetJSON),
		CreatedAt:    sub.CreatedAt,
	}
	if !sub.DisabledAt.IsZero() {
		t := sub.DisabledAt
		resp.DisabledAt = &t
	}
	if includeSecret {
		resp.Secret = base64.RawURLEncoding.EncodeToString(sub.Secret)
	}
	return resp
}

func (s *Server) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	bot := callerAccount(r)
	tenant := callerTenant(r)

	var req createSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	sub, err := subops.Create(r.Context(), s.cfg.Store, s.cfg.WebhookMgr, tenant, bot, subops.CreateInput{
		ChannelName:  req.ChannelName,
		ChannelID:    req.ChannelID,
		URL:          req.URL,
		Match:        []byte(req.Match),
		Events:       req.Events,
		ContextLines: req.ContextLines,
		DebounceMs:   req.DebounceMs,
		CooldownMs:   req.CooldownMs,
		Budget:       []byte(req.Budget),
	})
	if err != nil {
		writeSubopsError(w, err)
		return
	}
	_ = writeJSON(w, http.StatusCreated, toResponse(sub, true))
}

func (s *Server) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	bot := callerAccount(r)
	tenant := callerTenant(r)
	list, err := subops.List(r.Context(), s.cfg.Store, tenant, bot)
	if err != nil {
		writeSubopsError(w, err)
		return
	}
	out := make([]subscriptionResponse, 0, len(list))
	for _, sub := range list {
		out = append(out, toResponse(sub, false))
	}
	_ = writeJSON(w, http.StatusOK, map[string]any{"subscriptions": out})
}

func (s *Server) handleGetSubscription(w http.ResponseWriter, r *http.Request) {
	bot := callerAccount(r)
	tenant := callerTenant(r)
	sub, err := subops.Get(r.Context(), s.cfg.Store, tenant, bot, r.PathValue("id"))
	if err != nil {
		writeSubopsError(w, err)
		return
	}
	_ = writeJSON(w, http.StatusOK, toResponse(sub, false))
}

// updateSubscriptionRequest is the PATCH body. All fields optional;
// only present fields are applied.
type updateSubscriptionRequest struct {
	URL          *string         `json:"url,omitempty"`
	Match        json.RawMessage `json:"match,omitempty"`
	Events       []string        `json:"events,omitempty"`
	ContextLines *int            `json:"context_lines,omitempty"`
	DebounceMs   *int            `json:"debounce_ms,omitempty"`
	CooldownMs   *int            `json:"cooldown_ms,omitempty"`
	Budget       json.RawMessage `json:"budget,omitempty"`
	Disabled     *bool           `json:"disabled,omitempty"` // true to disable, false to re-enable
}

func (s *Server) handleUpdateSubscription(w http.ResponseWriter, r *http.Request) {
	bot := callerAccount(r)
	tenant := callerTenant(r)
	var req updateSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	in := subops.UpdateInput{
		URL:          req.URL,
		Match:        []byte(req.Match),
		Events:       req.Events,
		ContextLines: req.ContextLines,
		DebounceMs:   req.DebounceMs,
		CooldownMs:   req.CooldownMs,
		Budget:       []byte(req.Budget),
		Disabled:     req.Disabled,
	}
	// subops treats nil slice as "no change"; json.RawMessage of length 0
	// becomes an empty []byte which we want to round-trip as nil.
	if len(req.Match) == 0 {
		in.Match = nil
	}
	if len(req.Budget) == 0 {
		in.Budget = nil
	}
	sub, err := subops.Update(r.Context(), s.cfg.Store, s.cfg.WebhookMgr, tenant, bot, r.PathValue("id"), in)
	if err != nil {
		writeSubopsError(w, err)
		return
	}
	_ = writeJSON(w, http.StatusOK, toResponse(sub, false))
}

func (s *Server) handleDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	bot := callerAccount(r)
	tenant := callerTenant(r)
	if err := subops.Delete(r.Context(), s.cfg.Store, s.cfg.WebhookMgr, tenant, bot, r.PathValue("id")); err != nil {
		writeSubopsError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeSubopsError maps subops.Err* sentinels to HTTP status codes.
func writeSubopsError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, subops.ErrBadRequest):
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
	case errors.Is(err, subops.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, subops.ErrNotOwner):
		writeError(w, http.StatusForbidden, "not_owner", err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
	}
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, body any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(body)
}

// errorBody is the stable error envelope.
type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message,omitempty"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	body := errorBody{}
	body.Error.Code = code
	body.Error.Message = message
	_ = writeJSON(w, status, body)
}

// validateMatch validates a match-rules JSON document by compiling it
// via the matcher package. Returns matcher.ErrInvalidRule-wrapped errors
// so REST handlers can surface a 400 with a useful message. Used at
// create + update time so bad rules fail fast, not at first fire.
func validateMatch(raw json.RawMessage) error {
	return validateMatchJSON([]byte(raw))
}
