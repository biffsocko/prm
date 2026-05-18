package rest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/inbound"
	"github.com/biffsocko/prm/internal/storage"
)

// MaxInboundBodyBytes caps the size of an inbound integration POST.
// Upstream systems (Splunk, Graylog, etc.) generally send small JSON;
// 64 KB is generous. Larger bodies get 413.
const MaxInboundBodyBytes = 64 * 1024

// EventPublisher is what the rest server needs to republish a normalized
// inbound event onto a channel. server.Server satisfies this; the
// indirection avoids a rest -> server import (server already imports
// proto/auth/etc. which would cycle).
type EventPublisher interface {
	PublishInbound(ctx context.Context, tenantID, channelID, fromAccountID uuid.UUID, ev inbound.Event) error
}

func (s *Server) handleInbound(w http.ResponseWriter, r *http.Request) {
	if s.cfg.EventPublisher == nil {
		writeError(w, http.StatusNotImplemented, "no_publisher", "inbound endpoint not wired (no EventPublisher in Config)")
		return
	}

	// Path-supplied integration id is a sanity check; the actual auth
	// is the bearer token, which uniquely identifies an integration.
	pathID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_id", err.Error())
		return
	}

	hdr := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(hdr, prefix) {
		writeError(w, http.StatusUnauthorized, "missing_bearer", "Authorization: Bearer <integration-token> required")
		return
	}
	plaintext := strings.TrimSpace(strings.TrimPrefix(hdr, prefix))
	if plaintext == "" {
		writeError(w, http.StatusUnauthorized, "empty_token", "")
		return
	}
	sum := sha256.Sum256([]byte(plaintext))

	integ, err := s.cfg.Store.GetIntegrationByTokenHash(r.Context(), sum[:])
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid_token", "unknown / revoked / disabled integration token")
		return
	}
	if integ.ID != pathID {
		// Token is valid but doesn't match the path id -- somebody is
		// trying to use one integration's token at another integration's
		// URL. Refuse without disclosing which is which.
		writeError(w, http.StatusUnauthorized, "invalid_token", "token does not match path id")
		return
	}

	tenant, err := s.cfg.Store.GetTenantByID(r.Context(), integ.TenantID)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid_token", "token references unknown tenant")
		return
	}
	if tenant.Status == storage.TenantSuspended {
		writeError(w, http.StatusForbidden, "tenant_suspended", "")
		return
	}

	// Size cap. ContentLength is advisory (chunked / hostile clients can
	// lie), so we wrap with a LimitReader too.
	if r.ContentLength > MaxInboundBodyBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large",
			fmt.Sprintf("body exceeds %d bytes", MaxInboundBodyBytes))
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxInboundBodyBytes+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "read_body", err.Error())
		return
	}
	if len(body) > MaxInboundBodyBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large",
			fmt.Sprintf("body exceeds %d bytes", MaxInboundBodyBytes))
		return
	}

	adapter, err := inbound.Lookup(integ.Adapter)
	if err != nil {
		// Misconfigured integration (adapter no longer compiled in).
		s.log.Error("inbound: unknown adapter", "adapter", integ.Adapter, "integration_id", integ.ID)
		writeError(w, http.StatusInternalServerError, "adapter_unavailable", "")
		return
	}

	ev, err := adapter.Normalize(body, r.Header, integ.SettingsJSON)
	if err != nil {
		// Bad upstream payload. 400, not 500.
		writeError(w, http.StatusBadRequest, mapInboundReason(err), err.Error())
		return
	}

	if err := s.cfg.EventPublisher.PublishInbound(r.Context(), integ.TenantID, integ.ChannelID, integ.AccountID, ev); err != nil {
		s.log.Error("inbound: publish failed", "err", err, "integration_id", integ.ID)
		writeError(w, http.StatusInternalServerError, "publish_failed", "")
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func mapInboundReason(err error) string {
	switch {
	case errors.Is(err, inbound.ErrAdapterMissing):
		return "missing_field"
	case errors.Is(err, inbound.ErrAdapterBadInput):
		return "bad_input"
	case errors.Is(err, inbound.ErrAdapterRateLimit):
		return "rate_limited"
	default:
		return "adapter_error"
	}
}
