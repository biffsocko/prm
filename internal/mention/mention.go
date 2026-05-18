// Package mention resolves @-mentions in a chat message body to account
// UUIDs scoped to the tenant. Two forms are accepted:
//
//	@username                       resolved via storage.GetAccountByUsername
//	@019e3bbf-b303-72af-9fee-...    accepted as-is (no lookup; useful for bots)
//
// Username syntax is conservative: 1-32 chars of [a-zA-Z0-9_-].
// PRM enforces unique-per-tenant on usernames so @alice is unambiguous
// within a given tenant.
package mention

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/storage"
)

// mentionPattern matches a single @-mention token. Anchored with a
// boundary lookback approximation: the @ must be at the start of the
// string or preceded by whitespace / punctuation. We can't use Go's
// regexp lookbehind, so we capture the leading boundary as part of the
// match and trim it.
var mentionPattern = regexp.MustCompile(`(?:^|[\s,;:!?()\[\]{}<>"'/])@([A-Za-z0-9_-]{1,36})`)

// Resolve scans body for @-mentions and returns the deduplicated set of
// account UUIDs the mentions resolved to within the given tenant.
// Unknown mentions are silently skipped (no error). UUID-form mentions
// are returned as-is without a lookup.
//
// Returns nil (not empty slice) if there are no @-mentions in body, to
// avoid the storage round-trip on the message hot path.
func Resolve(ctx context.Context, st storage.Store, tenantID uuid.UUID, body string) []uuid.UUID {
	if !strings.Contains(body, "@") {
		return nil
	}
	matches := mentionPattern.FindAllStringSubmatch(body, -1)
	if len(matches) == 0 {
		return nil
	}
	seen := make(map[uuid.UUID]struct{}, len(matches))
	out := make([]uuid.UUID, 0, len(matches))

	for _, m := range matches {
		token := m[1]
		// Try UUID form first.
		if id, err := uuid.Parse(token); err == nil {
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
			continue
		}
		// Username form.
		acc, err := st.GetAccountByUsername(ctx, tenantID, token)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			// Real storage error: skip this mention, keep going.
			continue
		}
		if _, dup := seen[acc.ID]; dup {
			continue
		}
		seen[acc.ID] = struct{}{}
		out = append(out, acc.ID)
	}
	return out
}
