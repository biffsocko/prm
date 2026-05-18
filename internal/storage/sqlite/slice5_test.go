package sqlite_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/biffsocko/prm/internal/storage"
)

func TestStoredMessageRoundTripAndListing(t *testing.T) {
	ten, _, ch, s := mkTenantAccountChannel(t)
	ctx := context.Background()

	// Insert 5 messages with increasing timestamps.
	base := time.Now().UTC().Truncate(time.Millisecond)
	for i := 0; i < 5; i++ {
		m := &storage.StoredMessage{
			TenantID: ten.ID, ChannelID: ch.ID,
			FromAccountID: ch.OwnerID,
			Body:          fmt.Sprintf("msg-%d", i),
			TS:            base.Add(time.Duration(i) * time.Second),
		}
		if err := s.RecordMessage(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	// List newest 3 -> should be the last three, oldest-first.
	list, err := s.ListMessages(ctx, ten.ID, ch.ID, 3, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3, got %d", len(list))
	}
	wantBodies := []string{"msg-2", "msg-3", "msg-4"}
	for i, m := range list {
		if m.Body != wantBodies[i] {
			t.Errorf("idx %d: got %q want %q", i, m.Body, wantBodies[i])
		}
	}

	// Page back: ask for messages strictly before list[0].TS.
	older, err := s.ListMessages(ctx, ten.ID, ch.ID, 10, list[0].TS)
	if err != nil {
		t.Fatal(err)
	}
	if len(older) != 2 {
		t.Fatalf("expected 2 older, got %d", len(older))
	}
	if older[0].Body != "msg-0" || older[1].Body != "msg-1" {
		t.Errorf("paging order: %v", []string{older[0].Body, older[1].Body})
	}
}

func TestStoredMessageCrossTenantIsolation(t *testing.T) {
	ten, _, ch, s := mkTenantAccountChannel(t)
	ctx := context.Background()
	t2 := &storage.Tenant{Slug: "globex", DisplayName: "Globex"}
	if err := s.CreateTenant(ctx, t2); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordMessage(ctx, &storage.StoredMessage{
		TenantID: ten.ID, ChannelID: ch.ID, FromAccountID: ch.OwnerID,
		Body: "in acme only", TS: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListMessages(ctx, t2.ID, ch.ID, 10, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("cross-tenant list should be empty, got %d", len(got))
	}
}

func TestPurgeMessagesOlderThan(t *testing.T) {
	ten, _, ch, s := mkTenantAccountChannel(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// 2 old messages, 2 recent.
	for _, ts := range []time.Time{now.Add(-8 * 24 * time.Hour), now.Add(-7*24*time.Hour - time.Hour)} {
		_ = s.RecordMessage(ctx, &storage.StoredMessage{
			TenantID: ten.ID, ChannelID: ch.ID, FromAccountID: ch.OwnerID,
			Body: "old", TS: ts,
		})
	}
	for _, ts := range []time.Time{now.Add(-1 * time.Hour), now.Add(-1 * time.Minute)} {
		_ = s.RecordMessage(ctx, &storage.StoredMessage{
			TenantID: ten.ID, ChannelID: ch.ID, FromAccountID: ch.OwnerID,
			Body: "fresh", TS: ts,
		})
	}
	n, err := s.PurgeMessagesOlderThan(ctx, 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("expected 2 purged, got %d", n)
	}
	rest, _ := s.ListMessages(ctx, ten.ID, ch.ID, 10, time.Time{})
	if len(rest) != 2 {
		t.Errorf("expected 2 remaining, got %d", len(rest))
	}
}
