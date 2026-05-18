package channels_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/channels"
)

func TestRecentMessagesEmpty(t *testing.T) {
	r := channels.NewRegistry()
	c := r.GetOrCreate(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), "ch")
	if got := c.RecentMessages(8); len(got) != 0 {
		t.Errorf("empty channel should return no recent messages; got %d", len(got))
	}
	if got := c.RecentMessages(0); got != nil {
		t.Errorf("n=0 should return nil")
	}
}

func TestRecentMessagesOrderingAndCap(t *testing.T) {
	r := channels.NewRegistry()
	c := r.GetOrCreate(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), "ch")

	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		c.AppendHistory(channels.HistoryEntry{
			From: uuid.Must(uuid.NewV7()), Body: string(rune('a' + i)), TS: now.Add(time.Duration(i) * time.Millisecond),
		})
	}

	// Ask for 3 most recent -> bodies "c", "d", "e" in that (oldest-first) order.
	got := c.RecentMessages(3)
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	wantBodies := []string{"c", "d", "e"}
	for i, e := range got {
		if e.Body != wantBodies[i] {
			t.Errorf("index %d: got %q want %q", i, e.Body, wantBodies[i])
		}
	}
}

func TestRecentMessagesRingBufferWrapsAround(t *testing.T) {
	r := channels.NewRegistry()
	c := r.GetOrCreate(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), "ch")

	// Append more than the default capacity (32). Verify only the last 32
	// are retained, oldest-first.
	const total = 50
	for i := 0; i < total; i++ {
		c.AppendHistory(channels.HistoryEntry{Body: itoa(i)})
	}
	got := c.RecentMessages(32)
	if len(got) != 32 {
		t.Fatalf("expected 32, got %d", len(got))
	}
	// Oldest should be "18" (50 - 32), newest "49".
	if got[0].Body != itoa(total-32) {
		t.Errorf("oldest mismatch: got %q want %q", got[0].Body, itoa(total-32))
	}
	if got[len(got)-1].Body != itoa(total-1) {
		t.Errorf("newest mismatch: got %q want %q", got[len(got)-1].Body, itoa(total-1))
	}
}

func TestRecentMessagesCapsToBufferDepth(t *testing.T) {
	r := channels.NewRegistry()
	c := r.GetOrCreate(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), "ch")
	for i := 0; i < 4; i++ {
		c.AppendHistory(channels.HistoryEntry{Body: itoa(i)})
	}
	// Ask for many; should only return 4.
	got := c.RecentMessages(100)
	if len(got) != 4 {
		t.Fatalf("expected 4, got %d", len(got))
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	digits := []byte{}
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	if neg {
		return "-" + string(digits)
	}
	return string(digits)
}
