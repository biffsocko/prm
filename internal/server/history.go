package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/proto"
	"github.com/biffsocko/prm/internal/storage"
)

// historyWriterDepth is how many pending messages buffer between
// handleMsg (the hot path) and the background writer. A burst larger
// than this drops the oldest pending writes -- we accept losing some
// history entries under sustained storage saturation rather than
// blocking the broadcast path.
const historyWriterDepth = 4096

// historyWriter consumes stored-message records and writes them to
// storage. One goroutine; multiple concurrent enqueuers. Backpressure
// is dropped-with-counter, never blocking.
type historyWriter struct {
	store storage.Store
	in    chan *storage.StoredMessage

	dropped int64 // atomic
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

func newHistoryWriter(st storage.Store) *historyWriter {
	return &historyWriter{
		store: st,
		in:    make(chan *storage.StoredMessage, historyWriterDepth),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
}

// run drains the input channel until ctx cancels OR stop is closed.
// On shutdown, drains the remaining buffer with a short deadline so
// in-flight messages get a chance to land.
func (h *historyWriter) run(ctx context.Context) {
	defer close(h.done)
	for {
		select {
		case <-ctx.Done():
			h.drain(2 * time.Second)
			return
		case <-h.stop:
			h.drain(2 * time.Second)
			return
		case m := <-h.in:
			h.write(m)
		}
	}
}

func (h *historyWriter) drain(deadline time.Duration) {
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		select {
		case m := <-h.in:
			h.write(m)
		default:
			return
		}
	}
}

func (h *historyWriter) write(m *storage.StoredMessage) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := h.store.RecordMessage(ctx, m); err != nil {
		// Storage saturated or failed; bump drop counter so it shows up
		// in slow-storage diagnostics.
		atomic.AddInt64(&h.dropped, 1)
	}
}

// enqueue is called from the hot path (handleMsg). Nonblocking: drop on
// full queue and increment counter.
func (h *historyWriter) enqueue(m *storage.StoredMessage) {
	select {
	case h.in <- m:
	default:
		atomic.AddInt64(&h.dropped, 1)
	}
}

// Close requests the writer to stop. Safe to call multiple times.
func (h *historyWriter) Close() {
	h.once.Do(func() { close(h.stop) })
	<-h.done
}

// --- chathistory verb handler ---

func (c *Conn) handleChatHistory(ctx context.Context, m proto.ChatHistory) {
	if m.Channel == "" {
		c.sendError("invalid_request", "channel is required", m.ID)
		return
	}
	// Channel must exist and the caller must be authed (already
	// guaranteed by the post-auth dispatch). We don't currently
	// require the caller to have JOINed -- knowing the channel name
	// is sufficient since channels.GetChannelByName is tenant-scoped.
	ch, err := c.srv.store.GetChannelByName(ctx, c.tenantID, m.Channel)
	if err != nil {
		c.sendError("channel_not_found", "no such channel in this tenant", m.ID)
		return
	}
	limit := m.Limit
	if limit <= 0 {
		limit = 50
	}
	list, err := c.srv.store.ListMessages(ctx, c.tenantID, ch.ID, limit, m.BeforeTS)
	if err != nil {
		c.sendError("internal", "list messages", m.ID)
		return
	}
	wire := make([]proto.StoredMessageWire, 0, len(list))
	for _, sm := range list {
		wire = append(wire, proto.StoredMessageWire{
			ID:   sm.ID.String(),
			From: sm.FromAccountID.String(),
			TS:   sm.TS,
			Body: sm.Body,
		})
	}
	c.sendFrame(proto.ChatHistoryOK{ID: m.ID, Channel: m.Channel, Messages: wire})
}

// --- helpers used elsewhere ---

func newStoredMessage(tenantID, channelID, from uuid.UUID, body string, ts time.Time) *storage.StoredMessage {
	return &storage.StoredMessage{
		TenantID:      tenantID,
		ChannelID:     channelID,
		FromAccountID: from,
		Body:          body,
		TS:            ts,
	}
}
