// Package channels is the in-memory channel registry and broadcast
// fan-out for PRM.
//
// The hot path:
//
//	conn -> Decode msg -> Registry.Get(tenant, channel) -> Channel.Broadcast(precomputedBytes)
//	  -> for each member: member.Enqueue(bytes)        (sharded, RLock)
//
// Concurrency model:
//   - The Registry is sharded by hash(tenant_id, channel_id) into N shards.
//     Each shard owns its own RWMutex; there is no global lock on the hot
//     path.
//   - Within a shard, lookups take the RLock; mutations (create channel,
//     add/remove member) take the Lock.
//   - Each Channel additionally has its own RWMutex protecting its member
//     map, so Broadcast can run under an RLock while JOIN/PART under the
//     Lock. Two channels can broadcast in parallel even if they hash to
//     the same shard.
//
// Members are *connections*, not accounts. An account can have N concurrent
// connections (multi-device), each represented by a distinct Member.
package channels

import (
	"hash/fnv"
	"sync"
	"time"

	"github.com/google/uuid"
)

// numShards is the number of registry shards. Must be a power of two so we
// can mask cheaply. 64 is plenty for tens of thousands of channels; larger
// deployments can tune this later.
const numShards = 64

// Member is the connection-level interface the channel registry holds. The
// server's connection type implements this.
type Member interface {
	// ConnID is a stable id for this connection. An account may have
	// multiple concurrent connections; each gets a distinct ConnID.
	ConnID() uuid.UUID
	// AccountID is the authenticated account behind this connection.
	AccountID() uuid.UUID
	// DisplayName is the human-readable label for the account.
	DisplayName() string
	// Enqueue pushes precomputed wire bytes onto the connection's
	// outbound queue. Must be nonblocking: under backpressure, the
	// connection's queue drops messages and tags the connection as
	// lagging rather than slowing fan-out.
	Enqueue([]byte)
}

// HistoryEntry is one stored channel message in the in-memory ring buffer
// used for webhook context-attach. Bodies are stored unmodified; timestamps
// are server-stamped.
type HistoryEntry struct {
	From        uuid.UUID
	DisplayName string
	TS          time.Time
	Body        string
}

// defaultHistorySize is how many recent messages each channel retains
// for context-attach on webhook fires. Tunable per channel later;
// 32 is a comfortable default for slice 3.
const defaultHistorySize = 32

// Channel is a single channel's in-memory state.
type Channel struct {
	TenantID uuid.UUID
	ID       uuid.UUID
	Name     string

	mu      sync.RWMutex
	members map[uuid.UUID]Member // keyed by ConnID

	// History ring buffer for webhook context-attach. Protected by histMu
	// (separate from mu so a webhook reader doesn't block fan-out and
	// vice versa).
	histMu  sync.Mutex
	hist    []HistoryEntry
	histPos int // next write index
}

// newChannel creates an empty channel.
func newChannel(tenantID, id uuid.UUID, name string) *Channel {
	return &Channel{
		TenantID: tenantID,
		ID:       id,
		Name:     name,
		members:  make(map[uuid.UUID]Member, 8),
		hist:     make([]HistoryEntry, defaultHistorySize),
	}
}

// AddMember inserts a connection into the channel. Idempotent: replacing an
// existing entry with the same ConnID is fine. Returns true if the member
// is new to the channel (caller may want to broadcast a Presence(join)).
func (c *Channel) AddMember(m Member) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, existed := c.members[m.ConnID()]
	c.members[m.ConnID()] = m
	return !existed
}

// RemoveMember deletes a connection from the channel. Returns true if the
// member was present (caller may want to broadcast a Presence(part)).
func (c *Channel) RemoveMember(connID uuid.UUID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, existed := c.members[connID]
	delete(c.members, connID)
	return existed
}

// Broadcast pushes precomputed wire bytes to every connection in the
// channel.
//
// The bytes are the JSON-encoded frame plus trailing newline -- compute
// them once via proto.EncodeBytes and pass the same []byte here so every
// member's outbound write is just queue.push(sameRef).
//
// Broadcast takes the channel's RLock, which means concurrent broadcasts
// to the same channel can interleave but neither blocks a JOIN/PART
// indefinitely (the writer-priority guarantees of Go's RWMutex apply).
func (c *Channel) Broadcast(bytes []byte) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, m := range c.members {
		m.Enqueue(bytes)
	}
}

// BroadcastExcept is Broadcast but skips one ConnID. Useful for echo
// suppression on the sender's own connection if the server chooses not to
// echo (PRM does echo by default; this is for future flexibility).
func (c *Channel) BroadcastExcept(bytes []byte, skipConn uuid.UUID) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for id, m := range c.members {
		if id == skipConn {
			continue
		}
		m.Enqueue(bytes)
	}
}

// AppendHistory records one message in the channel's ring buffer for
// webhook context-attach. Fast: takes a separate mutex from the member
// list so it doesn't contend with broadcast fan-out.
func (c *Channel) AppendHistory(e HistoryEntry) {
	c.histMu.Lock()
	c.hist[c.histPos] = e
	c.histPos = (c.histPos + 1) % len(c.hist)
	c.histMu.Unlock()
}

// RecentMessages returns up to n most-recent messages from the ring buffer,
// oldest first. If n is 0 or negative, returns an empty slice.
// If n exceeds the ring depth, returns all messages currently in the
// buffer.
func (c *Channel) RecentMessages(n int) []HistoryEntry {
	if n <= 0 {
		return nil
	}
	c.histMu.Lock()
	defer c.histMu.Unlock()
	depth := len(c.hist)
	if n > depth {
		n = depth
	}
	out := make([]HistoryEntry, 0, n)
	// Walk backwards from histPos to pick up the n most recent entries.
	for i := 0; i < n; i++ {
		idx := (c.histPos - 1 - i + depth) % depth
		e := c.hist[idx]
		if e.TS.IsZero() && e.Body == "" {
			break // ring not full yet; reached unused slot
		}
		out = append(out, e)
	}
	// Reverse for oldest-first order.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// MemberCount returns the current member count. Approximate under concurrent
// load; intended for presence/observability, not for branching logic.
func (c *Channel) MemberCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.members)
}

// Members returns a snapshot of all current members. Slice is freshly
// allocated; safe to retain.
func (c *Channel) Members() []Member {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Member, 0, len(c.members))
	for _, m := range c.members {
		out = append(out, m)
	}
	return out
}

// --- Registry ---

// key is the sharded-map key. We pack two UUIDs into a struct so map lookups
// are direct (no string allocation per lookup on the hot path).
type key struct {
	tenantID uuid.UUID
	chanID   uuid.UUID
}

// shard is one slice of the sharded channel registry.
type shard struct {
	mu       sync.RWMutex
	channels map[key]*Channel
}

// Registry holds all live channels across all tenants, sharded for
// concurrent access without a global lock.
type Registry struct {
	shards [numShards]*shard
}

// NewRegistry constructs an empty Registry with all shards initialized.
func NewRegistry() *Registry {
	r := &Registry{}
	for i := range r.shards {
		r.shards[i] = &shard{channels: make(map[key]*Channel, 16)}
	}
	return r
}

// shardFor returns the shard responsible for the given channel key.
// fnv64a over the concatenated UUID bytes; masked to numShards.
func (r *Registry) shardFor(tenantID, chanID uuid.UUID) *shard {
	h := fnv.New64a()
	h.Write(tenantID[:])
	h.Write(chanID[:])
	return r.shards[h.Sum64()&(numShards-1)]
}

// Get returns the channel for (tenantID, chanID), or nil if it does not
// exist. Read-only path: takes only the shard's RLock.
func (r *Registry) Get(tenantID, chanID uuid.UUID) *Channel {
	s := r.shardFor(tenantID, chanID)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.channels[key{tenantID, chanID}]
}

// GetOrCreate returns the channel for (tenantID, chanID), creating it with
// the given display name if it doesn't exist yet. Used on JOIN.
func (r *Registry) GetOrCreate(tenantID, chanID uuid.UUID, name string) *Channel {
	s := r.shardFor(tenantID, chanID)
	k := key{tenantID, chanID}

	// Fast path: read lock, hit.
	s.mu.RLock()
	if c, ok := s.channels[k]; ok {
		s.mu.RUnlock()
		return c
	}
	s.mu.RUnlock()

	// Slow path: write lock, double-check, insert.
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.channels[k]; ok {
		return c
	}
	c := newChannel(tenantID, chanID, name)
	s.channels[k] = c
	return c
}

// Remove deletes a channel from the registry. Intended for slice 2+ when
// channels can be destroyed; slice 1 only ever creates.
func (r *Registry) Remove(tenantID, chanID uuid.UUID) {
	s := r.shardFor(tenantID, chanID)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.channels, key{tenantID, chanID})
}

// Count returns the approximate total channel count across all shards.
// Approximate under concurrent load; intended for observability.
func (r *Registry) Count() int {
	n := 0
	for _, s := range r.shards {
		s.mu.RLock()
		n += len(s.channels)
		s.mu.RUnlock()
	}
	return n
}

// RemoveMemberFromAll removes a connection from every channel it might be
// in. Called when a connection closes. Iterates all shards; for slice 1
// this is fine, for very large deployments slice 2+ should maintain a
// reverse index from ConnID to channel set.
func (r *Registry) RemoveMemberFromAll(connID uuid.UUID) {
	for _, s := range r.shards {
		s.mu.RLock()
		// Snapshot channels in this shard so we can release the shard lock
		// before touching each channel's lock (avoid lock-ordering hazards).
		chs := make([]*Channel, 0, len(s.channels))
		for _, c := range s.channels {
			chs = append(chs, c)
		}
		s.mu.RUnlock()
		for _, c := range chs {
			c.RemoveMember(connID)
		}
	}
}
