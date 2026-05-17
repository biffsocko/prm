package ha_test

import (
	"context"
	"testing"
	"time"

	"github.com/biffsocko/prm/internal/ha"
)

func TestLocalElectorAcquireReturnsCancellableContext(t *testing.T) {
	el := ha.NewLocal()
	ctx, err := el.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != nil {
		t.Fatal("expected acquired context to be alive")
	}
	if err := el.Release(); err != nil {
		t.Fatal(err)
	}
	// After Release, the context should cancel.
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("context should cancel after Release")
	}
}

func TestLocalElectorParentCancelCancelsLeader(t *testing.T) {
	el := ha.NewLocal()
	parent, cancel := context.WithCancel(context.Background())
	ctx, err := el.Acquire(parent)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("leader ctx should cancel when parent cancels")
	}
}

func TestLocalElectorReleaseIsIdempotent(t *testing.T) {
	el := ha.NewLocal()
	if err := el.Release(); err != nil {
		t.Errorf("Release before Acquire: %v", err)
	}
	if _, err := el.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := el.Release(); err != nil {
		t.Errorf("first Release: %v", err)
	}
	if err := el.Release(); err != nil {
		t.Errorf("second Release: %v", err)
	}
}
