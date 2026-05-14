package probe

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func staticProbe(s Status) Probe {
	return Func(func(context.Context) Status { return s })
}

func TestAllSuccess(t *testing.T) {
	t.Parallel()
	c := All(staticProbe(StatusUp), staticProbe(StatusUp))
	if got := c.Check(context.Background()); got != StatusUp {
		t.Fatalf("All(up,up) = %v, want up", got)
	}
}

func TestAllOneDown(t *testing.T) {
	t.Parallel()
	c := All(staticProbe(StatusUp), staticProbe(StatusDown))
	if got := c.Check(context.Background()); got != StatusDown {
		t.Fatalf("All(up,down) = %v, want down", got)
	}
}

func TestAllTimeoutDominates(t *testing.T) {
	t.Parallel()
	c := All(staticProbe(StatusDown), staticProbe(StatusTimeout))
	if got := c.Check(context.Background()); got != StatusTimeout {
		t.Fatalf("All(down,timeout) = %v, want timeout (worst)", got)
	}
}

func TestAnyOneUp(t *testing.T) {
	t.Parallel()
	c := Any(staticProbe(StatusDown), staticProbe(StatusUp))
	if got := c.Check(context.Background()); got != StatusUp {
		t.Fatalf("Any(down,up) = %v, want up", got)
	}
}

func TestAnyAllDown(t *testing.T) {
	t.Parallel()
	c := Any(staticProbe(StatusDown), staticProbe(StatusDown))
	if got := c.Check(context.Background()); got != StatusDown {
		t.Fatalf("Any(down,down) = %v, want down", got)
	}
}

func TestEmptyCompositeUp(t *testing.T) {
	t.Parallel()
	if got := All().Check(context.Background()); got != StatusUp {
		t.Fatalf("empty All = %v, want up", got)
	}
}

func TestCompositeRunsConcurrently(t *testing.T) {
	t.Parallel()
	var n atomic.Int32
	slow := Func(func(ctx context.Context) Status {
		n.Add(1)
		time.Sleep(20 * time.Millisecond)
		return StatusUp
	})
	c := All(slow, slow, slow, slow)
	start := time.Now()
	c.Check(context.Background())
	if elapsed := time.Since(start); elapsed > 60*time.Millisecond {
		t.Fatalf("expected concurrent execution, took %v", elapsed)
	}
	if n.Load() != 4 {
		t.Fatalf("expected 4 invocations, got %d", n.Load())
	}
}

func TestCompositeWithSyncAsyncer(t *testing.T) {
	t.Parallel()
	c := New(ModeAll, WithAsyncer(SyncFactory()))
	c.Add(staticProbe(StatusUp)).Add(staticProbe(StatusUp))
	if got := c.Check(context.Background()); got != StatusUp {
		t.Fatalf("got %v, want up", got)
	}
	if c.Len() != 2 {
		t.Fatalf("Len = %d, want 2", c.Len())
	}
}
