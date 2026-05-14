package runner

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gopherex/xprobe/pkg/probe"
	"github.com/gopherex/xprobe/pkg/reporter"
	"github.com/gopherex/xprobe/pkg/state"
)

func TestRunnerImmediateAndUpdates(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	p := probe.Func(func(context.Context) probe.Status {
		calls.Add(1)
		return probe.StatusUp
	})

	s := state.New()
	r := New(p, s,
		WithInterval(20*time.Millisecond),
		WithTimeout(50*time.Millisecond),
		RunImmediately(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	r.Run(ctx)

	if s.Get() != probe.StatusUp {
		t.Fatalf("state = %v, want up", s.Get())
	}
	if calls.Load() < 2 {
		t.Fatalf("expected at least 2 ticks, got %d", calls.Load())
	}
}

func TestRunnerReportsTransitions(t *testing.T) {
	t.Parallel()

	var st atomic.Int32
	st.Store(int32(probe.StatusDown))
	p := probe.Func(func(context.Context) probe.Status {
		return probe.Status(st.Load())
	})

	var transitions atomic.Int32
	rep := reporter.Func(func(_ context.Context, _ reporter.Event) {
		transitions.Add(1)
	})

	s := state.New()
	r := New(p, s,
		WithName("svc"),
		WithInterval(10*time.Millisecond),
		WithReporter(rep),
		RunImmediately(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	r.Start(ctx)

	time.Sleep(25 * time.Millisecond)
	st.Store(int32(probe.StatusUp))
	time.Sleep(40 * time.Millisecond)
	cancel()

	if transitions.Load() < 2 {
		t.Fatalf("expected at least 2 transitions (unknown->down, down->up), got %d", transitions.Load())
	}
}

func TestRunnerTimeout(t *testing.T) {
	t.Parallel()

	p := probe.Func(func(ctx context.Context) probe.Status {
		<-ctx.Done()
		return probe.StatusUp
	})

	s := state.New()
	r := New(p, s,
		WithInterval(time.Hour),
		WithTimeout(10*time.Millisecond),
		RunImmediately(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	r.Run(ctx)

	if s.Get() != probe.StatusTimeout {
		t.Fatalf("state = %v, want timeout", s.Get())
	}
}
