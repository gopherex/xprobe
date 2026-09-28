package runner

import (
	"context"
	"runtime"
	"sync"
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

func TestCheckUpdatesStateAndReports(t *testing.T) {
	t.Parallel()

	var st atomic.Int32
	st.Store(int32(probe.StatusDown))
	p := probe.Func(func(context.Context) probe.Status {
		return probe.Status(st.Load())
	})

	var events []reporter.Event
	rep := reporter.Func(func(_ context.Context, ev reporter.Event) {
		events = append(events, ev)
	})

	s := state.New()
	r := New(p, s, WithName("svc"), WithInterval(time.Hour), WithReporter(rep))

	if got := r.Check(context.Background()); got != probe.StatusDown {
		t.Fatalf("Check = %v, want down", got)
	}
	if s.Get() != probe.StatusDown {
		t.Fatalf("state = %v, want down", s.Get())
	}

	// Same status again: stored, but no transition to report.
	r.Check(context.Background())

	st.Store(int32(probe.StatusUp))
	if got := r.Check(context.Background()); got != probe.StatusUp {
		t.Fatalf("Check = %v, want up", got)
	}
	if s.Get() != probe.StatusUp {
		t.Fatalf("state = %v, want up", s.Get())
	}

	want := []reporter.Event{
		{Name: "svc", Prev: probe.StatusUnknown, Cur: probe.StatusDown},
		{Name: "svc", Prev: probe.StatusDown, Cur: probe.StatusUp},
	}
	if len(events) != len(want) {
		t.Fatalf("events = %+v, want %+v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("event[%d] = %+v, want %+v", i, events[i], want[i])
		}
	}
}

func TestCheckBeforeRun(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	p := probe.Func(func(context.Context) probe.Status {
		calls.Add(1)
		return probe.StatusUp
	})

	s := state.New()
	r := New(p, s, WithInterval(time.Hour))

	if got := r.Check(context.Background()); got != probe.StatusUp {
		t.Fatalf("Check = %v, want up", got)
	}
	if !s.HasBeenSet() || s.Get() != probe.StatusUp {
		t.Fatalf("state = %v (set=%v), want up", s.Get(), s.HasBeenSet())
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

func TestCheckTimeout(t *testing.T) {
	t.Parallel()

	p := probe.Func(func(ctx context.Context) probe.Status {
		<-ctx.Done()
		return probe.StatusUp
	})

	s := state.New()
	r := New(p, s, WithTimeout(10*time.Millisecond))

	if got := r.Check(context.Background()); got != probe.StatusTimeout {
		t.Fatalf("Check = %v, want timeout", got)
	}
	if s.Get() != probe.StatusTimeout {
		t.Fatalf("state = %v, want timeout", s.Get())
	}
}

func TestCheckCanceledContextKeepsState(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	p := probe.Func(func(context.Context) probe.Status {
		calls.Add(1)
		return probe.StatusDown
	})

	s := state.New()
	s.Set(probe.StatusUp)
	r := New(p, s)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if got := r.Check(ctx); got != probe.StatusUp {
		t.Fatalf("Check = %v, want cached up", got)
	}
	if s.Get() != probe.StatusUp {
		t.Fatalf("state = %v, want up (untouched)", s.Get())
	}
	if calls.Load() != 0 {
		t.Fatalf("probe ran %d times on a canceled ctx, want 0", calls.Load())
	}

	// Never-set state reports StatusUnknown and stays unset.
	fresh := state.New()
	if got := New(p, fresh).Check(ctx); got != probe.StatusUnknown {
		t.Fatalf("Check = %v, want unknown", got)
	}
	if fresh.HasBeenSet() {
		t.Fatal("state set by a canceled Check")
	}
}

// TestCheckConcurrentWithRunIsSerialized hammers Check from several
// goroutines while Run ticks as fast as it can. Each evaluation returns the
// opposite of the previous one, so every Set is a transition and the reporter
// sees one event per evaluation. With evaluations serialized, event k carries
// exactly the k-th probe result, every event chains from the previous one, and
// the final State equals the last evaluation.
func TestCheckConcurrentWithRunIsSerialized(t *testing.T) {
	t.Parallel()

	statusOf := func(seq int64) probe.Status {
		if seq%2 == 1 {
			return probe.StatusDown
		}
		return probe.StatusUp
	}

	var seq, last atomic.Int64
	p := probe.Func(func(context.Context) probe.Status {
		n := seq.Add(1)
		runtime.Gosched() // widen the window between check and Set
		last.Store(n)
		return statusOf(n)
	})

	var (
		mu     sync.Mutex
		events []reporter.Event
	)
	rep := reporter.Func(func(_ context.Context, ev reporter.Event) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	})

	s := state.New()
	r := New(p, s,
		WithInterval(time.Microsecond),
		WithTimeout(time.Second),
		WithReporter(rep),
	)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		r.Run(ctx)
	}()

	const workers, perWorker = 8, 200
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				r.Check(context.Background())
			}
		}()
	}
	wg.Wait()
	// Stop Run between evaluations: a tick interrupted by cancellation
	// discards its result by design, which would make "last probe call" and
	// "last stored evaluation" differ. Holding the evaluation lock guarantees
	// no tick is in flight; later ticks see the canceled ctx and skip.
	r.mu.Lock()
	cancel()
	r.mu.Unlock()
	<-runDone

	total := seq.Load()
	if total < workers*perWorker {
		t.Fatalf("evaluations = %d, want >= %d", total, workers*perWorker)
	}
	if got, want := s.Get(), statusOf(last.Load()); got != want {
		t.Fatalf("final state = %v, want %v (last evaluation #%d)", got, want, last.Load())
	}
	if last.Load() != total {
		t.Fatalf("last evaluation = #%d, want #%d", last.Load(), total)
	}

	mu.Lock()
	defer mu.Unlock()
	if int64(len(events)) != total {
		t.Fatalf("events = %d, want %d (one per evaluation)", len(events), total)
	}
	prev := probe.StatusUnknown
	for i, ev := range events {
		want := statusOf(int64(i + 1))
		if ev.Prev != prev || ev.Cur != want {
			t.Fatalf("event[%d] = %v->%v, want %v->%v", i, ev.Prev, ev.Cur, prev, want)
		}
		prev = ev.Cur
	}
}
