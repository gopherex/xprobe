package state

import (
	"context"
	"testing"
	"time"

	"github.com/gopherex/xprobe/pkg/probe"
)

func TestStateGetSet(t *testing.T) {
	t.Parallel()
	s := New()
	if got := s.Get(); got != probe.StatusUnknown {
		t.Fatalf("zero state = %v, want unknown", got)
	}
	prev, changed := s.Set(probe.StatusUp)
	if !changed || prev != probe.StatusUnknown {
		t.Fatalf("Set(up) prev=%v changed=%v", prev, changed)
	}
	if _, changed := s.Set(probe.StatusUp); changed {
		t.Fatal("Set same value must report changed=false")
	}
}

func TestStateSubscribeInitial(t *testing.T) {
	t.Parallel()
	s := New()
	s.Set(probe.StatusUp)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.Subscribe(ctx)

	select {
	case got := <-ch:
		if got != probe.StatusUp {
			t.Fatalf("initial = %v, want up", got)
		}
	case <-time.After(time.Second):
		t.Fatal("no initial value delivered")
	}
}

func TestStateSubscribeUpdates(t *testing.T) {
	t.Parallel()
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.Subscribe(ctx)
	<-ch // drain initial unknown

	s.Set(probe.StatusUp)
	select {
	case got := <-ch:
		if got != probe.StatusUp {
			t.Fatalf("got %v, want up", got)
		}
	case <-time.After(time.Second):
		t.Fatal("no update delivered")
	}
}

func TestStateSubscribeCloseOnCtx(t *testing.T) {
	t.Parallel()
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	ch := s.Subscribe(ctx)
	<-ch
	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel must close after ctx cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("channel did not close")
	}
}

func TestStateLatestWins(t *testing.T) {
	t.Parallel()
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.Subscribe(ctx)
	<-ch

	// Flood without reading; only the latest must survive.
	s.Set(probe.StatusDown)
	s.Set(probe.StatusUp)
	s.Set(probe.StatusDown)

	// Allow notifications to flush.
	time.Sleep(10 * time.Millisecond)
	got := <-ch
	if got != probe.StatusDown {
		t.Fatalf("latest = %v, want down", got)
	}
}

func TestStateConcurrentSetOrdering(t *testing.T) {
	t.Parallel()
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.Subscribe(ctx)
	<-ch // drain initial

	// Hammer Set from many goroutines. After all done, the last value
	// observed by the subscriber must equal s.Get() — i.e., notification
	// order must agree with final status.
	const N = 200
	var wg = make(chan struct{}, N)
	for i := range N {
		go func(i int) {
			defer func() { wg <- struct{}{} }()
			if i%2 == 0 {
				s.Set(probe.StatusUp)
			} else {
				s.Set(probe.StatusDown)
			}
		}(i)
	}
	for range N {
		<-wg
	}

	// Drain channel until it stabilizes — last received must equal Get.
	var last probe.Status = s.Get()
	deadline := time.After(100 * time.Millisecond)
drain:
	for {
		select {
		case v := <-ch:
			last = v
		case <-deadline:
			break drain
		}
	}
	if last != s.Get() {
		t.Fatalf("subscriber last=%v, state.Get=%v — notification order diverged from final status", last, s.Get())
	}
}

func TestStateHasBeenSet(t *testing.T) {
	t.Parallel()
	s := New()
	if s.HasBeenSet() {
		t.Fatal("fresh State must report HasBeenSet=false")
	}
	s.Set(probe.StatusUp)
	if !s.HasBeenSet() {
		t.Fatal("after Set, HasBeenSet must be true")
	}
}

func TestRegistry(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	a := r.Get("a")
	b := r.Get("b")
	if a == b {
		t.Fatal("different names must yield different States")
	}
	if r.Get("a") != a {
		t.Fatal("repeated Get must return same State")
	}
	if _, ok := r.Lookup("missing"); ok {
		t.Fatal("Lookup must not create")
	}
	names := r.Names()
	if len(names) != 2 {
		t.Fatalf("Names len = %d, want 2", len(names))
	}
}
