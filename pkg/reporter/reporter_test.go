package reporter

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/gopherex/xprobe/pkg/probe"
)

func TestFunc(t *testing.T) {
	t.Parallel()
	var got probe.Status
	r := Func(func(_ context.Context, ev Event) { got = ev.Cur })
	r.OnStatus(context.Background(), Event{Name: "x", Prev: probe.StatusUnknown, Cur: probe.StatusUp})
	if got != probe.StatusUp {
		t.Fatalf("got %v", got)
	}
}

func TestMulti(t *testing.T) {
	t.Parallel()
	var n atomic.Int32
	inc := Func(func(context.Context, Event) { n.Add(1) })
	Multi{inc, inc, inc}.OnStatus(context.Background(), Event{})
	if n.Load() != 3 {
		t.Fatalf("n = %d", n.Load())
	}
}

func TestNop(t *testing.T) {
	t.Parallel()
	Nop{}.OnStatus(context.Background(), Event{})
}
