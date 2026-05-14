// Package reporter defines a side-effect interface invoked on probe status
// transitions. Implementations: logging, metrics emission, alerting hooks.
//
// Reporters are called synchronously from the runner.Runner tick goroutine.
// A slow reporter blocks the next tick — keep work fast or dispatch to a
// worker pool inside your implementation. The Multi adapter calls children
// sequentially for the same reason.
package reporter

import (
	"context"

	"github.com/gopherex/xprobe/pkg/probe"
)

// Event describes a probe status transition.
//
// Fields are extensible: future additions (e.g. duration, last error) can
// be appended without breaking existing Reporter implementations.
type Event struct {
	// Name is the probe identifier set via runner.WithName.
	// Empty when the runner has no name configured.
	Name string
	// Prev is the status before the transition.
	Prev probe.Status
	// Cur is the new status.
	Cur probe.Status
}

// Reporter is invoked when a probe's cached status changes.
type Reporter interface {
	OnStatus(ctx context.Context, ev Event)
}

// Func adapts a plain function into a Reporter.
type Func func(ctx context.Context, ev Event)

func (f Func) OnStatus(ctx context.Context, ev Event) { f(ctx, ev) }

// Multi fans out to several reporters sequentially.
type Multi []Reporter

func (m Multi) OnStatus(ctx context.Context, ev Event) {
	for _, r := range m {
		r.OnStatus(ctx, ev)
	}
}

// Nop discards all reports.
type Nop struct{}

func (Nop) OnStatus(context.Context, Event) {}
