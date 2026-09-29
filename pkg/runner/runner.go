// Package runner periodically executes a Probe and pushes the result into a
// State, invoking Reporters on transitions. Decouples probe execution from
// transport — transports read cached State instead of running checks
// per-request (matters for gRPC Watch, expensive checks, or bursty traffic).
package runner

import (
	"context"
	"sync"
	"time"

	"github.com/gopherex/xprobe/pkg/probe"
	"github.com/gopherex/xprobe/pkg/reporter"
	"github.com/gopherex/xprobe/pkg/state"
)

const (
	DefaultInterval = 5 * time.Second
	DefaultTimeout  = 2 * time.Second
)

// Runner polls a probe.Probe and updates a state.State.
//
// Every evaluation — a scheduled tick or an explicit Check — runs under one
// mutex held across the probe call, the State update and the reporter call,
// so results land in State (and reach the reporter) strictly in evaluation
// order. A Runner must not be copied after first use.
type Runner struct {
	mu        sync.Mutex // serializes evaluations
	name      string
	probe     probe.Probe
	state     *state.State
	interval  time.Duration
	timeout   time.Duration
	reporter  reporter.Reporter
	initialOK bool
}

// Option configures a Runner.
type Option func(*Runner)

func WithName(n string) Option            { return func(r *Runner) { r.name = n } }
func WithInterval(d time.Duration) Option { return func(r *Runner) { r.interval = d } }
func WithTimeout(d time.Duration) Option  { return func(r *Runner) { r.timeout = d } }
func WithReporter(rep reporter.Reporter) Option {
	return func(r *Runner) { r.reporter = rep }
}

// RunImmediately performs an initial check before the first tick.
// Default: false (state stays StatusUnknown until first tick).
func RunImmediately() Option { return func(r *Runner) { r.initialOK = true } }

// New constructs a Runner. The provided state is updated on every tick.
func New(p probe.Probe, s *state.State, opts ...Option) *Runner {
	r := &Runner{
		probe:    p,
		state:    s,
		interval: DefaultInterval,
		timeout:  DefaultTimeout,
		reporter: reporter.Nop{},
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Run blocks until ctx is canceled, periodically checking the probe.
func (r *Runner) Run(ctx context.Context) {
	if r.initialOK {
		r.tick(ctx)
	}

	t := time.NewTicker(r.interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.tick(ctx)
		}
	}
}

// Start launches Run in a goroutine and returns immediately.
func (r *Runner) Start(ctx context.Context) {
	go r.Run(ctx)
}

// Check evaluates the probe right now, outside the tick schedule, and returns
// the resulting status. It behaves exactly like a tick: the probe is bounded
// by the configured timeout (StatusTimeout on expiry), the result is stored
// in State, and the reporter fires on a transition.
//
// Check is safe to call before Run and concurrently with it; evaluations are
// serialized, so a Check waits for an in-flight tick (and vice versa) and the
// State always holds the result of the latest evaluation.
//
// If ctx is canceled (before or during the evaluation), nothing is stored
// and no reporter fires — as with a tick on shutdown — and Check returns the
// current cached status: StatusUnknown if State has never been set.
func (r *Runner) Check(ctx context.Context) probe.Status {
	r.mu.Lock()
	defer r.mu.Unlock()

	if s, ok := r.evaluate(ctx); ok {
		return s
	}
	return r.state.Get()
}

func (r *Runner) tick(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.evaluate(ctx)
}

// evaluate runs the probe, stores the result and reports a transition.
// It reports false, storing nothing, if ctx is canceled. Callers must hold
// r.mu.
func (r *Runner) evaluate(ctx context.Context) (probe.Status, bool) {
	if ctx.Err() != nil {
		return probe.StatusUnknown, false
	}

	checkCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	ch := make(chan probe.Result, 1)
	go func() { ch <- probe.Evaluate(checkCtx, r.probe) }()

	var result probe.Result
	select {
	case <-checkCtx.Done():
		if ctx.Err() != nil {
			return probe.StatusUnknown, false
		}
		result = probe.Result{Status: probe.StatusTimeout, Reason: checkCtx.Err().Error()}
	case result = <-ch:
	}

	if ctx.Err() != nil {
		return probe.StatusUnknown, false
	}
	prev, changed := r.state.SetResult(result)
	if changed {
		r.reporter.OnStatus(ctx, reporter.Event{Name: r.name, Prev: prev.Status, Cur: result.Status, PrevReason: prev.Reason, Reason: result.Reason})
	}
	return result.Status, true
}
