// Package runner periodically executes a Probe and pushes the result into a
// State, invoking Reporters on transitions. Decouples probe execution from
// transport — transports read cached State instead of running checks
// per-request (matters for gRPC Watch, expensive checks, or bursty traffic).
package runner

import (
	"context"
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
type Runner struct {
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

func (r *Runner) tick(ctx context.Context) {
	checkCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	ch := make(chan probe.Status, 1)
	go func() { ch <- r.probe.Check(checkCtx) }()

	var s probe.Status
	select {
	case <-checkCtx.Done():
		if ctx.Err() != nil {
			return
		}
		s = probe.StatusTimeout
	case s = <-ch:
	}

	prev, changed := r.state.Set(s)
	if changed {
		r.reporter.OnStatus(ctx, reporter.Event{Name: r.name, Prev: prev, Cur: s})
	}
}
