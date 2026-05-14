package probe

import (
	"context"
	"sync"
)

// Mode controls how a Composite aggregates child probe statuses.
type Mode uint8

const (
	// ModeAll requires every child probe to be StatusUp.
	// The reported status is the worst observed.
	ModeAll Mode = iota
	// ModeAny requires at least one child probe to be StatusUp.
	// The reported status is the best observed.
	ModeAny
)

// Composite runs multiple probes concurrently and aggregates their results.
type Composite struct {
	mu      sync.Mutex
	probes  []Probe
	mode    Mode
	factory AsyncerFactory
}

// CompositeOption configures a Composite at construction time.
type CompositeOption func(*Composite)

// WithAsyncer overrides the default AsyncerFactory used to run child probes.
func WithAsyncer(f AsyncerFactory) CompositeOption {
	return func(c *Composite) { c.factory = f }
}

// New builds a Composite with the given mode and options.
func New(mode Mode, opts ...CompositeOption) *Composite {
	c := &Composite{mode: mode, factory: PoolFactory(0)}
	for _, o := range opts {
		o(c)
	}
	return c
}

// All returns a Composite that requires every probe to be StatusUp.
func All(probes ...Probe) *Composite {
	c := New(ModeAll)
	c.probes = append(c.probes, probes...)
	return c
}

// Any returns a Composite that succeeds when at least one probe is StatusUp.
func Any(probes ...Probe) *Composite {
	c := New(ModeAny)
	c.probes = append(c.probes, probes...)
	return c
}

// Add appends a probe to the Composite. Safe for concurrent use.
func (c *Composite) Add(p Probe) *Composite {
	c.mu.Lock()
	c.probes = append(c.probes, p)
	c.mu.Unlock()
	return c
}

// Len reports the current number of child probes.
func (c *Composite) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.probes)
}

func (c *Composite) snapshot() []Probe {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Probe, len(c.probes))
	copy(out, c.probes)
	return out
}

// Check runs all child probes concurrently and aggregates by Mode.
// Empty composites report StatusUp.
func (c *Composite) Check(ctx context.Context) Status {
	probes := c.snapshot()
	if len(probes) == 0 {
		return StatusUp
	}

	statuses := make([]Status, len(probes))
	async := c.factory()
	for i, p := range probes {
		async.Go(func() { statuses[i] = p.Check(ctx) })
	}
	async.Wait()

	return reduce(statuses, c.mode)
}

func reduce(statuses []Status, mode Mode) Status {
	switch mode {
	case ModeAll:
		worst := StatusUp
		for _, s := range statuses {
			if rank(s) > rank(worst) {
				worst = s
			}
		}
		return worst
	case ModeAny:
		best := statuses[0]
		for _, s := range statuses[1:] {
			if rank(s) < rank(best) {
				best = s
			}
		}
		return best
	}
	return StatusUnknown
}

// rank: lower is healthier. Up < Unknown < Down < Timeout.
func rank(s Status) int {
	switch s {
	case StatusUp:
		return 0
	case StatusUnknown:
		return 1
	case StatusDown:
		return 2
	case StatusTimeout:
		return 3
	}
	return 4
}
