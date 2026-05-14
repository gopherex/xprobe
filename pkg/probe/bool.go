package probe

import (
	"context"
	"sync/atomic"
)

// Bool is a probe whose Status is toggled by external code via Set.
// Safe for concurrent use.
type Bool struct {
	healthy atomic.Bool
}

func NewBool() *Bool { return &Bool{} }

// Set updates the underlying health flag.
func (b *Bool) Set(v bool) { b.healthy.Store(v) }

// Get returns the current flag value.
func (b *Bool) Get() bool { return b.healthy.Load() }

func (b *Bool) Check(_ context.Context) Status {
	if b.healthy.Load() {
		return StatusUp
	}
	return StatusDown
}
