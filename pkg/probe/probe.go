// Package probe defines the core probe abstractions: a status enum,
// the Probe interface, function adapters and composition helpers.
package probe

import "context"

// Status represents the outcome of a probe check.
//
// Zero value is StatusUnknown so a never-checked probe is distinguishable
// from one that has been checked and reported StatusUp.
type Status uint8

const (
	StatusUnknown Status = iota
	StatusUp
	StatusDown
	StatusTimeout
)

// OK reports whether the status is StatusUp.
func (s Status) OK() bool { return s == StatusUp }

func (s Status) String() string {
	switch s {
	case StatusUp:
		return "up"
	case StatusDown:
		return "down"
	case StatusTimeout:
		return "timeout"
	default:
		return "unknown"
	}
}

// Probe is anything that can report a health Status given a context.
//
// Implementations MUST honor ctx cancellation: long-running checks (network
// I/O, external dependencies) must abort and return promptly when ctx is
// canceled. Callers such as runner.Runner and the HTTP/gRPC transports spawn
// a goroutine per check and rely on ctx to bound its lifetime — a probe that
// ignores ctx will leak a goroutine on every timeout.
//
// The returned Status should reflect the most recent observation. Returning
// StatusUnknown is appropriate only when no determination could be made
// (e.g. ctx canceled before any work).
type Probe interface {
	Check(ctx context.Context) Status
}

// Func adapts a plain function into a Probe.
type Func func(ctx context.Context) Status

func (f Func) Check(ctx context.Context) Status { return f(ctx) }

// FromError wraps an error-returning function: nil error -> StatusUp, otherwise StatusDown.
func FromError(f func(ctx context.Context) error) Probe {
	return Func(func(ctx context.Context) Status {
		if f(ctx) != nil {
			return StatusDown
		}
		return StatusUp
	})
}

// Named attaches a human-readable name to an underlying Probe.
type Named struct {
	Probe
	name string
}

func WithName(name string, p Probe) *Named { return &Named{Probe: p, name: name} }

func (n *Named) Name() string { return n.name }
