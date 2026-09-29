package probe

import (
	"context"
	"sync/atomic"
)

// Bool is a toggleable probe. Its zero value is down. Status and reason are
// stored atomically and may be read or updated concurrently.
type Bool struct{ result atomic.Pointer[Result] }

func NewBool() *Bool { return &Bool{} }

// Set updates health and clears any previous reason.
func (b *Bool) Set(v bool) { b.SetReason(v, "") }

// SetReason explains an unhealthy flag. A healthy flag always clears reason.
func (b *Bool) SetReason(v bool, reason string) {
	r := Result{Status: StatusDown, Reason: reason}
	if v {
		r = Result{Status: StatusUp}
	}
	b.result.Store(&r)
}
func (b *Bool) Get() bool                        { return b.CheckResult(context.Background()).Status.OK() }
func (b *Bool) Check(ctx context.Context) Status { return b.CheckResult(ctx).Status }
func (b *Bool) CheckResult(context.Context) Result {
	if r := b.result.Load(); r != nil {
		return *r
	}
	return Result{Status: StatusDown}
}
