package probe

import "context"

// Result is one observation. Reason explains a non-Up result; it is empty
// after recovery. Reasons can be exposed by HTTP handlers and reporters.
type Result struct {
	Status Status
	Reason string
}

// Detailed is an optional extension of Probe. Evaluate calls CheckResult
// instead of Check, so status and reason always describe the same observation.
type Detailed interface {
	Probe
	CheckResult(context.Context) Result
}

// ResultFunc adapts a detailed check without breaking the Probe contract.
type ResultFunc func(context.Context) Result

func (f ResultFunc) Check(ctx context.Context) Status       { return f.CheckResult(ctx).Status }
func (f ResultFunc) CheckResult(ctx context.Context) Result { return f(ctx).Normalized() }

// Normalized clears obsolete reasons on healthy results.
func (r Result) Normalized() Result {
	if r.Status == StatusUp {
		r.Reason = ""
	}
	return r
}

// Evaluate performs exactly one check, preserving details where available.
func Evaluate(ctx context.Context, p Probe) Result {
	if d, ok := p.(Detailed); ok {
		return d.CheckResult(ctx).Normalized()
	}
	return Result{Status: p.Check(ctx)}
}
func (n *Named) CheckResult(ctx context.Context) Result {
	r := Evaluate(ctx, n.Probe)
	if !r.Status.OK() && n.name != "" {
		if r.Reason == "" {
			r.Reason = r.Status.String()
		}
		r.Reason = n.name + ": " + r.Reason
	}
	return r
}
