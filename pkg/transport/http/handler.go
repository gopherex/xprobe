// Package httpprobe exposes probes over HTTP with sensible defaults for
// Kubernetes liveness/readiness/startup checks, but generic enough for any
// service. It supports two modes:
//
//   - Pull: Handler runs the probe synchronously per request (default).
//   - Cached: CachedHandler reads a state.State updated by a runner.Runner
//     out-of-band — required for expensive checks, gRPC parity, or when
//     many concurrent requests would otherwise stampede the probe.
package httpprobe

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gopherex/xprobe/pkg/probe"
)

// DefaultTimeout caps each probe check served via Handler.
const DefaultTimeout = 30 * time.Second

type handlerOpts struct {
	name    string
	timeout time.Duration
	json    bool
}

// Option configures Handler behavior.
type Option func(*handlerOpts)

// WithName tags the handler with a name surfaced in responses.
func WithName(name string) Option { return func(o *handlerOpts) { o.name = name } }

// WithTimeout overrides the per-request probe deadline. Non-positive values
// are ignored.
func WithTimeout(d time.Duration) Option {
	return func(o *handlerOpts) {
		if d > 0 {
			o.timeout = d
		}
	}
}

// AsJSON switches the response body to JSON {name,status}.
func AsJSON() Option { return func(o *handlerOpts) { o.json = true } }

// WaitProbe runs probe.Check under a derived context with the given timeout.
// On deadline expiry, StatusTimeout is returned; the probe goroutine may
// outlive the call but will not block the caller.
func WaitProbe(ctx context.Context, p probe.Probe, timeout time.Duration) probe.Status {
	return WaitResult(ctx, p, timeout).Status
}

// WaitResult bounds one check and preserves its reason.
func WaitResult(ctx context.Context, p probe.Probe, timeout time.Duration) probe.Result {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ch := make(chan probe.Result, 1)
	go func() { ch <- probe.Evaluate(waitCtx, p) }()
	select {
	case <-waitCtx.Done():
		return probe.Result{Status: probe.StatusTimeout, Reason: waitCtx.Err().Error()}
	case r := <-ch:
		return r
	}
}

// Handler returns an http.HandlerFunc that runs the probe and renders
// the result. Status -> HTTP code: Up=200, Timeout=504, others=503.
func Handler(p probe.Probe, opts ...Option) http.HandlerFunc {
	o := handlerOpts{timeout: DefaultTimeout}
	for _, f := range opts {
		f(&o)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		render(w, WaitResult(r.Context(), p, o.timeout), o)
	}
}

func render(w http.ResponseWriter, r probe.Result, o handlerOpts) {
	st := r.Status
	if o.json {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(codeFor(st))
		_ = json.NewEncoder(w).Encode(struct {
			Name   string `json:"name,omitempty"`
			Status string `json:"status"`
			Reason string `json:"reason,omitempty"`
		}{Name: o.name, Status: st.String(), Reason: r.Reason})
		return
	}
	w.WriteHeader(codeFor(st))
	if st == probe.StatusUp {
		_, _ = w.Write([]byte("Healthy"))
		return
	}
	body := "Unhealthy"
	if o.name != "" {
		body += " " + o.name
	}
	if st == probe.StatusTimeout {
		body += " (timeout)"
	}
	if r.Reason != "" {
		body += ": " + r.Reason
	}
	_, _ = w.Write([]byte(body))
}

func codeFor(s probe.Status) int {
	switch s {
	case probe.StatusUp:
		return http.StatusOK
	case probe.StatusTimeout:
		return http.StatusGatewayTimeout
	default:
		return http.StatusServiceUnavailable
	}
}
