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
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ch := make(chan probe.Status, 1)
	go func() { ch <- p.Check(waitCtx) }()

	select {
	case <-waitCtx.Done():
		return probe.StatusTimeout
	case s := <-ch:
		return s
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
		s := WaitProbe(r.Context(), p, o.timeout)
		code := codeFor(s)

		if o.json {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(struct {
				Name   string `json:"name,omitempty"`
				Status string `json:"status"`
			}{Name: o.name, Status: s.String()})
			return
		}

		w.WriteHeader(code)
		if s == probe.StatusUp {
			_, _ = w.Write([]byte("Healthy"))
			return
		}
		body := "Unhealthy"
		if o.name != "" {
			body += " " + o.name
		}
		if s == probe.StatusTimeout {
			body += " (timeout)"
		}
		_, _ = w.Write([]byte(body))
	}
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
