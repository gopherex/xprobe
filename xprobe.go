// Package xprobe provides composable, transport-agnostic health probes.
//
// Layout:
//
//	pkg/probe     — core types (Status, Probe, Composite)
//	pkg/state     — cached status with pub/sub (used by gRPC Watch and cached HTTP)
//	pkg/runner    — periodic poller pushing Probe results into State
//	pkg/reporter  — side-effect hooks invoked on status transitions
//	pkg/transport/http  — HTTP handlers (pull or cached)
//	pkg/transport/grpc  — grpc.health.v1 server (separate go.mod)
//
// The root re-exports the most common identifiers for ergonomic use.
package xprobe

import (
	"context"
	"net/http"

	"github.com/gopherex/xprobe/pkg/probe"
	httpprobe "github.com/gopherex/xprobe/pkg/transport/http"
)

type (
	Status     = probe.Status
	Probe      = probe.Probe
	Func       = probe.Func
	Composite  = probe.Composite
	Bool       = probe.Bool
	HTTPProbe  = httpprobe.HTTPProbe
	HTTPOption = httpprobe.Option
)

const (
	StatusUnknown = probe.StatusUnknown
	StatusUp      = probe.StatusUp
	StatusDown    = probe.StatusDown
	StatusTimeout = probe.StatusTimeout
)

func NewBool() *Bool                                    { return probe.NewBool() }
func FromError(f func(ctx context.Context) error) Probe { return probe.FromError(f) }
func All(probes ...Probe) *Composite                    { return probe.All(probes...) }
func Any(probes ...Probe) *Composite                    { return probe.Any(probes...) }
func Liveness(p Probe, opts ...HTTPOption) *HTTPProbe   { return httpprobe.Liveness(p, opts...) }
func Readiness(p Probe, opts ...HTTPOption) *HTTPProbe  { return httpprobe.Readiness(p, opts...) }
func Startup(p Probe, opts ...HTTPOption) *HTTPProbe    { return httpprobe.Startup(p, opts...) }
func Mux(probes ...*HTTPProbe) *http.ServeMux           { return httpprobe.Mux(probes...) }
