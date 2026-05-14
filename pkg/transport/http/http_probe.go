package httpprobe

import (
	"net/http"

	"github.com/gopherex/xprobe/pkg/probe"
)

// Conventional Kubernetes paths.
const (
	DefaultLivenessPath  = "/healthz/liveness"
	DefaultReadinessPath = "/healthz/readiness"
	DefaultStartupPath   = "/healthz/startup"
)

// HTTPProbe binds a Handler to a path and method.
type HTTPProbe struct {
	path   string
	method string
	http.Handler
}

func (p *HTTPProbe) Path() string   { return p.path }
func (p *HTTPProbe) Method() string { return p.method }

// NewHTTPProbe constructs an HTTPProbe served under method GET at path.
func NewHTTPProbe(path string, h http.Handler) *HTTPProbe {
	return &HTTPProbe{path: path, method: http.MethodGet, Handler: h}
}

// Liveness builds an HTTPProbe at DefaultLivenessPath named "liveness".
func Liveness(p probe.Probe, opts ...Option) *HTTPProbe {
	opts = append([]Option{WithName("liveness")}, opts...)
	return NewHTTPProbe(DefaultLivenessPath, Handler(p, opts...))
}

// Readiness builds an HTTPProbe at DefaultReadinessPath named "readiness".
func Readiness(p probe.Probe, opts ...Option) *HTTPProbe {
	opts = append([]Option{WithName("readiness")}, opts...)
	return NewHTTPProbe(DefaultReadinessPath, Handler(p, opts...))
}

// Startup builds an HTTPProbe at DefaultStartupPath named "startup".
func Startup(p probe.Probe, opts ...Option) *HTTPProbe {
	opts = append([]Option{WithName("startup")}, opts...)
	return NewHTTPProbe(DefaultStartupPath, Handler(p, opts...))
}

// Mux mounts each HTTPProbe on a fresh http.ServeMux.
func Mux(probes ...*HTTPProbe) *http.ServeMux {
	mux := http.NewServeMux()
	for _, hp := range probes {
		mux.Handle(hp.path, hp.Handler)
	}
	return mux
}
