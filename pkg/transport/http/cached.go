package httpprobe

import (
	"net/http"

	"github.com/gopherex/xprobe/pkg/state"
)

// CachedHandler returns an http.HandlerFunc that reads from a state.State
// without invoking any probe. Pair with a runner.Runner that periodically
// updates the state. Response codes match the pull-mode Handler.
//
// Status is read instantly — no timeout applies. If the state has never been
// updated (StatusUnknown), the response is 503.
func CachedHandler(s *state.State, opts ...Option) http.HandlerFunc {
	o := handlerOpts{}
	for _, f := range opts {
		f(&o)
	}

	return func(w http.ResponseWriter, _ *http.Request) {
		render(w, s.Result(), o)
	}
}
