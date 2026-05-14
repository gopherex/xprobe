package httpprobe

import (
	"encoding/json"
	"net/http"

	"github.com/gopherex/xprobe/pkg/probe"
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
		st := s.Get()
		code := codeFor(st)

		if o.json {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(struct {
				Name   string `json:"name,omitempty"`
				Status string `json:"status"`
			}{Name: o.name, Status: st.String()})
			return
		}

		w.WriteHeader(code)
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
		_, _ = w.Write([]byte(body))
	}
}
