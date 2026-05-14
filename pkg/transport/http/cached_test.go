package httpprobe

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gopherex/xprobe/pkg/probe"
	"github.com/gopherex/xprobe/pkg/state"
)

func TestCachedHandler(t *testing.T) {
	t.Parallel()
	s := state.New()
	h := CachedHandler(s, WithName("svc"))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("unknown -> code %d, want 503", rr.Code)
	}

	s.Set(probe.StatusUp)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK || rr.Body.String() != "Healthy" {
		t.Fatalf("up -> %d %q", rr.Code, rr.Body.String())
	}

	s.Set(probe.StatusTimeout)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusGatewayTimeout || !strings.Contains(rr.Body.String(), "timeout") {
		t.Fatalf("timeout -> %d %q", rr.Code, rr.Body.String())
	}
}
