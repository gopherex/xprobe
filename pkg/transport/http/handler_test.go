package httpprobe

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gopherex/xprobe/pkg/probe"
)

func static(s probe.Status) probe.Probe {
	return probe.Func(func(context.Context) probe.Status { return s })
}

func TestHandlerUp(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()
	Handler(static(probe.StatusUp), WithName("svc")).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rr.Code)
	}
	if rr.Body.String() != "Healthy" {
		t.Fatalf("body = %q", rr.Body.String())
	}
}

func TestHandlerDown(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()
	Handler(static(probe.StatusDown), WithName("svc")).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Unhealthy svc") {
		t.Fatalf("body = %q", rr.Body.String())
	}
}

func TestHandlerTimeoutCode(t *testing.T) {
	t.Parallel()
	slow := probe.Func(func(ctx context.Context) probe.Status {
		<-ctx.Done()
		return probe.StatusDown
	})
	rr := httptest.NewRecorder()
	Handler(slow, WithTimeout(5*time.Millisecond)).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusGatewayTimeout {
		t.Fatalf("code = %d, want 504", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "timeout") {
		t.Fatalf("body = %q", rr.Body.String())
	}
}

func TestHandlerJSON(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()
	Handler(static(probe.StatusUp), WithName("svc"), AsJSON()).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	var got struct {
		Name, Status string
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "svc" || got.Status != "up" {
		t.Fatalf("payload = %+v", got)
	}
}

func TestWaitProbeTimeout(t *testing.T) {
	t.Parallel()
	hang := probe.Func(func(ctx context.Context) probe.Status {
		<-ctx.Done()
		return probe.StatusDown
	})
	if got := WaitProbe(context.Background(), hang, 5*time.Millisecond); got != probe.StatusTimeout {
		t.Fatalf("got %v, want timeout", got)
	}
}

func TestMuxRouting(t *testing.T) {
	t.Parallel()
	live := Liveness(static(probe.StatusUp))
	ready := Readiness(static(probe.StatusDown))
	mux := Mux(live, ready)

	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + DefaultLivenessPath)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "Healthy" {
		t.Fatalf("liveness: %d %q", resp.StatusCode, body)
	}

	resp, err = http.Get(srv.URL + DefaultReadinessPath)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 503 || !strings.Contains(string(body), "readiness") {
		t.Fatalf("readiness: %d %q", resp.StatusCode, body)
	}
}
