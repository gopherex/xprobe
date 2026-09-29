package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gopherex/xprobe/pkg/probe"
	"github.com/gopherex/xprobe/pkg/reporter"
	"github.com/gopherex/xprobe/pkg/runner"
	"github.com/gopherex/xprobe/pkg/state"
	httpprobe "github.com/gopherex/xprobe/pkg/transport/http"
)

func TestReasonsThroughRunnerAndHTTP(t *testing.T) {
	t.Parallel()
	reason := "connection refused"
	var checks int
	p := probe.WithName("database", probe.FromError(func(context.Context) error {
		checks++
		if reason == "" {
			return nil
		}
		return errors.New(reason)
	}))
	cache := state.New()
	var events []reporter.Event
	r := runner.New(probe.All(p), cache, runner.WithReporter(reporter.Func(func(_ context.Context, e reporter.Event) { events = append(events, e) })))
	for _, next := range []string{"connection refused", "authentication failed", ""} {
		reason = next
		r.Check(t.Context())
		rec := httptest.NewRecorder()
		httpprobe.CachedHandler(cache, httpprobe.AsJSON())(rec, httptest.NewRequest("GET", "/", nil))
		var body struct{ Status, Reason string }
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		want := next
		if want != "" {
			want = "database: " + want
		}
		if body.Reason != want {
			t.Fatalf("reason %q, want %q", body.Reason, want)
		}
		if next == "" && (rec.Code != 200 || body.Status != "up") {
			t.Fatalf("recovery: %s", rec.Body)
		}
	}
	if checks != 3 {
		t.Fatalf("expected one check per evaluation, got %d", checks)
	}
	if len(events) != 3 || events[1].Prev != events[1].Cur || events[1].Reason != "database: authentication failed" || events[2].Reason != "" {
		t.Fatalf("events: %+v", events)
	}
}

func TestCompositeReasonsAndLegacy(t *testing.T) {
	t.Parallel()
	bad := probe.WithName("db", probe.FromError(func(context.Context) error { return errors.New("offline") }))
	legacy := probe.WithName("cache", probe.Func(func(context.Context) probe.Status { return probe.StatusDown }))
	r := probe.All(bad, legacy).CheckResult(t.Context())
	if r.Status != probe.StatusDown || r.Reason != "db: offline; cache: down" {
		t.Fatalf("result: %+v", r)
	}
	healthy := probe.Func(func(context.Context) probe.Status { return probe.StatusUp })
	r = probe.Any(bad, healthy).CheckResult(t.Context())
	if r.Status != probe.StatusUp || r.Reason != "" {
		t.Fatalf("any: %+v", r)
	}
	rec := httptest.NewRecorder()
	httpprobe.Handler(bad, httpprobe.AsJSON())(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != 503 || rec.Body.String() != "{\"status\":\"down\",\"reason\":\"db: offline\"}\n" {
		t.Fatalf("pull: %d %s", rec.Code, rec.Body)
	}
}

func TestTimeoutAndCancellationDetails(t *testing.T) {
	t.Parallel()
	p := probe.ResultFunc(func(ctx context.Context) probe.Result {
		<-ctx.Done()
		return probe.Result{Status: probe.StatusTimeout, Reason: ctx.Err().Error()}
	})
	s := state.New()
	r := runner.New(p, s, runner.WithTimeout(time.Millisecond))
	r.Check(t.Context())
	if got := s.Result(); got.Status != probe.StatusTimeout || got.Reason != context.DeadlineExceeded.Error() {
		t.Fatalf("timeout: %+v", got)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := s.Result()
	r.Check(ctx)
	if s.Result() != before {
		t.Fatal("cancellation changed cache")
	}
	s.Set(probe.StatusUp)
	if s.Result().Reason != "" {
		t.Fatal("Set kept stale reason")
	}
}

func TestConcurrentReasonSnapshots(t *testing.T) {
	t.Parallel()
	b := probe.NewBool()
	done := make(chan struct{})
	var invalid atomic.Bool
	go func() {
		defer close(done)
		for range 10000 {
			b.SetReason(false, "offline")
			b.Set(true)
		}
	}()
	for range 10000 {
		r := probe.Evaluate(t.Context(), b)
		if r.Status == probe.StatusUp && r.Reason != "" {
			invalid.Store(true)
		}
	}
	<-done
	if invalid.Load() {
		t.Fatal("torn status/reason snapshot")
	}
}
