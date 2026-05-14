# xprobe

Composable, transport-agnostic health probes for Go services.
Pull-mode HTTP (Kubernetes liveness/readiness/startup), cached HTTP, and `grpc.health.v1` — all backed by the same probe primitives.

- **Stdlib-only core.** gRPC transport lives in a separate module so non-gRPC users don't pull `google.golang.org/grpc`.
- **Status taxonomy** distinguishes `Up` / `Down` / `Timeout` / `Unknown` — slow probes don't masquerade as failures.
- **Composite probes** with clear `All` (AND) / `Any` (OR) semantics, concurrent execution.
- **Cached state with pub/sub** — required for gRPC `Watch`, useful for expensive checks under burst traffic.
- **Pluggable reporters** (slog included) invoked only on status transitions.

Minimum Go version: **1.25.0**

---

## Install

Core (probes + HTTP transport):

```bash
go get github.com/gopherex/xprobe@latest
```

gRPC transport (optional, separate module):

```bash
go get github.com/gopherex/xprobe/pkg/transport/grpc@latest
```

---

## Quickstart — Kubernetes liveness/readiness

```go
package main

import (
	"context"
	"errors"
	"log"
	"net/http"

	"github.com/gopherex/xprobe"
)

func main() {
	live := xprobe.NewBool()
	live.Set(true)

	ready := xprobe.All(
		xprobe.FromError(pingDB),
		xprobe.FromError(pingCache),
	)

	mux := xprobe.Mux(
		xprobe.Liveness(live),
		xprobe.Readiness(ready),
	)

	log.Fatal(http.ListenAndServe(":8080", mux))
}

func pingDB(ctx context.Context) error    { return nil }
func pingCache(ctx context.Context) error { return errors.New("offline") }
```

Endpoints exposed:

| Path                   | Source                |
|------------------------|-----------------------|
| `/healthz/liveness`    | `NewBool().Set(true)` |
| `/healthz/readiness`   | DB AND cache up       |
| `/healthz/startup`     | (if you add one)      |

Response codes: `200 Healthy` / `503 Unhealthy <name>` / `504 Unhealthy <name> (timeout)`.

---

## Status taxonomy

```go
xprobe.StatusUnknown  // never checked (zero value)
xprobe.StatusUp       // healthy
xprobe.StatusDown     // probe returned a failure
xprobe.StatusTimeout  // probe exceeded its deadline
```

`Status.OK()` is `true` only for `StatusUp`.

---

## Defining probes

### From an error-returning func

```go
p := xprobe.FromError(func(ctx context.Context) error {
	return db.PingContext(ctx)
})
```

### Toggle flag

```go
b := xprobe.NewBool()
b.Set(true)   // mark up
b.Set(false)  // mark down
```

Useful as a liveness flag flipped during graceful shutdown.

### Custom

```go
p := xprobe.Func(func(ctx context.Context) xprobe.Status {
	if queueDepth() > 1000 {
		return xprobe.StatusDown
	}
	return xprobe.StatusUp
})
```

### Composition

```go
xprobe.All(p1, p2, p3)  // AND — worst status wins
xprobe.Any(p1, p2)      // OR  — best status wins
```

Children run concurrently. Customize concurrency:

```go
import "github.com/gopherex/xprobe/pkg/probe"

c := probe.New(probe.ModeAll, probe.WithAsyncer(probe.PoolFactory(4)))
c.Add(p1).Add(p2)
```

---

## HTTP — pull mode (default)

`Handler` runs the probe synchronously per request. Suitable for cheap checks (in-memory flag, simple ping).

```go
import httpprobe "github.com/gopherex/xprobe/pkg/transport/http"

h := httpprobe.Handler(p,
	httpprobe.WithName("db"),
	httpprobe.WithTimeout(2*time.Second),
	httpprobe.AsJSON(),  // optional JSON body
)
http.Handle("/healthz/db", h)
```

JSON output:

```json
{"name":"db","status":"up"}
```

---

## HTTP — cached mode

For expensive probes, or to share state with gRPC. A `Runner` polls the probe in the background and writes into a `State`; the handler reads from the State without ever invoking the probe.

```go
import (
	httpprobe "github.com/gopherex/xprobe/pkg/transport/http"
	"github.com/gopherex/xprobe/pkg/runner"
	"github.com/gopherex/xprobe/pkg/state"
)

s := state.New()
r := runner.New(myProbe, s,
	runner.WithName("db"),
	runner.WithInterval(5*time.Second),
	runner.WithTimeout(2*time.Second),
	runner.RunImmediately(),
)
r.Start(ctx)  // non-blocking

http.Handle("/healthz/db", httpprobe.CachedHandler(s, httpprobe.WithName("db")))
```

The probe runs at most once per interval, regardless of request volume.

---

## Reporters

Side-effect hooks invoked when cached status changes (never on every tick).

```go
import (
	"log/slog"
	"github.com/gopherex/xprobe/pkg/reporter"
	"github.com/gopherex/xprobe/pkg/runner"
)

r := runner.New(myProbe, s,
	runner.WithReporter(reporter.Slog(slog.Default())),
)
```

Built-in: `reporter.Slog`, `reporter.Func`, `reporter.Multi`, `reporter.Nop`.

Implement `reporter.Reporter` to plug Prometheus, OpenTelemetry, alerting, etc.

```go
type Event struct {
	Name string
	Prev probe.Status
	Cur  probe.Status
}

type Reporter interface {
	OnStatus(ctx context.Context, ev Event)
}
```

Reporters are called synchronously from the runner tick goroutine — a slow
reporter blocks the next tick. Dispatch to a worker pool if needed.

---

## gRPC — `grpc.health.v1`

The gRPC health protocol requires the server to know status synchronously and stream changes. The gRPC transport reads from a `state.Registry`; back it with `runner.Runner` per service.

```go
package main

import (
	"context"
	"net"

	"google.golang.org/grpc"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/gopherex/xprobe/pkg/probe"
	"github.com/gopherex/xprobe/pkg/runner"
	"github.com/gopherex/xprobe/pkg/state"
	grpcprobe "github.com/gopherex/xprobe/pkg/transport/grpc"
)

func main() {
	ctx := context.Background()

	reg := state.NewRegistry()

	// Overall service status: empty name "" per grpc.health.v1 convention.
	runner.New(myCompositeProbe, reg.Get(""),
		runner.WithInterval(5*time.Second),
		runner.RunImmediately(),
	).Start(ctx)

	// Per-component status.
	runner.New(probe.FromError(pingDB), reg.Get("db.Service"),
		runner.WithInterval(2*time.Second),
		runner.RunImmediately(),
	).Start(ctx)

	srv := grpc.NewServer()
	hv1.RegisterHealthServer(srv, grpcprobe.New(reg))

	lis, _ := net.Listen("tcp", ":9000")
	srv.Serve(lis)
}
```

Client usage (any standard gRPC health client works):

```go
c := hv1.NewHealthClient(conn)
resp, _ := c.Check(ctx, &hv1.HealthCheckRequest{Service: "db.Service"})
// resp.Status: SERVING / NOT_SERVING / UNKNOWN

stream, _ := c.Watch(ctx, &hv1.HealthCheckRequest{Service: ""})
for {
	msg, err := stream.Recv()
	if err != nil { break }
	// react to msg.Status
}
```

Status mapping:

| xprobe                     | grpc.health.v1     |
|----------------------------|--------------------|
| `StatusUp`                 | `SERVING`          |
| `StatusDown`               | `NOT_SERVING`      |
| `StatusTimeout`            | `NOT_SERVING`      |
| `StatusUnknown` (post-Set) | `UNKNOWN`          |
| never Set                  | `SERVICE_UNKNOWN`  |

`Check` on a service that has never been registered returns `codes.NotFound`.
`Watch` on an unknown service emits `SERVICE_UNKNOWN` then auto-registers,
so a subsequent `Set` is streamed on the same stream — matches
`google.golang.org/grpc/health` reference behavior.

⚠ Each `Watch` RPC holds a goroutine + buffered channel until the client
cancels. Rate-limit public-facing endpoints.

---

## Layout

```
xprobe/
├── xprobe.go                       facade — aliases + quick-start
├── go.mod                          stdlib only
└── pkg/
    ├── probe/                      Status, Probe, Func, Bool, Composite, Asyncer
    ├── state/                      State (cache + pub/sub), Registry
    ├── runner/                     Background poller
    ├── reporter/                   Reporter interface + slog adapter
    └── transport/
        ├── http/                   pull (Handler) + cached (CachedHandler)
        └── grpc/                   grpc.health.v1 server — SEPARATE go.mod
```

Why a separate `go.mod` for gRPC: keeps the core dependency-free for users who only need HTTP probes.

---

## Architecture

```
                 ┌── reporter.Slog / metrics / alerts
                 │
Probe ──poll──▶ Runner ──Set──▶ State ──Subscribe──▶ gRPC Watch stream
                                  ▲
                                  └── HTTP CachedHandler (instant read)

Probe ────────────────────────────▶ HTTP Handler (pull, on-request)
```

Pick pull-mode HTTP when your check is cheap. Switch to cached HTTP / gRPC when checks are expensive, traffic is bursty, or you need streaming updates.

---

## License

See [LICENSE](LICENSE).
