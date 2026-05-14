package grpcprobe

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/gopherex/xprobe/pkg/probe"
	xstate "github.com/gopherex/xprobe/pkg/state"
)

func dial(t *testing.T, reg *xstate.Registry) hv1.HealthClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	hv1.RegisterHealthServer(srv, New(reg))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return hv1.NewHealthClient(conn)
}

func TestCheckServing(t *testing.T) {
	t.Parallel()
	reg := xstate.NewRegistry()
	reg.Get("svc").Set(probe.StatusUp)

	c := dial(t, reg)
	resp, err := c.Check(context.Background(), &hv1.HealthCheckRequest{Service: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != hv1.HealthCheckResponse_SERVING {
		t.Fatalf("got %v, want SERVING", resp.Status)
	}
}

func TestCheckUnknownService(t *testing.T) {
	t.Parallel()
	reg := xstate.NewRegistry()
	c := dial(t, reg)
	_, err := c.Check(context.Background(), &hv1.HealthCheckRequest{Service: "missing"})
	if err == nil {
		t.Fatal("expected NotFound error")
	}
}

func TestWatchUnknownService(t *testing.T) {
	t.Parallel()
	reg := xstate.NewRegistry()
	c := dial(t, reg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream, err := c.Watch(ctx, &hv1.HealthCheckRequest{Service: "later"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != hv1.HealthCheckResponse_SERVICE_UNKNOWN {
		t.Fatalf("initial = %v, want SERVICE_UNKNOWN", first.Status)
	}

	// Auto-registered: a subsequent Set should reach this stream.
	reg.Get("later").Set(probe.StatusUp)
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != hv1.HealthCheckResponse_SERVING {
		t.Fatalf("after Set(up) = %v, want SERVING", resp.Status)
	}
}

func TestWatchStream(t *testing.T) {
	t.Parallel()
	reg := xstate.NewRegistry()
	st := reg.Get("svc")

	c := dial(t, reg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream, err := c.Watch(ctx, &hv1.HealthCheckRequest{Service: "svc"})
	if err != nil {
		t.Fatal(err)
	}

	first, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != hv1.HealthCheckResponse_SERVICE_UNKNOWN {
		t.Fatalf("initial = %v, want SERVICE_UNKNOWN", first.Status)
	}

	st.Set(probe.StatusUp)
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != hv1.HealthCheckResponse_SERVING {
		t.Fatalf("after up = %v, want SERVING", resp.Status)
	}

	st.Set(probe.StatusDown)
	resp, err = stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != hv1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("after down = %v, want NOT_SERVING", resp.Status)
	}
}
