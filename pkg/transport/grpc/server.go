// Package grpcprobe implements the grpc.health.v1 service backed by an
// xprobe state.Registry. Unlike the HTTP transport, the gRPC health protocol
// requires the server to know the status synchronously and to stream updates,
// which is why this transport reads from cached State rather than running
// probes per request.
//
// # Watch & resource usage
//
// Each Watch RPC subscribes to a state.State, which spawns one goroutine and
// one buffered channel until the client cancels the stream. Public-facing
// gRPC endpoints should rate-limit Watch (or place this server behind one)
// to prevent connection-exhaustion DoS.
package grpcprobe

import (
	"context"

	"google.golang.org/grpc/codes"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/gopherex/xprobe/pkg/probe"
	xstate "github.com/gopherex/xprobe/pkg/state"
)

// Server implements grpc_health_v1.HealthServer backed by a state.Registry.
// The empty service name "" is conventionally the overall server status.
type Server struct {
	hv1.UnimplementedHealthServer
	reg *xstate.Registry
}

// New constructs a Server reading from the given Registry.
func New(reg *xstate.Registry) *Server { return &Server{reg: reg} }

// Check returns the current status of the requested service.
// Returns codes.NotFound if no State has been registered for the name —
// matches reference google.golang.org/grpc/health behavior.
func (s *Server) Check(_ context.Context, req *hv1.HealthCheckRequest) (*hv1.HealthCheckResponse, error) {
	st, ok := s.reg.Lookup(req.GetService())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown service %q", req.GetService())
	}
	return &hv1.HealthCheckResponse{Status: mapStatus(st.Get(), st.HasBeenSet())}, nil
}

// Watch streams status updates for the requested service.
//
// Per grpc.health.v1, the current status is sent first, then any subsequent
// changes. The stream stays open until the client cancels or the server
// terminates.
//
// Unknown services receive SERVICE_UNKNOWN as the initial message and the
// stream stays open: if the service is later registered and Set is called,
// the new status is streamed. This matches reference behavior.
func (s *Server) Watch(req *hv1.HealthCheckRequest, stream hv1.Health_WatchServer) error {
	name := req.GetService()
	st, registered := s.reg.Lookup(name)
	if !registered {
		// Send the initial SERVICE_UNKNOWN, then begin tracking the
		// service so a later Set is observable on this stream.
		if err := stream.Send(&hv1.HealthCheckResponse{
			Status: hv1.HealthCheckResponse_SERVICE_UNKNOWN,
		}); err != nil {
			return err
		}
		st = s.reg.Get(name)
	}

	ctx := stream.Context()
	ch := st.Subscribe(ctx)

	// If we just auto-registered the service, Subscribe will deliver an
	// initial StatusUnknown that semantically duplicates the SERVICE_UNKNOWN
	// we already sent. Drop it.
	first := true
	for cur := range ch {
		if first && !registered {
			first = false
			if !st.HasBeenSet() {
				continue
			}
		}
		first = false

		if err := stream.Send(&hv1.HealthCheckResponse{
			Status: mapStatus(cur, st.HasBeenSet()),
		}); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// mapStatus translates xprobe.Status into a gRPC ServingStatus.
// hasBeenSet distinguishes "service exists but its status was never reported"
// (SERVICE_UNKNOWN) from "status is genuinely unknown" (UNKNOWN).
func mapStatus(s probe.Status, hasBeenSet bool) hv1.HealthCheckResponse_ServingStatus {
	if !hasBeenSet {
		return hv1.HealthCheckResponse_SERVICE_UNKNOWN
	}
	switch s {
	case probe.StatusUp:
		return hv1.HealthCheckResponse_SERVING
	case probe.StatusDown, probe.StatusTimeout:
		return hv1.HealthCheckResponse_NOT_SERVING
	default:
		return hv1.HealthCheckResponse_UNKNOWN
	}
}
