// Package state holds the cached health status of a probe along with
// pub/sub semantics so transports (gRPC Watch, log/metric reporters, cached
// HTTP) can react to changes without re-running the underlying probe.
package state

import (
	"context"
	"sync"

	"github.com/gopherex/xprobe/pkg/probe"
)

// State is a thread-safe cached probe.Status with subscriber broadcast.
//
// Subscribers receive the current status immediately on Subscribe, then any
// subsequent changes. The latest value wins — if a subscriber is slow, older
// updates are dropped while the channel is full.
//
// Notifications are serialized with respect to Set: subscribers will never
// observe an ordering inconsistent with the status mutations themselves.
type State struct {
	mu     sync.RWMutex
	result probe.Result
	set    bool
	subs   map[chan probe.Status]struct{}
}

// New creates a State initialized to StatusUnknown. HasBeenSet returns false
// until the first Set call.
func New() *State {
	return &State{subs: make(map[chan probe.Status]struct{})}
}

// Get returns the current cached status.
func (s *State) Get() probe.Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.result.Status
}

// HasBeenSet reports whether Set has ever been called on this State.
// Used by transports (notably gRPC) to distinguish "service exists but its
// status was never reported" from "service status is genuinely up/down".
func (s *State) HasBeenSet() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.set
}

// Set replaces the cached status and notifies subscribers if the value
// changed. Returns the previous status and whether a change occurred.
//
// The lock is held across subscriber notifications. drainSend is
// non-blocking, so this does not stall callers, and it guarantees that
// concurrent Set calls produce a notification order consistent with the
// final status.
func (s *State) Set(st probe.Status) (prev probe.Status, changed bool) {
	old, changed := s.SetResult(probe.Result{Status: st})
	return old.Status, changed
}

// Result returns the status and reason from the same cached observation.
func (s *State) Result() probe.Result {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.result
}

// SetResult atomically replaces an observation. A reason-only change is a
// change too. Status subscribers are notified only when the status changes.
func (s *State) SetResult(r probe.Result) (prev probe.Result, changed bool) {
	r = r.Normalized()
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, wasSet := s.result, s.set
	s.set = true
	if wasSet && prev == r {
		return prev, false
	}
	s.result = r
	if !wasSet || prev.Status != r.Status {
		for ch := range s.subs {
			drainSend(ch, r.Status)
		}
	}
	return prev, true
}

// Subscribe returns a channel receiving status updates. The current value is
// delivered immediately. The channel is closed when ctx is canceled.
func (s *State) Subscribe(ctx context.Context) <-chan probe.Status {
	ch := make(chan probe.Status, 1)
	s.mu.Lock()
	ch <- s.result.Status
	s.subs[ch] = struct{}{}
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		delete(s.subs, ch)
		s.mu.Unlock()
		close(ch)
	}()
	return ch
}

// drainSend keeps the latest value in a size-1 channel without blocking.
// Must be called with no contention on ch from other writers — which is
// guaranteed by holding State.mu in Set.
func drainSend(ch chan probe.Status, st probe.Status) {
	select {
	case ch <- st:
	default:
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- st:
		default:
		}
	}
}
