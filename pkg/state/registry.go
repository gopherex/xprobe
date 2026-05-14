package state

import "sync"

// Registry maps service names to States. The empty name "" is conventionally
// used for the overall service status (matches grpc.health.v1 semantics).
type Registry struct {
	mu sync.RWMutex
	m  map[string]*State
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{m: make(map[string]*State)}
}

// Get returns the State for name, creating one if absent.
func (r *Registry) Get(name string) *State {
	r.mu.RLock()
	if s, ok := r.m[name]; ok {
		r.mu.RUnlock()
		return s
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.m[name]; ok {
		return s
	}
	s := New()
	r.m[name] = s
	return s
}

// Lookup returns the State for name and whether it existed.
// Unlike Get, it does not create missing entries.
func (r *Registry) Lookup(name string) (*State, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.m[name]
	return s, ok
}

// Names returns a snapshot of registered service names.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.m))
	for n := range r.m {
		out = append(out, n)
	}
	return out
}
