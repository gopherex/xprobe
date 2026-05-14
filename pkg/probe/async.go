package probe

import "sync"

// Asyncer schedules concurrent work and waits for completion.
// A fresh Asyncer is obtained per check via AsyncerFactory.
type Asyncer interface {
	Go(fn func())
	Wait()
}

// AsyncerFactory builds a fresh Asyncer per use.
type AsyncerFactory func() Asyncer

// SyncAsyncer runs each func inline. Wait is a no-op.
type SyncAsyncer struct{}

func (SyncAsyncer) Go(f func()) { f() }
func (SyncAsyncer) Wait()       {}

// SyncFactory returns an AsyncerFactory yielding SyncAsyncer.
func SyncFactory() AsyncerFactory { return func() Asyncer { return SyncAsyncer{} } }

// PoolAsyncer fans out to goroutines bounded by an optional semaphore.
// maxConcurrent <= 0 means unbounded.
type PoolAsyncer struct {
	wg  sync.WaitGroup
	sem chan struct{}
}

func NewPoolAsyncer(maxConcurrent int) *PoolAsyncer {
	var sem chan struct{}
	if maxConcurrent > 0 {
		sem = make(chan struct{}, maxConcurrent)
	}
	return &PoolAsyncer{sem: sem}
}

func (p *PoolAsyncer) Go(f func()) {
	p.wg.Go(func() {
		if p.sem != nil {
			p.sem <- struct{}{}
			defer func() { <-p.sem }()
		}
		f()
	})
}

func (p *PoolAsyncer) Wait() { p.wg.Wait() }

// PoolFactory returns an AsyncerFactory yielding fresh PoolAsyncer instances.
func PoolFactory(maxConcurrent int) AsyncerFactory {
	return func() Asyncer { return NewPoolAsyncer(maxConcurrent) }
}
