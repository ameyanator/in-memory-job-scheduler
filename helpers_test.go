package scheduler

import (
	"sync"
	"time"
)

// epoch is an arbitrary fixed start time, so tests never depend on the real date.
var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// fakeClock is a Clock that moves only when a test advances it.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: epoch} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func noop() {}
