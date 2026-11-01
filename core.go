package scheduler

// core holds the capacity and dispatch rules. It has no locks or goroutines;
// callers must serialize every method.
type core struct {
	cfg          Config
	clock        Clock
	live         queue
	rerun        queue
	liveRunning  int
	rerunRunning int
}

func newCore(cfg Config, clock Clock) *core {
	return &core{cfg: cfg, clock: clock}
}

// submit adds a job to the waiting queue for its role.
func (c *core) submit(job Job) {
	key := c.cfg.key(job, c.clock.Now())
	if job.Role == Rerun {
		c.rerun.push(job, key)
	} else {
		c.live.push(job, key)
	}
}

// next claims a slot for the next eligible job. It reports false if no job can
// start, in which case no slot is taken.
func (c *core) next() (Job, bool) {
	if c.liveRunning+c.rerunRunning >= c.cfg.Total {
		return Job{}, false
	}
	if c.rerun.len() > 0 && c.rerunRunning < c.cfg.RerunMax {
		c.rerunRunning++
		return c.rerun.pop(), true
	}
	if c.live.len() > 0 {
		c.liveRunning++
		return c.live.pop(), true
	}
	return Job{}, false
}

// finish releases the slot held by a running job of the given role.
func (c *core) finish(role Role) {
	if role == Rerun {
		c.rerunRunning--
	} else {
		c.liveRunning--
	}
}

// idle reports whether no job is waiting or running.
func (c *core) idle() bool {
	return c.live.len() == 0 && c.rerun.len() == 0 &&
		c.liveRunning == 0 && c.rerunRunning == 0
}
