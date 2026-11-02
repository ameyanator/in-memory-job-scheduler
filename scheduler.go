package scheduler

import (
	"errors"
	"log"
	"sync"
)

// ErrClosed is returned by Submit after Close has been called.
var ErrClosed = errors.New("scheduler: closed")

// Scheduler runs submitted jobs on a pool of Config.Total workers. It is safe
// for concurrent use.
type Scheduler struct {
	mu      sync.Mutex
	wake    *sync.Cond // Signals idle workers; uses mu.
	core    *core
	closed  bool
	workers sync.WaitGroup
}

// New validates cfg and starts the workers. Call Close to stop them.
func New(cfg Config, clock Clock) (*Scheduler, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		return nil, errors.New("scheduler: clock is nil")
	}
	s := &Scheduler{core: newCore(cfg, clock)}
	s.wake = sync.NewCond(&s.mu)
	s.workers.Add(cfg.Total)
	for range cfg.Total {
		go s.work()
	}
	return s, nil
}

// Submit queues a job. It returns an error if the job is invalid or the
// scheduler is closed.
func (s *Scheduler) Submit(job Job) error {
	if err := job.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.core.submit(job)
	// Workers are interchangeable, and a submit adds one job, so one is enough.
	s.wake.Signal()
	return nil
}

// Close rejects new submits, runs every accepted job, and returns once all
// workers have exited. It must not be called from inside a job.
func (s *Scheduler) Close() {
	s.mu.Lock()
	s.closed = true
	s.wake.Broadcast()
	s.mu.Unlock()
	s.workers.Wait()
}

func (s *Scheduler) work() {
	defer s.workers.Done()
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if job, ok := s.core.next(); ok {
			s.mu.Unlock()
			run(job) // worker runs the job and mutex is free to do other operations as necessary
			s.mu.Lock()
			s.core.finish(job.Role)
			// One finish enables at most one start, which this worker takes.
			continue
		}
		if s.closed && s.core.idle() {
			// Workers asleep behind a rerun at its limit won't otherwise wake.
			s.wake.Broadcast()
			return
		}
		s.wake.Wait()
	}
}

// run executes a job, containing a panic so the worker can release its slot.
func run(job Job) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("scheduler: job %q panicked: %v", job.ID, r)
		}
	}()
	job.Run()
}
