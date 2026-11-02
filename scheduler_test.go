package scheduler

import (
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// timeout only fails a hung test. No test waits for it to pass.
const timeout = 10 * time.Second

func newTestScheduler(t *testing.T, total, rerunMax int) (*Scheduler, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	s, err := New(Config{Total: total, RerunMax: rerunMax, K: testK, U: testU}, clock)
	if err != nil {
		t.Fatal(err)
	}
	return s, clock
}

// gate makes jobs report when they start and block until released.
type gate struct {
	started chan string
	release chan struct{}
}

func newGate() *gate {
	return &gate{started: make(chan string, 1024), release: make(chan struct{})}
}

func (g *gate) job(id string, role Role, typ Type) Job {
	return Job{ID: id, Role: role, Type: typ, Run: func() {
		g.started <- id
		<-g.release
	}}
}

func (g *gate) next(t *testing.T) string {
	t.Helper()
	select {
	case id := <-g.started:
		return id
	case <-time.After(timeout):
		t.Fatal("timed out waiting for a job to start")
		return ""
	}
}

func (g *gate) releaseOne(t *testing.T) {
	t.Helper()
	select {
	case g.release <- struct{}{}:
	case <-time.After(timeout):
		t.Fatal("timed out releasing a job")
	}
}

func mustSubmit(t *testing.T, s *Scheduler, job Job) {
	t.Helper()
	if err := s.Submit(job); err != nil {
		t.Fatal(err)
	}
}

func mustClose(t *testing.T, s *Scheduler) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		s.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("Close did not return")
	}
}

// counts reads the core's state under the scheduler's lock.
func counts(s *Scheduler) (running, waiting int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.core
	return c.liveRunning + c.rerunRunning, c.live.len() + c.rerun.len()
}

func TestNewRejectsBadInput(t *testing.T) {
	if _, err := New(Config{}, SystemClock{}); err == nil {
		t.Fatal("accepted an invalid config")
	}
	if _, err := New(Config{Total: 1, RerunMax: 1, K: 1, U: 1}, nil); err == nil {
		t.Fatal("accepted a nil clock")
	}
}

func TestSubmitRejectsInvalidJob(t *testing.T) {
	s, _ := newTestScheduler(t, 1, 1)
	defer mustClose(t, s)
	if err := s.Submit(Job{Role: Live, Type: Quick}); err == nil {
		t.Fatal("accepted a job with no Run")
	}
}

func TestSchedulerS1(t *testing.T) {
	s, _ := newTestScheduler(t, 4, 1)
	g := newGate()
	for i := range 6 {
		mustSubmit(t, s, g.job(fmt.Sprint(i), Live, Quick))
	}
	for range 4 {
		g.next(t)
	}
	if running, waiting := counts(s); running != 4 || waiting != 2 {
		t.Fatalf("running=%d waiting=%d, want 4 and 2", running, waiting)
	}

	for range 6 {
		g.releaseOne(t)
	}
	mustClose(t, s)
}

func TestSchedulerS2(t *testing.T) {
	s, _ := newTestScheduler(t, 4, 1)
	g := newGate()
	for i := range 4 {
		mustSubmit(t, s, g.job(fmt.Sprintf("live-%d", i), Live, Quick))
	}
	for range 4 {
		g.next(t)
	}
	mustSubmit(t, s, g.job("waiting-live", Live, Quick))
	mustSubmit(t, s, g.job("rerun", Rerun, Quick))

	g.releaseOne(t)
	if got := g.next(t); got != "rerun" {
		t.Fatalf("freed slot went to %q, want rerun", got)
	}

	for range 5 {
		g.releaseOne(t)
	}
	mustClose(t, s)
}

// An idle worker must wake for every submit, one job at a time.
func TestIdleWorkerWakesOnSubmit(t *testing.T) {
	s, _ := newTestScheduler(t, 2, 1)
	defer mustClose(t, s)
	for i := range 2000 {
		done := make(chan struct{})
		mustSubmit(t, s, Job{Role: Live, Type: Quick, Run: func() { close(done) }})
		select {
		case <-done:
		case <-time.After(timeout):
			t.Fatalf("job %d never ran", i)
		}
	}
}

// Workers idle behind a rerun at its limit must still start a live job.
func TestWorkerStartsLiveWhileRerunsAreBlocked(t *testing.T) {
	s, _ := newTestScheduler(t, 2, 1)
	g := newGate()
	mustSubmit(t, s, g.job("rerun-1", Rerun, Quick))
	g.next(t)
	mustSubmit(t, s, g.job("rerun-2", Rerun, Quick))
	mustSubmit(t, s, g.job("live", Live, Quick))

	if got := g.next(t); got != "live" {
		t.Fatalf("started %q, want live", got)
	}
	for range 3 {
		g.releaseOne(t)
	}
	mustClose(t, s)
}

func TestSchedulerRunsJobsInKeyOrder(t *testing.T) {
	s, clock := newTestScheduler(t, 1, 1)
	g := newGate()
	mustSubmit(t, s, g.job("first", Live, Quick))
	g.next(t)

	mustSubmit(t, s, g.job("full", Live, Full))
	clock.Advance(time.Second)
	mustSubmit(t, s, g.job("quick", Live, Quick))
	clock.Advance(testK)
	mustSubmit(t, s, g.job("late quick", Live, Quick))

	for _, want := range []string{"quick", "full", "late quick"} {
		g.releaseOne(t)
		if got := g.next(t); got != want {
			t.Fatalf("started %q, want %q", got, want)
		}
	}
	g.releaseOne(t)
	mustClose(t, s)
}

func TestJobCanSubmitAnotherJob(t *testing.T) {
	s, _ := newTestScheduler(t, 1, 1)
	done := make(chan struct{})
	mustSubmit(t, s, Job{Role: Live, Type: Quick, Run: func() {
		if err := s.Submit(Job{Role: Live, Type: Quick, Run: func() { close(done) }}); err != nil {
			t.Error(err)
		}
	}})
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("inner job never ran")
	}
	mustClose(t, s)
}

func TestPanickingJobReleasesItsSlot(t *testing.T) {
	original := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(original)

	s, _ := newTestScheduler(t, 1, 1)
	done := make(chan struct{})
	mustSubmit(t, s, Job{Role: Live, Type: Quick, Run: func() { panic("boom") }})
	mustSubmit(t, s, Job{Role: Live, Type: Quick, Run: func() { close(done) }})
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("job after the panic never ran")
	}
	mustClose(t, s)
	if running, _ := counts(s); running != 0 {
		t.Fatalf("running = %d after close, want 0", running)
	}
}

func TestCloseDrainsQueuedJobs(t *testing.T) {
	s, _ := newTestScheduler(t, 2, 1)
	var ran atomic.Int64
	for i := range 100 {
		role := Live
		if i%2 == 0 {
			role = Rerun
		}
		mustSubmit(t, s, Job{Role: role, Type: Quick, Run: func() { ran.Add(1) }})
	}
	mustClose(t, s)
	if got := ran.Load(); got != 100 {
		t.Fatalf("ran %d jobs, want 100", got)
	}
}

// Idle workers sleep while reruns wait behind rerun_max, and must still exit.
func TestCloseReturnsWithWorkersIdleBehindRerunLimit(t *testing.T) {
	s, _ := newTestScheduler(t, 4, 1)
	g := newGate()
	for i := range 3 {
		mustSubmit(t, s, g.job(fmt.Sprint(i), Rerun, Quick))
	}
	g.next(t)

	closed := make(chan struct{})
	go func() {
		s.Close()
		close(closed)
	}()
	for range 3 {
		g.releaseOne(t)
	}
	select {
	case <-closed:
	case <-time.After(timeout):
		t.Fatal("Close did not return")
	}
}

func TestSubmitAfterClose(t *testing.T) {
	s, _ := newTestScheduler(t, 1, 1)
	mustClose(t, s)
	err := s.Submit(Job{Role: Live, Type: Quick, Run: noop})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit = %v, want ErrClosed", err)
	}
}

func TestCloseTwice(t *testing.T) {
	s, _ := newTestScheduler(t, 2, 1)
	mustClose(t, s)
	mustClose(t, s)
}

// Every job accepted while Close runs must execute, and rejected ones must not.
func TestSubmitRacingClose(t *testing.T) {
	s, _ := newTestScheduler(t, 4, 2)
	var accepted, ran atomic.Int64
	var once sync.Once
	first := make(chan struct{})

	var submitters sync.WaitGroup
	for range 4 {
		submitters.Go(func() {
			for {
				err := s.Submit(Job{Role: Live, Type: Quick, Run: func() {
					ran.Add(1)
					once.Do(func() { close(first) })
				}})
				if err != nil {
					return
				}
				accepted.Add(1)
			}
		})
	}
	<-first
	mustClose(t, s)
	submitters.Wait()
	if accepted.Load() != ran.Load() {
		t.Fatalf("accepted %d, ran %d", accepted.Load(), ran.Load())
	}
}

// Concurrent submitters must never break the caps, and each job runs once.
func TestConcurrentStress(t *testing.T) {
	const (
		total      = 8
		rerunMax   = 3
		submitters = 8
		perWorker  = 2000
	)
	s, clock := newTestScheduler(t, total, rerunMax)

	var running, reruns, peakRunning, peakReruns atomic.Int64
	raise := func(peak *atomic.Int64, v int64) {
		for {
			old := peak.Load()
			if v <= old || peak.CompareAndSwap(old, v) {
				return
			}
		}
	}
	runs := make([]atomic.Int64, submitters*perWorker)

	var wg sync.WaitGroup
	for sub := range submitters {
		wg.Go(func() {
			for i := range perWorker {
				id := sub*perWorker + i
				role, typ := Live, Quick
				if i%3 == 0 {
					role = Rerun
				}
				if i%5 == 0 {
					typ = Full
				}
				err := s.Submit(Job{Role: role, Type: typ, Urgent: role == Rerun && i%2 == 0, Run: func() {
					raise(&peakRunning, running.Add(1))
					if role == Rerun {
						raise(&peakReruns, reruns.Add(1))
					}
					runs[id].Add(1)
					runtime.Gosched()
					if role == Rerun {
						reruns.Add(-1)
					}
					running.Add(-1)
				}})
				if err != nil {
					t.Error(err) // t.Fatal must not run outside the test goroutine.
					return
				}
				clock.Advance(time.Millisecond)
			}
		})
	}
	wg.Wait()
	mustClose(t, s)

	if got := peakRunning.Load(); got > total {
		t.Fatalf("C1: peak running %d, total %d", got, total)
	}
	if got := peakReruns.Load(); got > rerunMax {
		t.Fatalf("C2: peak reruns %d, max %d", got, rerunMax)
	}
	counted := make([]int64, len(runs))
	for i := range runs {
		counted[i] = runs[i].Load()
	}
	if i := slices.IndexFunc(counted, func(n int64) bool { return n != 1 }); i >= 0 {
		t.Fatalf("job %d ran %d times, want 1", i, counted[i])
	}
}
