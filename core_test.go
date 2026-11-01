package scheduler

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"
)

func newTestCore(total, rerunMax int) (*core, *fakeClock) {
	clock := newFakeClock()
	cfg := Config{Total: total, RerunMax: rerunMax, K: testK, U: testU}
	return newCore(cfg, clock), clock
}

func submitN(c *core, role Role, n int) {
	for i := range n {
		c.submit(Job{ID: fmt.Sprintf("%d-%d", role, i), Role: role, Type: Quick, Run: noop})
	}
}

// dispatch starts jobs until none can start and returns how many of each started.
func dispatch(c *core) (live, rerun int) {
	for {
		job, ok := c.next()
		if !ok {
			return live, rerun
		}
		if job.Role == Rerun {
			rerun++
		} else {
			live++
		}
	}
}

func wantRunning(t *testing.T, c *core, live, rerun int) {
	t.Helper()
	if c.liveRunning != live || c.rerunRunning != rerun {
		t.Fatalf("running live=%d rerun=%d, want live=%d rerun=%d",
			c.liveRunning, c.rerunRunning, live, rerun)
	}
}

func TestS1LiveUsesAllSlots(t *testing.T) {
	c, _ := newTestCore(4, 1)
	submitN(c, Live, 6)
	dispatch(c)
	wantRunning(t, c, 4, 0)
	if c.live.len() != 2 {
		t.Fatalf("live waiting = %d, want 2", c.live.len())
	}
}

func TestS2NextFreedSlotGoesToRerun(t *testing.T) {
	c, _ := newTestCore(4, 1)
	submitN(c, Live, 6)
	dispatch(c)
	submitN(c, Rerun, 1)

	if _, ok := c.next(); ok {
		t.Fatal("started a job with no free slot")
	}
	c.finish(Live)
	job, ok := c.next()
	if !ok || job.Role != Rerun {
		t.Fatalf("next = %+v, %v, want the rerun", job, ok)
	}
	wantRunning(t, c, 3, 1)
}

func TestS3LiveLimitedWhileRerunRuns(t *testing.T) {
	c, _ := newTestCore(4, 2)
	submitN(c, Rerun, 1)
	submitN(c, Live, 6)
	dispatch(c)
	wantRunning(t, c, 3, 1)
}

func TestLiveRegainsCapacityWhenRerunWorkEnds(t *testing.T) {
	c, _ := newTestCore(4, 2)
	submitN(c, Rerun, 1)
	submitN(c, Live, 6)
	dispatch(c)

	c.finish(Rerun)
	dispatch(c)
	wantRunning(t, c, 4, 0)
}

func TestLiveTakesSlotWhenRerunsAreAtTheirLimit(t *testing.T) {
	c, _ := newTestCore(4, 2)
	submitN(c, Rerun, 5)
	submitN(c, Live, 5)
	dispatch(c)
	wantRunning(t, c, 2, 2)

	c.finish(Live)
	job, ok := c.next()
	if !ok || job.Role != Live {
		t.Fatalf("next = %+v, %v, want a live job", job, ok)
	}
}

func TestRunningLiveJobsAreNotPreempted(t *testing.T) {
	c, _ := newTestCore(4, 2)
	submitN(c, Live, 4)
	dispatch(c)
	submitN(c, Rerun, 5)

	// Live stays above its target of 2 until jobs finish.
	if _, ok := c.next(); ok {
		t.Fatal("started a job with no free slot")
	}
	wantRunning(t, c, 4, 0)
	c.finish(Live)
	dispatch(c)
	wantRunning(t, c, 3, 1)
	c.finish(Live)
	dispatch(c)
	wantRunning(t, c, 2, 2)
}

func TestRerunReplacesFinishedRerun(t *testing.T) {
	c, _ := newTestCore(4, 1)
	submitN(c, Rerun, 2)
	submitN(c, Live, 4)
	dispatch(c)
	wantRunning(t, c, 3, 1)

	c.finish(Rerun)
	job, ok := c.next()
	if !ok || job.Role != Rerun {
		t.Fatalf("next = %+v, %v, want the waiting rerun", job, ok)
	}
}

func TestRerunMaxEqualToTotal(t *testing.T) {
	c, _ := newTestCore(2, 2)
	submitN(c, Live, 1)
	submitN(c, Rerun, 3)
	dispatch(c)
	wantRunning(t, c, 0, 2)
}

func TestNextOnEmptyCoreTakesNoSlot(t *testing.T) {
	c, _ := newTestCore(4, 2)
	if _, ok := c.next(); ok {
		t.Fatal("next returned a job from an empty core")
	}
	wantRunning(t, c, 0, 0)
}

func TestIdle(t *testing.T) {
	c, _ := newTestCore(4, 2)
	if !c.idle() {
		t.Fatal("new core is not idle")
	}
	submitN(c, Rerun, 1)
	if c.idle() {
		t.Fatal("idle with a job waiting")
	}
	dispatch(c)
	if c.idle() {
		t.Fatal("idle with a job running")
	}
	c.finish(Rerun)
	if !c.idle() {
		t.Fatal("not idle after the last job finished")
	}
}

func TestSubmitUsesClockForAging(t *testing.T) {
	c, clock := newTestCore(1, 1)
	c.submit(Job{ID: "full", Role: Live, Type: Full, Run: noop})
	clock.Advance(testK - time.Second)
	c.submit(Job{ID: "before K", Role: Live, Type: Quick, Run: noop})
	clock.Advance(2 * time.Second)
	c.submit(Job{ID: "after K", Role: Live, Type: Quick, Run: noop})

	var got []string
	for range 3 {
		job, ok := c.next()
		if !ok {
			t.Fatal("no job to start")
		}
		got = append(got, job.ID)
		c.finish(job.Role)
	}
	want := []string{"before K", "full", "after K"}
	if !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestUrgentRerunRunsBeforeNormalRerunButNotBeforeRunningLive(t *testing.T) {
	c, _ := newTestCore(1, 1)
	submitN(c, Live, 1)
	dispatch(c)
	c.submit(Job{ID: "normal", Role: Rerun, Type: Quick, Run: noop})
	c.submit(Job{ID: "urgent", Role: Rerun, Type: Quick, Urgent: true, Run: noop})

	if _, ok := c.next(); ok {
		t.Fatal("urgent rerun started with no free slot")
	}
	c.finish(Live)
	if job, _ := c.next(); job.ID != "urgent" {
		t.Fatalf("next = %q, want urgent", job.ID)
	}
}

// checkInvariants asserts the capacity rules against a state where dispatch
// has run to completion.
func checkInvariants(t *testing.T, c *core) {
	t.Helper()
	running := c.liveRunning + c.rerunRunning
	if running > c.cfg.Total {
		t.Fatalf("C1: %d running, total %d", running, c.cfg.Total)
	}
	if c.rerunRunning > c.cfg.RerunMax {
		t.Fatalf("C2: %d reruns running, max %d", c.rerunRunning, c.cfg.RerunMax)
	}
	if running == c.cfg.Total {
		return
	}
	if c.live.len() > 0 {
		t.Fatalf("C6: free slot with %d live waiting", c.live.len())
	}
	if c.rerun.len() > 0 && c.rerunRunning < c.cfg.RerunMax {
		t.Fatalf("C6: free slot with %d eligible reruns waiting", c.rerun.len())
	}
}

// maxLive is the capacity formula from DESIGN.md.
func maxLive(c *core) int {
	return c.cfg.Total - min(c.cfg.RerunMax, c.rerunRunning+c.rerun.len())
}

func TestRandomOperationsKeepInvariants(t *testing.T) {
	for seed := range int64(200) {
		rng := rand.New(rand.NewSource(seed))
		total := 1 + rng.Intn(8)
		c, clock := newTestCore(total, 1+rng.Intn(total))

		var running []Role
		for range 2000 {
			switch rng.Intn(3) {
			case 0:
				c.submit(Job{Role: Live, Type: Quick, Run: noop})
			case 1:
				c.submit(Job{Role: Rerun, Type: Full, Run: noop})
			case 2:
				if len(running) > 0 {
					i := rng.Intn(len(running))
					c.finish(running[i])
					running = slices.Delete(running, i, i+1)
				}
			}
			clock.Advance(time.Millisecond)

			for {
				job, ok := c.next()
				if !ok {
					break
				}
				running = append(running, job.Role)
				if job.Role == Live && c.liveRunning > maxLive(c) {
					t.Fatalf("seed %d: live started above its target: %d > %d",
						seed, c.liveRunning, maxLive(c))
				}
			}
			checkInvariants(t, c)
		}
	}
}
