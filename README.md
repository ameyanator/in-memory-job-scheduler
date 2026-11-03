# In-memory job scheduler

A small in-memory job scheduler in Go. A fixed pool of workers runs jobs, each holding one slot while it runs.

- **Role** (`Live` or `Rerun`) decides how much capacity a job may use.
- **Type** (`Quick` or `Full`) decides which eligible job runs next.
- **Urgent** reruns start sooner than normal reruns, while normal reruns still make progress.

See [DESIGN.md](DESIGN.md) for the capacity rule, ordering, concurrency and trade-offs.

## Requirements

Go 1.26 or later. There are no third-party dependencies.

## Running the tests

```
go test -race ./...
```

The tests are deterministic: ordering tests use a fake clock, and concurrency tests synchronise on channels rather than sleeps.

Useful variations:

```
go test -race -v -count=1 ./...   # list each test and skip the result cache
go test -race -count=20 ./...     # repeat to look for flaky concurrency failures
go test -run xxx -bench . ./...   # queue push and pop benchmark
```

With `-v`, the scheduler tests also print each job as it starts.

## Usage

This is a library package with no `main`.

```go
package main

import (
	"fmt"
	"time"

	scheduler "github.com/ameyanator/in-memory-job-scheduler"
)

func main() {
	s, err := scheduler.New(scheduler.Config{
		Total:    4,                // Jobs running at once, across both roles.
		RerunMax: 2,                // Reruns running at once.
		K:        30 * time.Second, // After this wait, new quick jobs can't pass a full job.
		U:        5 * time.Second,  // Head start for urgent reruns over normal reruns.
	}, scheduler.SystemClock{})
	if err != nil {
		panic(err)
	}

	err = s.Submit(scheduler.Job{
		ID:     "rerun-42",
		Role:   scheduler.Rerun,
		Type:   scheduler.Full,
		Urgent: true,
		Run:    func() { fmt.Println("running rerun-42") },
	})
	if err != nil {
		panic(err)
	}

	s.Close() // Rejects new jobs, runs everything queued, then returns.
}
```

## Layout

| File | Contents |
|---|---|
| `config.go` | `Config`, `Job`, validation and the ordering key |
| `clock.go` | The injectable `Clock` |
| `queue.go` | Min-heap of waiting jobs, one per role |
| `core.go` | Capacity rule and dispatch, with no locking |
| `scheduler.go` | Worker pool, locking, wakeups and shutdown |
