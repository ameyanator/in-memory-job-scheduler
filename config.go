// Package scheduler is an in-memory job scheduler: a fixed pool of workers runs
// jobs under role-based capacity limits and type-based ordering.
package scheduler

import (
	"errors"
	"fmt"
	"time"
)

// Role decides how much capacity a job may use. The zero value is invalid.
type Role int

const (
	Live Role = iota + 1
	Rerun
)

// Type decides which eligible job runs next. The zero value is invalid.
type Type int

const (
	Quick Type = iota + 1
	Full
)

// Job is a unit of work that holds one slot while Run executes.
type Job struct {
	ID     string
	Role   Role
	Type   Type
	Urgent bool // Valid only for reruns.
	Run    func()
}

// Validate reports whether the job can be accepted.
func (j Job) Validate() error {
	if j.Role != Live && j.Role != Rerun {
		return fmt.Errorf("job %q: invalid role %d", j.ID, j.Role)
	}
	if j.Type != Quick && j.Type != Full {
		return fmt.Errorf("job %q: invalid type %d", j.ID, j.Type)
	}
	if j.Urgent && j.Role != Rerun {
		return fmt.Errorf("job %q: only reruns can be urgent", j.ID)
	}
	if j.Run == nil {
		return fmt.Errorf("job %q: Run is nil", j.ID)
	}
	return nil
}

// Config holds the scheduler's limits and ordering delays.
type Config struct {
	Total    int           // Maximum jobs running at once, across both roles.
	RerunMax int           // Maximum reruns running at once.
	K        time.Duration // Wait after which new quick jobs can't pass a full job.
	U        time.Duration // Head start an urgent rerun gets over a normal rerun.
}

// Validate reports whether the config is usable.
func (c Config) Validate() error {
	switch {
	case c.Total < 1:
		return errors.New("config: Total must be at least 1")
	case c.RerunMax < 1:
		// An accepted rerun could never run, so a draining Close would hang.
		return errors.New("config: RerunMax must be at least 1")
	case c.RerunMax > c.Total:
		return errors.New("config: RerunMax must not exceed Total")
	case c.K <= 0:
		return errors.New("config: K must be positive")
	case c.U <= 0:
		return errors.New("config: U must be positive")
	}
	return nil
}

// key returns a job's sort key, fixed at submission. The lowest key runs first.
// Normal full reruns can be passed by new quick jobs for K + U, not K; see
// DESIGN.md.
func (c Config) key(j Job, submitted time.Time) time.Time {
	key := submitted
	if j.Type == Full {
		key = key.Add(c.K)
	}
	if j.Role == Rerun && !j.Urgent {
		key = key.Add(c.U)
	}
	return key
}
