package scheduler

import "time"

// Clock supplies the current time.
type Clock interface {
	Now() time.Time
}

// SystemClock is a Clock that reads the system time.
type SystemClock struct{}

// Now returns the current time.
func (SystemClock) Now() time.Time { return time.Now() }
