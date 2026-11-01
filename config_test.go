package scheduler

import (
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	valid := Config{Total: 4, RerunMax: 2, K: time.Minute, U: time.Second}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(*Config) {}, false},
		{"rerun max equal to total", func(c *Config) { c.RerunMax = 4 }, false},
		{"zero total", func(c *Config) { c.Total = 0 }, true},
		{"negative total", func(c *Config) { c.Total = -1 }, true},
		{"zero rerun max", func(c *Config) { c.RerunMax = 0 }, true},
		{"rerun max above total", func(c *Config) { c.RerunMax = 5 }, true},
		{"zero K", func(c *Config) { c.K = 0 }, true},
		{"negative K", func(c *Config) { c.K = -time.Second }, true},
		{"zero U", func(c *Config) { c.U = 0 }, true},
		{"negative U", func(c *Config) { c.U = -time.Second }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := valid
			tt.mutate(&c)
			if err := c.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJobValidate(t *testing.T) {
	tests := []struct {
		name    string
		job     Job
		wantErr bool
	}{
		{"live", Job{Role: Live, Type: Quick, Run: noop}, false},
		{"normal rerun", Job{Role: Rerun, Type: Full, Run: noop}, false},
		{"urgent rerun", Job{Role: Rerun, Type: Quick, Urgent: true, Run: noop}, false},
		{"zero role", Job{Type: Quick, Run: noop}, true},
		{"unknown role", Job{Role: 99, Type: Quick, Run: noop}, true},
		{"zero type", Job{Role: Live, Run: noop}, true},
		{"unknown type", Job{Role: Live, Type: 99, Run: noop}, true},
		{"urgent live", Job{Role: Live, Type: Quick, Urgent: true, Run: noop}, true},
		{"nil run", Job{Role: Live, Type: Quick}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.job.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestKey(t *testing.T) {
	const k, u = time.Minute, 10 * time.Second
	c := Config{K: k, U: u}

	tests := []struct {
		name string
		job  Job
		want time.Duration
	}{
		{"live quick", Job{Role: Live, Type: Quick}, 0},
		{"live full", Job{Role: Live, Type: Full}, k},
		{"urgent quick rerun", Job{Role: Rerun, Type: Quick, Urgent: true}, 0},
		{"normal quick rerun", Job{Role: Rerun, Type: Quick}, u},
		{"urgent full rerun", Job{Role: Rerun, Type: Full, Urgent: true}, k},
		{"normal full rerun", Job{Role: Rerun, Type: Full}, k + u},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.key(tt.job, epoch).Sub(epoch)
			if got != tt.want {
				t.Fatalf("key offset = %v, want %v", got, tt.want)
			}
		})
	}
}
