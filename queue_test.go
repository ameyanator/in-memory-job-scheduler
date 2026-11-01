package scheduler

import (
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"
	"time"
)

const (
	testK = time.Minute
	testU = 10 * time.Second
)

var testConfig = Config{Total: 4, RerunMax: 2, K: testK, U: testU}

// submission is a job pushed at an offset from epoch.
type submission struct {
	id     string
	at     time.Duration
	typ    Type
	role   Role
	urgent bool
}

// drain pushes the submissions in order and returns the IDs in the order popped.
func drain(subs []submission) []string {
	var q queue
	for _, s := range subs {
		job := Job{ID: s.id, Role: s.role, Type: s.typ, Urgent: s.urgent}
		q.push(job, testConfig.key(job, epoch.Add(s.at)))
	}
	got := make([]string, 0, len(subs))
	for q.len() > 0 {
		got = append(got, q.pop().ID)
	}
	return got
}

func TestQueueOrdering(t *testing.T) {
	tests := []struct {
		name string
		subs []submission
		want []string
	}{
		{
			name: "O1: quick passes a full job within K",
			subs: []submission{
				{id: "full", at: 0, typ: Full, role: Live},
				{id: "quick", at: testK - time.Second, typ: Quick, role: Live},
			},
			want: []string{"quick", "full"},
		},
		{
			name: "O2: quick submitted after K can't pass a full job",
			subs: []submission{
				{id: "full", at: 0, typ: Full, role: Live},
				{id: "quick", at: testK + time.Second, typ: Quick, role: Live},
			},
			want: []string{"full", "quick"},
		},
		{
			name: "O2 boundary: at exactly K, the earlier submission runs first",
			subs: []submission{
				{id: "full", at: 0, typ: Full, role: Live},
				{id: "quick", at: testK, typ: Quick, role: Live},
			},
			want: []string{"full", "quick"},
		},
		{
			name: "O2: an aged full job doesn't pass quick jobs that came before it",
			subs: []submission{
				{id: "quick", at: 0, typ: Quick, role: Live},
				{id: "full", at: time.Second, typ: Full, role: Live},
			},
			want: []string{"quick", "full"},
		},
		{
			name: "O3: equal jobs run in submission order",
			subs: []submission{
				{id: "a", at: 0, typ: Quick, role: Live},
				{id: "b", at: 0, typ: Quick, role: Live},
				{id: "c", at: 0, typ: Quick, role: Live},
			},
			want: []string{"a", "b", "c"},
		},
		{
			name: "Part 3: urgent passes a normal rerun within U",
			subs: []submission{
				{id: "normal", at: 0, typ: Quick, role: Rerun},
				{id: "urgent", at: testU - time.Second, typ: Quick, role: Rerun, urgent: true},
			},
			want: []string{"urgent", "normal"},
		},
		{
			name: "Part 3: urgent submitted after U can't pass a normal rerun",
			subs: []submission{
				{id: "normal", at: 0, typ: Quick, role: Rerun},
				{id: "urgent", at: testU + time.Second, typ: Quick, role: Rerun, urgent: true},
			},
			want: []string{"normal", "urgent"},
		},
		{
			name: "Part 3: a normal full rerun can be passed until K + U, then not",
			subs: []submission{
				{id: "normal full", at: 0, typ: Full, role: Rerun},
				{id: "inside", at: testK + testU - time.Second, typ: Quick, role: Rerun, urgent: true},
				{id: "outside", at: testK + testU + time.Second, typ: Quick, role: Rerun, urgent: true},
			},
			want: []string{"inside", "normal full", "outside"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := drain(tt.subs); !slices.Equal(got, tt.want) {
				t.Fatalf("order = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueueFullJobNotStarvedByQuickStream(t *testing.T) {
	subs := []submission{{id: "full", at: 0, typ: Full, role: Live}}
	for i := range 120 {
		subs = append(subs, submission{
			id:   fmt.Sprintf("quick-%d", i),
			at:   time.Duration(i) * time.Second, // 0s to 119s, with K at 60s.
			typ:  Quick,
			role: Live,
		})
	}

	// Quick jobs from 0s to 59s go first; the 60s one ties and loses on seq.
	if got := slices.Index(drain(subs), "full"); got != 60 {
		t.Fatalf("full job ran at position %d, want 60", got)
	}
}

func TestQueueMatchesSortedOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	const n = 50_000

	var q queue
	want := make([]item, n)
	for i := range n {
		key := epoch.Add(time.Duration(rng.Intn(1000)) * time.Second)
		job := Job{ID: fmt.Sprint(i)}
		q.push(job, key)
		want[i] = item{key: key, seq: uint64(i), job: job}
	}
	sort.Slice(want, func(i, j int) bool { return itemHeap(want).Less(i, j) })

	for i := range n {
		if got := q.pop().ID; got != want[i].job.ID {
			t.Fatalf("pop %d = %s, want %s", i, got, want[i].job.ID)
		}
	}
	if q.len() != 0 {
		t.Fatalf("len = %d after draining, want 0", q.len())
	}
}

func TestQueueInterleavedPushAndPop(t *testing.T) {
	var q queue
	q.push(Job{ID: "a"}, epoch)
	q.push(Job{ID: "b"}, epoch)
	if got := q.pop().ID; got != "a" {
		t.Fatalf("pop = %s, want a", got)
	}
	q.push(Job{ID: "c"}, epoch)
	q.push(Job{ID: "d"}, epoch.Add(-time.Second))

	for _, want := range []string{"d", "b", "c"} {
		if got := q.pop().ID; got != want {
			t.Fatalf("pop = %s, want %s", got, want)
		}
	}
}

func BenchmarkQueuePushPop(b *testing.B) {
	var q queue
	for i := range 50_000 {
		q.push(Job{}, epoch.Add(time.Duration(i)))
	}
	b.ResetTimer()
	for i := range b.N {
		q.push(Job{}, epoch.Add(time.Duration(i)))
		q.pop()
	}
}
