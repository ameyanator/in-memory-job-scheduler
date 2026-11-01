package scheduler

import (
	"container/heap"
	"time"
)

// queue is a min-heap of waiting jobs ordered by key, then submission order.
// Push and pop are O(log n). It is not safe for concurrent use.
type queue struct {
	items   itemHeap
	nextSeq uint64
}

type item struct {
	key time.Time
	seq uint64 // Submission order; breaks key ties (O3).
	job Job
}

func (q *queue) len() int { return q.items.Len() }

func (q *queue) push(job Job, key time.Time) {
	heap.Push(&q.items, item{key: key, seq: q.nextSeq, job: job})
	q.nextSeq++
}

// pop removes and returns the lowest-ordered job. The queue must not be empty.
func (q *queue) pop() Job {
	return heap.Pop(&q.items).(item).job
}

// itemHeap implements heap.Interface.
type itemHeap []item

func (h itemHeap) Len() int { return len(h) }

func (h itemHeap) Less(i, j int) bool {
	if c := h[i].key.Compare(h[j].key); c != 0 {
		return c < 0
	}
	return h[i].seq < h[j].seq
}

func (h itemHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *itemHeap) Push(x any) { *h = append(*h, x.(item)) }

func (h *itemHeap) Pop() any {
	old := *h
	last := len(old) - 1
	it := old[last]
	old[last] = item{} // Release the job's closure for GC.
	*h = old[:last]
	return it
}
