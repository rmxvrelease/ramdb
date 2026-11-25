// Package queue provides a generic, non-concurrent FIFO queue.
package queue

// Queue is a generic first-in-first-out queue backed by a ring buffer.
// It is not safe for concurrent use; guard it with a sync.Mutex if needed.
type Queue[T any] struct {
	buf   []T
	head  int // index of the front element
	count int // number of elements currently stored
}

// New returns an empty queue.
func New[T any]() *Queue[T] {
	return &Queue[T]{}
}

// NewWithCapacity returns an empty queue with room preallocated for n elements.
func NewWithCapacity[T any](n int) *Queue[T] {
	if n < 0 {
		n = 0
	}
	return &Queue[T]{buf: make([]T, n)}
}

// Len reports how many elements are in the queue.
func (q *Queue[T]) Len() int { return q.count }

// IsEmpty reports whether the queue has no elements.
func (q *Queue[T]) IsEmpty() bool { return q.count == 0 }

// Enqueue adds v to the back of the queue.
func (q *Queue[T]) Enqueue(v T) {
	if q.count == len(q.buf) {
		q.grow()
	}
	tail := (q.head + q.count) % len(q.buf)
	q.buf[tail] = v
	q.count++
}

// Dequeue removes and returns the front element. The bool is false if the
// queue was empty, in which case the zero value of T is returned.
func (q *Queue[T]) Dequeue() (T, bool) {
	var zero T
	if q.count == 0 {
		return zero, false
	}
	v := q.buf[q.head]
	q.buf[q.head] = zero // release reference so the GC can reclaim it
	q.head = (q.head + 1) % len(q.buf)
	q.count--
	return v, true
}

// Peek returns the front element without removing it. The bool is false if the
// queue is empty.
func (q *Queue[T]) Peek() (T, bool) {
	var zero T
	if q.count == 0 {
		return zero, false
	}
	return q.buf[q.head], true
}

// PeekBack returns the back (most recently enqueued) element without removing
// it. The bool is false if the queue is empty.
func (q *Queue[T]) PeekBack() (T, bool) {
	var zero T
	if q.count == 0 {
		return zero, false
	}
	return q.buf[(q.head+q.count-1)%len(q.buf)], true
}

// Clear removes all elements, releasing references so they can be collected.
func (q *Queue[T]) Clear() {
	var zero T
	for i := range q.buf {
		q.buf[i] = zero
	}
	q.head = 0
	q.count = 0
}

// grow doubles the buffer capacity (or allocates a small buffer when empty)
// and re-lays the elements out starting at index 0.
func (q *Queue[T]) grow() {
	newCap := len(q.buf) * 2
	if newCap == 0 {
		newCap = 8
	}
	newBuf := make([]T, newCap)
	// Copy existing elements into the new buffer in FIFO order.
	for i := 0; i < q.count; i++ {
		newBuf[i] = q.buf[(q.head+i)%len(q.buf)]
	}
	q.buf = newBuf
	q.head = 0
}
