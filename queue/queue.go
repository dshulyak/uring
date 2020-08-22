package queue

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/dshulyak/uring"
)

var (
	// ErrClosed returned if queue was closed.
	ErrClosed = errors.New("closed")
	// closed is a bit set to sqe.userData to notify completionLoop that ring
	// is being closed.
	closed uint64 = 1 << 63
)

func newResult() *result {
	return &result{
		ch:   make(chan struct{}, 1),
		free: true,
	}
}

// result is an object for sending completion notifications.
type result struct {
	uring.CQEntry
	ch   chan struct{}
	free bool
}

func newQueue(ring *uring.Ring, qp *Params) *queue {
	var (
		minComplete uint32
	)
	if qp.WaitMethod == WaitEnter {
		minComplete = 1
	}
	results := make([]*result, ring.CQSize()*2)
	for i := range results {
		results[i] = newResult()
	}
	return &queue{
		ring:        ring,
		signal:      make(chan struct{}, 1),
		results:     results,
		limit:       ring.CQSize(),
		minComplete: minComplete,
	}
}

// queue provides thread safe access to uring.Ring instance.
type queue struct {
	ring        *uring.Ring
	minComplete uint32

	mu     sync.Mutex
	nonce  uint32
	closed bool

	signal chan struct{}

	wg sync.WaitGroup

	results []*result

	inflight, limit uint32
}

func (q *queue) startCompletionLoop() {
	q.wg.Add(1)
	go q.completionLoop()
}

// completionLoop ...
func (q *queue) completionLoop() {
	defer q.wg.Done()
	for q.tryComplete() {
	}
}

func (q *queue) tryComplete() bool {
	cqe, err := q.ring.GetCQEntry(q.minComplete)
	// EAGAIN - if head is equal to tail of completion queue
	if err == syscall.EAGAIN || err == syscall.EINTR {
		//gosched is needed if q.minComplete = 0 without eventfd
		runtime.Gosched()
		return true
	} else if err != nil {
		// FIXME
		panic(err)
	}
	if cqe.UserData()&closed > 0 {
		return false
	}

	req := q.results[cqe.UserData()%uint64(len(q.results))]
	req.CQEntry = cqe
	req.ch <- struct{}{}
	return true
}

func (q *queue) prepare() (*uring.SQEntry, error) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil, ErrClosed
	}
	inflight := atomic.AddUint32(&q.inflight, 1)
	if inflight > q.limit {
		<-q.signal
		if q.closed {
			q.mu.Unlock()
			return nil, ErrClosed
		}
	}
	for {
		entry := q.ring.GetSQEntry()
		if entry != nil {
			return entry, nil
		}
		runtime.Gosched()
	}
}

//go:norace
// norace is for results, results is a free-list with a static size,
// it is twice as large as a cq. we provide a guarantee that no more
// than cq size of entries are inflight at the same time, it means that any
// particular result will be reused only after cq entries were completed.
// if it happens that some submissions are stuck in the queue, then we need
// associated result.
// In the worst case performance of the results free-list will o(n),
// but for it to happen we need to have submission that can take longer to complete
// then all other submissions in the queue.

func (q *queue) complete(sqe *uring.SQEntry) (uring.CQEntry, error) {
	var req *result
	for {
		req = q.results[q.nonce%uint32(len(q.results))]
		if req.free {
			break
		}
		q.nonce++
	}
	req.free = false

	sqe.SetUserData(uint64(q.nonce))
	q.nonce++
	q.ring.Flush()

	// it is safe to unlock before enter, only if there are more sq slots available after this one was flushed.
	// if there are no slots then the one of the goroutines will have to wait in the loop until sqe is ready (not nil).

	q.mu.Unlock()

	_, err := q.ring.Enter(1, 0)
	if err != nil {
		return uring.CQEntry{}, err
	}
	_, open := <-req.ch
	cqe := req.CQEntry
	req.free = true

	if atomic.AddUint32(&q.inflight, ^uint32(0)) == q.limit {
		q.signal <- struct{}{}
	}
	if !open {
		return uring.CQEntry{}, ErrClosed
	}
	return cqe, nil
}

func (q *queue) Ring() *uring.Ring {
	return q.ring
}

// Complete blocks until an available submission exists, submits and blocks until completed.
// Goroutine that executes Complete will be parked.
func (q *queue) Complete(f func(*uring.SQEntry)) (uring.CQEntry, error) {
	sqe, err := q.prepare()
	if err != nil {
		return uring.CQEntry{}, err
	}
	f(sqe)
	return q.complete(sqe)
}

func (q *queue) Close() error {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	close(q.signal)

	sqe := q.ring.GetSQEntry()
	uring.Nop(sqe)
	sqe.SetUserData(closed)
	sqe.SetFlags(uring.IOSQE_IO_DRAIN)

	_, err := q.ring.Submit(0)
	if err != nil {
		//FIXME
		return err
	}
	q.wg.Wait()
	for _, req := range q.results {
		close(req.ch)
	}
	q.results = nil
	return nil
}
