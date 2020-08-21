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

func newResult() *Result {
	return &Result{
		ch: make(chan struct{}, 1),
	}
}

// Result is an object for sending completion notifications.
type Result struct {
	ch chan struct{}
	uring.CQEntry
}

func newQueue(ring *uring.Ring, qp *Params) *queue {
	var (
		inflight    uint32
		reqmu       sync.Mutex
		minComplete uint32
	)
	if qp.WaitMethod == WaitEnter {
		minComplete = 1
	}
	results := make([]*Result, ring.CQSize()*2)
	for i := range results {
		results[i] = newResult()
	}
	return &queue{
		ring:        ring,
		reqCond:     sync.NewCond(&reqmu),
		results:     results,
		inflight:    &inflight,
		minComplete: minComplete,
	}
}

// queue provides thread safe access to uring.Ring instance.
type queue struct {
	ring        *uring.Ring
	minComplete uint32

	reqCond *sync.Cond
	nonce   uint32
	closed  bool
	wg      sync.WaitGroup

	rmu     sync.Mutex
	results []*Result

	inflight *uint32
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
	// this is always non-blocking
	req.ch <- struct{}{}
	return true
}

func (q *queue) prepare() (*uring.SQEntry, error) {
	q.reqCond.L.Lock()
	if q.closed {
		q.reqCond.L.Unlock()
		return nil, ErrClosed
	}
	inflight := atomic.AddUint32(q.inflight, 1)
	if inflight > q.ring.CQSize() {
		q.reqCond.Wait()
		if q.closed {
			q.reqCond.L.Unlock()
			return nil, ErrClosed
		}
	}
	// with sqpoll we cannot rely on mutex that guards Enter
	// if it happens that all sqe's were filled but sq polling thread didn't
	// update the head yet - program will crash
	for {
		entry := q.ring.GetSQEntry()
		if entry != nil {
			return entry, nil
		}
		runtime.Gosched()
	}
}

//go:norace
// q.results is a free list that never changes size, race-free access to a certain
// position covered by an algorithm.

func (q *queue) completeAsync(sqe *uring.SQEntry) (*Result, error) {
	sqe.SetUserData(uint64(q.nonce))
	req := q.results[q.nonce%uint32(len(q.results))]

	q.nonce++

	q.ring.Flush()

	// it is safe to unlock before enter, only if there are more sq slots available after this one was flushed.
	// if there are no slots then the one of the goroutines will have to wait in the loop until sqe is ready (not nil).

	q.reqCond.L.Unlock()
	_, err := q.ring.Enter(1, 0)

	if err != nil {
		return nil, err
	}
	return req, nil
}

func (q *queue) complete(sqe *uring.SQEntry) (uring.CQEntry, error) {
	req, err := q.completeAsync(sqe)
	if err != nil {
		return uring.CQEntry{}, err
	}
	_, open := <-req.ch
	cqe := req.CQEntry
	if atomic.AddUint32(q.inflight, ^uint32(0)) >= q.ring.CQSize() {
		q.reqCond.L.Lock()
		q.reqCond.Signal()
		q.reqCond.L.Unlock()
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
	q.reqCond.L.Lock()
	q.closed = true
	q.reqCond.Broadcast()
	q.reqCond.L.Unlock()

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
		if req == nil {
			continue
		}
		close(req.ch)
	}
	q.results = nil
	return nil
}
