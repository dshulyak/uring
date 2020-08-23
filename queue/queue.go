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

type SQOperation func(sqe *uring.SQEntry)

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

	rmu     sync.Mutex
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

//go:norace
// results is a free-list with a static size,
// it is twice as large as a cq. we provide a guarantee that no more
// than cq size of entries are inflight at the same time, it means that any
// particular result will be reused only after cq number of entries were completed.
// if it happens that some submissions are stuck in the queue, then we can reuse
// associated result.
// In the worst case performance of the results array will be O(n),
// but for it to happen we need to have submission that can take longer to complete
// then all other submissions in the queue cumulatively.

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

// prepare acquires submission lock and registers n inflights operations.
func (q *queue) prepare(n uint32) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrClosed
	}
	inflight := atomic.AddUint32(&q.inflight, n)
	if inflight > q.limit {
		<-q.signal
		if q.closed {
			q.mu.Unlock()
			return ErrClosed
		}
	}
	return nil
}

func (q *queue) getSQEntry() *uring.SQEntry {
	// in general sq entry will be available without looping
	// but in case of SQPOLL we may need to wait here
	for {
		entry := q.ring.GetSQEntry()
		if entry != nil {
			return entry
		}
		runtime.Gosched()
	}
}

// completed must be called after all n completions were reaped and results are not needed.
func (q *queue) completed(n uint32) {
	if atomic.AddUint32(&q.inflight, ^uint32(n-1)) == q.limit {
		q.signal <- struct{}{}
	}
}

func (q *queue) fillResult(sqe *uring.SQEntry) *result {
	var res *result
	for {
		pos := q.nonce % uint32(len(q.results))
		res = q.results[pos]
		if res.free {
			break
		}
		q.nonce++
	}
	res.free = false

	sqe.SetUserData(uint64(q.nonce))
	q.nonce++
	return res
}

func (q *queue) submit(n uint32) error {
	q.ring.Flush()
	q.mu.Unlock()
	_, err := q.ring.Enter(n, 0)
	return err
}

func (q *queue) Ring() *uring.Ring {
	return q.ring
}

// Complete blocks until an available submission exists, submits and blocks until completed.
// Goroutine that executes Complete will be parked.
func (q *queue) Complete(opt SQOperation) (uring.CQEntry, error) {
	// acquire lock
	if err := q.prepare(1); err != nil {
		return uring.CQEntry{}, err
	}

	// get sqe and fill it with data
	sqe := q.getSQEntry()
	opt(sqe)
	res := q.fillResult(sqe)

	// submit to uring
	if err := q.submit(1); err != nil {
		return uring.CQEntry{}, err
	}

	// wait
	_, open := <-res.ch
	cqe := res.CQEntry
	res.free = true
	q.completed(1)
	if !open {
		return uring.CQEntry{}, ErrClosed
	}
	return cqe, nil
}

// Batch submits operations atomically and in the order they are provided.
func (q *queue) Batch(cqes []uring.CQEntry, opts []SQOperation) ([]uring.CQEntry, error) {
	n := uint32(len(opts))
	if err := q.prepare(n); err != nil {
		return nil, err
	}
	results := make([]*result, len(opts))
	for i := range opts {
		sqe := q.getSQEntry()
		opts[i](sqe)
		results[i] = q.fillResult(sqe)
	}

	if err := q.submit(n); err != nil {
		return nil, err
	}
	exit := false
	for _, res := range results {
		_, open := <-res.ch
		res.free = true
		q.completed(1)
		if !open {
			exit = true
			continue
		}
		cqes = append(cqes, res.CQEntry)
	}
	if exit {
		return nil, ErrClosed
	}
	return cqes, nil
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
