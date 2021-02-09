package loop

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dshulyak/uring"
)

var (
	// ErrClosed returned if queue was closed.
	ErrClosed = errors.New("uring: closed")
	// closed is a bit set to sqe.userData to notify completionLoop that ring
	// is being closed.
	closed uint64 = 1 << 63
)

func newResult() *result {
	return &result{
		ch: make(chan struct{}, 1),
	}
}

// result is an object for sending completion notifications.
type result struct {
	free  uint32
	nonce uint32

	uring.CQEntry
	ch chan struct{}
}

func (r *result) isFree() bool {
	return atomic.LoadUint32(&r.free) == 0
}

func (r *result) unfree() {
	atomic.StoreUint32(&r.free, 1)
}

func (r *result) setFree() {
	atomic.StoreUint32(&r.free, 0)
}

type SQOperation func(sqe *uring.SQEntry)

func newQueue(ring *uring.Ring, qp *Params) *queue {
	var (
		minComplete uint32
	)
	if qp.WaitMethod == WaitEnter {
		minComplete = 1
	}
	results := make([]*result, ring.CQSize())
	for i := range results {
		results[i] = newResult()
	}
	subLock := sync.Mutex{}

	q := &queue{
		ring:            ring,
		submissionTimer: qp.SubmissionTimer,
		signal:          make(chan struct{}, 1),
		results:         results,
		limit:           ring.CQSize(),
		submitLimit:     ring.SQSize(),
		submitEvent:     sync.NewCond(&subLock),
		minComplete:     minComplete,
	}
	q.startSubmitLoop()
	return q
}

// queue provides thread safe access to uring.Ring instance.
type queue struct {
	ring        *uring.Ring
	minComplete uint32

	submissionTimer time.Duration

	mu     sync.Mutex
	nonce  uint32
	closed bool

	signal chan struct{}

	wg sync.WaitGroup

	// results is a free-list with a static size.
	// it is as large as completion queue.
	// it will happen that some completions will be faster than other,
	// in such case complexity of getting free result will grow from O(1) to O(n)
	// worst case
	results []*result

	inflight uint32
	// completion queue size
	limit uint32

	// submission queue size
	submitLimit uint32

	submitEvent  *sync.Cond
	submitCount  uint32
	submitCloser chan error
}

func (q *queue) startSubmitLoop() {
	if q.submissionTimer == 0 {
		return
	}
	q.wg.Add(1)

	var (
		duration = q.submissionTimer
		timeout  = false

		timer = time.AfterFunc(duration, func() {
			q.submitEvent.L.Lock()
			timeout = true
			q.submitEvent.Signal()
			q.submitEvent.L.Unlock()
		})
	)
	go func() {
		defer q.wg.Done()
		defer timer.Stop()
		for {
			q.submitEvent.L.Lock()

			// event is fired:
			// - when queue is full
			// - on timer
			// - when queue is closed

			for q.submitCount != q.submitLimit && !timeout && q.submitCloser == nil {
				q.submitEvent.Wait()
			}
			total := q.submitCount
			timed := timeout
			closed := q.submitCloser

			timeout = false
			q.submitCount = 0
			q.submitEvent.L.Unlock()

			if closed != nil {
				closed <- nil
				return
			}

			if total > 0 {
				_, err := q.ring.Enter(total, 0)
				if err != nil {
					panic(err)
				}
			}
			if !timed {
				timer.Stop()
			}
			timer.Reset(duration)
		}
	}()
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
		// gosched is needed if q.minComplete = 0 without eventfd
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
	// we will wait if submition queue is full.
	// it won't be long cause if it is full submition loop receives
	// event to submit pending immediatly.
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
		if res.isFree() {
			break
		}
		q.nonce++
	}
	res.unfree()
	res.nonce = q.nonce

	sqe.SetUserData(uint64(q.nonce))
	q.nonce++
	return res
}

func (q *queue) submit(n uint32) error {
	q.ring.Flush()
	if q.submissionTimer == 0 {
		// for sync submit unlock before enter
		q.mu.Unlock()
		_, err := q.ring.Enter(n, 0)
		return err
	}
	// for async submit unlock after notifying batch submitter
	defer q.mu.Unlock()
	q.submitEvent.L.Lock()
	q.submitCount += n
	if q.submitCount == q.submitLimit {
		q.submitEvent.Signal()
	}
	closed := q.submitCloser != nil
	q.submitEvent.L.Unlock()
	if closed {
		return ErrClosed
	}
	return nil
}

func (q *queue) Ring() *uring.Ring {
	return q.ring
}

//go:norace

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

	err := q.submit(1)
	if err != nil {
		return uring.CQEntry{}, err
	}
	// wait
	_, open := <-res.ch
	cqe := res.CQEntry
	if cqe.UserData() != uint64(res.nonce) {
		panic("received result for a wrong request")
	}

	// fillResult method always checks if result is free by atomically loading
	// free marker. on x86 write is never reordered with older reads
	// but on systems with less strong memory model it might be possible
	//
	// in this part i rely on store/release - load/acquire semantics
	// to enforce earlier described contstraint
	res.setFree()
	q.completed(1)
	if !open {
		return uring.CQEntry{}, ErrClosed
	}
	return cqe, nil
}

//go:norace

// Batch submits operations atomically and in the order they are provided.
func (q *queue) Batch(cqes []uring.CQEntry, opts []SQOperation) ([]uring.CQEntry, error) {
	n := uint32(len(opts))
	// lock is acqured in prepare and released in submit guarantees
	// that operations are sent in order. e.g. no other goroutine can concurrently
	// chime in due to runtime.Gosched in getSQEntry
	if err := q.prepare(n); err != nil {
		return nil, err
	}
	results := make([]*result, len(opts))
	for i := range opts {
		sqe := q.getSQEntry()
		opts[i](sqe)
		results[i] = q.fillResult(sqe)
	}

	err := q.submit(n)
	if err != nil {
		return nil, err
	}

	exit := false
	for _, res := range results {
		_, open := <-res.ch
		res.setFree()
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

	if q.submissionTimer != 0 {
		q.submitEvent.L.Lock()
		q.submitCloser = make(chan error, 1)
		q.submitEvent.Signal()
		q.submitEvent.L.Unlock()

		<-q.submitCloser
	}

	sqe := q.ring.GetSQEntry()
	uring.Nop(sqe)
	sqe.SetUserData(closed)
	sqe.SetFlags(uring.IOSQE_IO_DRAIN)

	_, err := q.ring.Submit(0)
	if err != nil {
		return err
	}
	q.wg.Wait()
	for _, req := range q.results {
		close(req.ch)
	}
	q.results = nil
	return nil
}
