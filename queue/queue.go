package queue

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/dshulyak/uring"
)

var (
	// ErrClosed returned if queue was closed.
	ErrClosed = errors.New("closed")
	// closed is a bit set to sqe.userData to notify completionLoop that ring
	// is being closed.
	closed uint64 = 1 << 63
)

var resultPool = sync.Pool{
	New: func() interface{} {
		return &Result{
			ch: make(chan struct{}, 1),
		}
	},
}

// Result is an object for sending completion notifications.
type Result struct {
	ch chan struct{}
	uring.CQEntry
}

// Wait returns channel for waiting. CQEntry is valid only if channel wasn't closed.
func (r *Result) Wait() <-chan struct{} {
	return r.ch
}

// Dispose puts Result back to the reusable pool.
func (r *Result) Dispose() {
	resultPool.Put(r)
}

// QueueOption ...
type QueueOption func(q *Queue)

// WAIT will make completion loop to Enter with minComplete=1.
// During idle periods completionLoop thread will sleep.
//
// Registering files and buffers requires uring to become idle, with WAIT we are
// entering the queue and waiting until the next event is completed. Even if queue is
// empty this makes uring think that it is not idle. As a consequence Registering files
// or buffers leads to deadlock.
func WAIT(q *Queue) {
	q.minComplete = 1
}

// POLL will make completion loop to poll uring completion queue until
// entry is available.
// Polling is somewhat faster but will use more cpu, especially during idle periods.
func POLL(q *Queue) {
	q.minComplete = 0
}

// Setup io_uring instance and return instance of the queue.
func Setup(size uint, params *uring.IOUringParams, opts ...QueueOption) (*Queue, error) {
	ring, err := uring.Setup(size, params)
	if err != nil {
		return nil, err
	}
	q := New(ring)
	for _, opt := range opts {
		opt(q)
	}
	return q, nil
}

func New(ring *uring.Ring) *Queue {
	q := newQueue(ring)
	q.closeRing = true
	q.wg.Add(1)
	go q.completionLoop()
	return q
}

func newQueue(ring *uring.Ring) *Queue {
	var (
		inflight uint32
		reqmu    sync.Mutex
	)
	return &Queue{
		ring:     ring,
		reqCond:  sync.NewCond(&reqmu),
		results:  make(map[uint64]*Result, ring.CQSize()),
		inflight: &inflight,
	}
}

// Queue provides thread safe access to uring.Ring instance.
type Queue struct {
	ring        *uring.Ring
	closeRing   bool
	minComplete uint32

	reqCond *sync.Cond
	nonce   uint32
	closed  bool
	wg      sync.WaitGroup

	rmu     sync.Mutex
	results map[uint64]*Result

	inflight *uint32
}

// completionLoop ...
// Spinning with gosched allows to reap completions ~20% faster.
func (q *Queue) completionLoop() {
	defer q.wg.Done()
	for q.tryComplete() {
	}
}

func (q *Queue) tryComplete() bool {
	cqe, err := q.ring.GetCQEntry(q.minComplete)
	// EAGAIN - if head is equal to tail of completion queue
	if err == syscall.EAGAIN || err == syscall.EINTR {
		runtime.Gosched()
		return true
	} else if err != nil {
		// FIXME
		panic(err)
	}

	if cqe.UserData()&closed > 0 {
		return false
	}

	q.rmu.Lock()
	req := q.results[cqe.UserData()]
	delete(q.results, cqe.UserData())
	q.rmu.Unlock()

	req.CQEntry = cqe
	req.ch <- struct{}{}

	if atomic.AddUint32(q.inflight, ^uint32(0)) >= q.ring.CQSize() {
		q.reqCond.L.Lock()
		q.reqCond.Signal()
		q.reqCond.L.Unlock()
	}
	return true
}

func (q *Queue) prepare() (*uring.SQEntry, error) {
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

func (q *Queue) completeAsync(sqe *uring.SQEntry) (*Result, error) {
	req := resultPool.Get().(*Result)

	q.rmu.Lock()
	q.results[uint64(q.nonce)] = req
	q.rmu.Unlock()
	sqe.SetUserData(uint64(q.nonce))

	q.nonce++

	q.ring.Flush()

	// it is safe to unlock before enter, only if there are more sq slots available after this one was flushed.
	// if there are no slots then the one of the goroutines will have to wait in the loop until sqe is not nil.

	q.reqCond.L.Unlock()
	_, err := q.ring.Enter(1, 0)

	if err != nil {
		return nil, err
	}
	return req, nil
}

func (q *Queue) complete(sqe *uring.SQEntry) (uring.CQEntry, error) {
	req, err := q.completeAsync(sqe)
	if err != nil {
		return uring.CQEntry{}, err
	}
	_, open := <-req.Wait()
	if !open {
		return uring.CQEntry{}, ErrClosed
	}
	cqe := req.CQEntry
	req.Dispose()
	return cqe, nil
}

func (q *Queue) Ring() *uring.Ring {
	return q.ring
}

//go:uintptrescapes

// Syscall ...
func (q *Queue) Syscall(addr uintptr, opts func(*uring.SQEntry)) (uring.CQEntry, error) {
	sqe, err := q.prepare()
	if err != nil {
		return uring.CQEntry{}, err
	}
	opts(sqe)
	sqe.SetAddr(uint64(addr))
	return q.complete(sqe)
}

// Complete blocks until an available submission exists, submits and blocks until completed.
// Goroutine that executes Complete will be parked.
func (q *Queue) Complete(f func(*uring.SQEntry)) (uring.CQEntry, error) {
	sqe, err := q.prepare()
	if err != nil {
		return uring.CQEntry{}, err
	}
	f(sqe)
	return q.complete(sqe)
}

// CompleteAsync blocks until an available submission exists, submits and returns future-like object.
// Caller must ensure pointers that were used for SQEntry will remain valid until completon.
// After request completed - caller should call result.Dispose()
func (q *Queue) CompleteAsync(f func(*uring.SQEntry)) (*Result, error) {
	sqe, err := q.prepare()
	if err != nil {
		return nil, err
	}
	f(sqe)
	return q.completeAsync(sqe)
}

func (q *Queue) Close() error {
	q.reqCond.L.Lock()
	q.closed = true
	q.reqCond.Broadcast()
	q.reqCond.L.Unlock()

	sqe := q.ring.GetSQEntry()
	uring.Nop(sqe)
	sqe.SetUserData(closed)

	_, err := q.ring.Submit(0)
	if err != nil {
		//FIXME
		return err
	}
	q.wg.Wait()
	for nonce, req := range q.results {
		close(req.ch)
		delete(q.results, nonce)
	}
	if q.closeRing {
		return q.ring.Close()
	}
	return nil
}

func (q *Queue) RegisterBuffers(ptr unsafe.Pointer, len uint64) error {
	return q.Ring().RegisterBuffers(ptr, len)
}

func (q *Queue) RegisterFiles(fds []int32) error {
	return q.Ring().RegisterFiles(fds)
}
