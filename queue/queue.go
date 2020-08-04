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
	Closed        = errors.New("closed")
	closed uint64 = 1 << 63
)

var requestPool = sync.Pool{
	New: func() interface{} {
		return &request{
			ch: make(chan struct{}, 1),
		}
	},
}

type request struct {
	sqe uring.SQEntry

	ch chan struct{}
	uring.CQEntry
}

func (r *request) Wait() <-chan struct{} {
	return r.ch
}

func (r *request) Dispose() {
	requestPool.Put(r)
}

func New(ring *uring.Ring) *Queue {
	var inflight uint32
	q := &Queue{
		requests: make(chan *request, ring.SQSize()),
		quit:     make(chan struct{}),
		inflight: &inflight,
		wakeS:    make(chan struct{}, 1),
		ring:     ring,
		results:  make(map[uint64]*request, ring.CQSize()),
	}
	q.wg.Add(2)
	go q.completionLoop()
	go q.submitionLoop()
	return q
}

// Queue synchronizes access to uring.Ring instance.
// Future optimizations:
// - use (lock-free?) buffer for submitting requests
//   will allow to submit entries in a batch, if they are added concurrently
// Note:
// - Completion loop with syscall is always slower.
//   epoll_wait and uring_enter are both slower with low number of workers.
//   But it keeps one of the golang P busy. Maybe this tradeoff
type Queue struct {
	wg   sync.WaitGroup
	quit chan struct{}

	requests chan *request

	rmu     sync.Mutex
	results map[uint64]*request

	inflight *uint32
	wakeS    chan struct{}

	ring *uring.Ring
}

func (q *Queue) Complete(sqe uring.SQEntry) (uring.CQEntry, error) {
	req := requestPool.Get().(*request)
	req.sqe = sqe

	select {
	case q.requests <- req:
		<-req.ch
		cqe := req.CQEntry
		req.Dispose()
		return cqe, nil
	case <-q.quit:
		return uring.CQEntry{}, Closed
	}
}

// CompleteAsync will not block, and will allow caller to send many requests
// before blocking goroutine.
func (q *Queue) CompleteAsync(sqe uring.SQEntry) (*request, error) {
	req := requestPool.Get().(*request)
	req.sqe = sqe

	select {
	case q.requests <- req:
		return req, nil
	case <-q.quit:
		return nil, Closed
	}
}

func (q *Queue) completionLoop() {
	defer q.wg.Done()
	wake := q.ring.CQSize() - 1
	for {
		cqe, err := q.ring.GetCQEntry(0)
		if err == syscall.EAGAIN || err == syscall.EINTR {
			runtime.Gosched()
			continue
		} else if err != nil {
			// FIXME
			panic(err)
		}

		if cqe.UserData()&closed > 0 {
			return
		}

		q.rmu.Lock()
		req := q.results[cqe.UserData()]
		delete(q.results, cqe.UserData())
		q.rmu.Unlock()

		req.CQEntry = cqe
		req.ch <- struct{}{}

		switch atomic.AddUint32(q.inflight, ^uint32(0)) {
		case wake:
			q.wakeS <- struct{}{}
		}
	}
}

func (q *Queue) submitionLoop() {
	defer q.wg.Done()
	var (
		active chan *request = q.requests
		nonce  uint64
	)
	for {
		select {
		case <-q.wakeS:
			active = q.requests
		case req := <-active:
			sqe := req.sqe
			sqe.SetUserData(nonce)

			q.rmu.Lock()
			q.results[nonce] = req
			q.rmu.Unlock()

			_ = q.ring.Push(sqe)
			_, err := q.ring.Submit(0)
			if err != nil {
				// FIXME
				panic(err)
			}

			// completionLoop will wait on wakeC only after LoadUint32
			// returned 0
			switch atomic.AddUint32(q.inflight, 1) {
			case q.ring.CQSize():
				active = nil
			}
			nonce++
		case <-q.quit:
			var sqe uring.SQEntry
			uring.Nop(&sqe)
			sqe.SetUserData(closed)

			_ = q.ring.Push(sqe)
			_, err := q.ring.Submit(0)
			if err != nil {
				//FIXME
				panic(err)
			}
			return
		}
	}
}

func (q *Queue) Close() {
	close(q.quit)
	q.wg.Wait()
	for nonce, req := range q.results {
		req.ch <- struct{}{}
		delete(q.results, nonce)
	}

}
