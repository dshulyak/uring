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
		quit:     make(chan struct{}),
		inflight: &inflight,
		wakeS:    make(chan struct{}, 1),
		ring:     ring,
		results:  make(map[uint64]*request, ring.CQSize()),
	}
	q.wg.Add(1)
	go q.completionLoop()
	return q
}

// Queue provides thread safe access to uring.Ring instance.
type Queue struct {
	wg   sync.WaitGroup
	quit chan struct{}

	reqmu sync.Mutex

	rmu     sync.Mutex
	results map[uint64]*request

	inflight *uint32
	wakeS    chan struct{}

	nonce uint32

	ring *uring.Ring
}

// completionLoop ...
// Spinning with gosched allows to reap completions ~20% faster.
func (q *Queue) completionLoop() {
	defer q.wg.Done()
	wake := q.ring.CQSize()
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

		if atomic.AddUint32(q.inflight, ^uint32(0)) == wake {
			q.wakeS <- struct{}{}
		}
	}
}

func (q *Queue) CompleteAsync(sqe uring.SQEntry) (*request, error) {
	q.reqmu.Lock()
	if atomic.AddUint32(q.inflight, 1) > q.ring.CQSize() {
		select {
		case <-q.wakeS:
		case <-q.quit:
			q.reqmu.Unlock()
			return nil, Closed
		}
	}
	sqe.SetUserData(uint64(q.nonce))
	req := requestPool.Get().(*request)

	q.rmu.Lock()
	q.results[uint64(q.nonce)] = req
	q.rmu.Unlock()

	sqe.SetUserData(uint64(q.nonce))
	_ = q.ring.Flush(sqe)

	q.nonce++
	q.reqmu.Unlock()

	// submitting sqe in batch doesn't make any substantial difference
	_, err := q.ring.Enter(1, 0)
	if err != nil {
		return nil, err
	}

	return req, nil

}

func (q *Queue) Complete(sqe uring.SQEntry) (uring.CQEntry, error) {
	req, err := q.CompleteAsync(sqe)
	if err != nil {
		return uring.CQEntry{}, err
	}
	<-req.ch
	cqe := req.CQEntry
	req.Dispose()
	return cqe, nil
}

func (q *Queue) Close() error {
	close(q.quit)

	var sqe uring.SQEntry
	uring.Nop(&sqe)
	sqe.SetUserData(closed)

	_ = q.ring.Push(sqe)
	_, err := q.ring.Submit(0)
	if err != nil {
		//FIXME
		return err
	}
	q.wg.Wait()
	for nonce, req := range q.results {
		req.ch <- struct{}{}
		delete(q.results, nonce)
	}
	return nil
}
