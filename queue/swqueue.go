package queue

import (
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/dshulyak/uring"
)

func NewSWQueue(ring *uring.Ring) *SWQueue {
	var inflight uint32
	q := &SWQueue{
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

// SWQueue can be use only from a single thread.
type SWQueue struct {
	wg   sync.WaitGroup
	quit chan struct{}

	rmu     sync.Mutex
	results map[uint64]*request

	inflight *uint32
	wakeS    chan struct{}

	nonce uint32

	ring *uring.Ring
}

func (q *SWQueue) completionLoop() {
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

func (q *SWQueue) CompleteAsync(sqe uring.SQEntry) (*request, error) {
	if atomic.AddUint32(q.inflight, 1) > q.ring.CQSize() {
		select {
		case <-q.wakeS:
		case <-q.quit:
			return nil, Closed
		}
	}
	sqe.SetUserData(uint64(q.nonce))
	req := requestPool.Get().(*request)
	q.rmu.Lock()
	q.results[uint64(q.nonce)] = req
	q.rmu.Unlock()

	sqe.SetUserData(uint64(q.nonce))
	_ = q.ring.Push(sqe)
	_, err := q.ring.Submit(1, 0)

	q.nonce++
	return req, err

}

func (q *SWQueue) Complete(sqe uring.SQEntry) (uring.CQEntry, error) {
	req, err := q.CompleteAsync(sqe)
	if err != nil {
		return uring.CQEntry{}, err
	}
	<-req.ch
	req.Dispose()
	return req.CQEntry, nil
}

func (q *SWQueue) Close() error {
	close(q.quit)

	var sqe uring.SQEntry
	uring.Nop(&sqe)
	sqe.SetUserData(closed)

	_ = q.ring.Push(sqe)
	_, err := q.ring.Submit(1, 0)
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
