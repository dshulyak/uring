package queue

import (
	"errors"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/dshulyak/uring"
)

var Closed = errors.New("closed")

var completionPool = sync.Pool{
	New: func() interface{} {
		return make(chan uring.CQEntry, 1)
	},
}

type request struct {
	sqe uring.SQEntry
	cqe chan uring.CQEntry
}

func New(ring *uring.Ring) *Queue {
	var inflight uint32
	q := &Queue{
		requests: make(chan request, ring.SQSize()),
		quit:     make(chan struct{}),
		inflight: &inflight,
		wakeC:    make(chan struct{}, 1),
		wakeS:    make(chan struct{}, 1),
		ring:     ring,
		results:  make(map[uint64]chan uring.CQEntry, ring.CQSize()),
	}
	q.wg.Add(2)
	go q.completionLoop()
	go q.submitionLoop()
	return q
}

// Queue synchronizes access to uring.Ring instance.
type Queue struct {
	wg   sync.WaitGroup
	quit chan struct{}

	requests chan request

	rmu     sync.Mutex
	results map[uint64]chan uring.CQEntry

	inflight     *uint32
	wakeC, wakeS chan struct{}

	ring *uring.Ring
}

func (q *Queue) Complete(sqe uring.SQEntry) (uring.CQEntry, error) {
	req := request{
		sqe: sqe,
		cqe: completionPool.Get().(chan uring.CQEntry),
	}
	select {
	case q.requests <- req:
		select {
		case cqe := <-req.cqe:
			completionPool.Put(req.cqe)
			return cqe, nil
		case <-q.quit:
			return uring.CQEntry{}, Closed
		}
	case <-q.quit:
		return uring.CQEntry{}, Closed
	}
}

func (q *Queue) completionLoop() {
	defer q.wg.Done()
	select {
	case <-q.quit:
		return
	case <-q.wakeC:
	}
	wake := q.ring.CQSize() - 1
	for {
		// TODO why GetCQEntry(1) returns EINTR frequently?
		cqe, err := q.ring.GetCQEntry(0)
		if err == syscall.EAGAIN || err == syscall.EINTR {
			continue
		} else if err != nil {
			// FIXME
			panic(err)
		}

		q.rmu.Lock()
		result := q.results[cqe.UserData()]
		delete(q.results, cqe.UserData())
		q.rmu.Unlock()

		result <- cqe

		switch atomic.AddUint32(q.inflight, ^uint32(0)) {
		case wake:
			q.wakeS <- struct{}{}
		case 0:
			select {
			case <-q.quit:
				return
			case <-q.wakeC:
			}
		}
	}
}

func (q *Queue) submitionLoop() {
	defer q.wg.Done()
	var (
		active chan request = q.requests
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
			q.results[nonce] = req.cqe
			q.rmu.Unlock()

			total := q.ring.Push(sqe)
			_, err := q.ring.Submit(total, 0)
			if err != nil {
				// FIXME
				panic(err)
			}

			// completionLoop will wait on wakeC only after LoadUint32
			// returned 0
			switch atomic.AddUint32(q.inflight, total) {
			case total:
				q.wakeC <- struct{}{}
			case q.ring.CQSize():
				active = nil
			}
			nonce++
		case <-q.quit:
			return
		}
	}
}

func (q *Queue) Close() {
	close(q.quit)
	q.wg.Wait()
}
