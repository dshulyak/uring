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
	var subCount uint32
	q := &Queue{
		subms:    make(chan request, ring.SQSize()),
		compls:   make(chan uring.CQEntry, ring.CQSize()),
		quit:     make(chan struct{}),
		subCount: &subCount,
		wakeC:    make(chan struct{}, 1),
		ring:     ring,
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

	subms  chan request
	compls chan uring.CQEntry

	subCount *uint32
	wakeC    chan struct{}

	ring *uring.Ring
}

func (q *Queue) Complete(sqe uring.SQEntry) (uring.CQEntry, error) {
	req := request{
		sqe: sqe,
		cqe: completionPool.Get().(chan uring.CQEntry),
	}
	select {
	case q.subms <- req:
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
	for {
		if atomic.LoadUint32(q.subCount) == 0 {
			select {
			case <-q.quit:
				return
			case <-q.wakeC:
			}
		}
		// TODO why GetCQEntry(1) returns EINTR frequently?
		cqe, err := q.ring.GetCQEntry(0)
		if err == syscall.EAGAIN || err == syscall.EINTR {
			continue
		} else if err != nil {
			// FIXME
			panic(err)
		}
		q.compls <- cqe
		atomic.AddUint32(q.subCount, ^uint32(0))
	}
}

func (q *Queue) submitionLoop() {
	defer q.wg.Done()
	var (
		results              = map[uint64]chan uring.CQEntry{}
		active  chan request = q.subms
		limit                = q.ring.CQSize()
		nonce   uint64
	)
	for {
		if limit == 0 {
			active = nil
		} else {
			active = q.subms
		}
		select {
		case cqe := <-q.compls:
			results[cqe.UserData()] <- cqe
			delete(results, cqe.UserData())
			limit++
		case req := <-active:
			limit--
			sqe := req.sqe
			sqe.SetUserData(nonce)
			results[nonce] = req.cqe

			total := q.ring.Push(sqe)

			_, err := q.ring.Submit(total, 0)
			if err != nil {
				// FIXME
				panic(err)
			}

			// completionLoop will block on wakeC only after LoadUint32
			// returned 0
			if atomic.AddUint32(q.subCount, total) == total {
				q.wakeC <- struct{}{}
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
