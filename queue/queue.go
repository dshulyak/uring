package queue

import (
	"errors"
	"sync"
	"syscall"

	"github.com/dshulyak/uring"
)

var Closed = errors.New("closed")

type request struct {
	sqe uring.SQEntry
	cqe chan uring.CQEntry
}

func New(ring *uring.Ring) *Queue {
	q := &Queue{
		subms:  make(chan request, ring.SQSize()),
		compls: make(chan uring.CQEntry, ring.CQSize()),
		quit:   make(chan struct{}),
		ring:   ring,
	}
	q.wg.Add(2)
	go q.completionLoop()
	go q.submitionLoop()
	return q
}

// Queue synchronizes access to uring.Ring instance.
// Goals:
// - goroutines that are waiting must be parked
// - should not overflow uring completion queue
// - completion entries must be returned as soon as they are available
// - available submission entries should be submitted in one batch
type Queue struct {
	wg   sync.WaitGroup
	quit chan struct{}

	subms  chan request
	compls chan uring.CQEntry

	ring *uring.Ring
}

func (q *Queue) Complete(sqe uring.SQEntry) (uring.CQEntry, error) {
	req := request{
		sqe: sqe,
		cqe: make(chan uring.CQEntry, 1),
	}
	select {
	case q.subms <- req:
		select {
		case cqe := <-req.cqe:
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
		select {
		case <-q.quit:
			return
		default:
		}
		cqe, err := q.ring.GetCQEntry(0)
		if errors.Is(err, syscall.EAGAIN) {
			continue
		} else if err != nil {
			panic(err)
		}
		q.compls <- cqe
	}
}

func (q *Queue) submitionLoop() {
	defer q.wg.Done()
	var (
		results              = map[uint64]chan uring.CQEntry{}
		active  chan request = q.subms
		limit                = q.ring.CQSize()
		sqsize               = q.ring.SQSize()
		nonce   uint64
	)
	for {
		select {
		case cqe := <-q.compls:
			results[cqe.UserData()] <- cqe
			delete(results, cqe.UserData())
			if limit == 0 {
				active = q.subms
			}
			limit++
		case req := <-active:
			var total uint32
			for {
				limit--
				sqe := req.sqe
				sqe.SetUserData(nonce)
				results[nonce] = req.cqe
				nonce++
				total += q.ring.Push(sqe)
				if limit == 0 {
					active = nil
					break
				}
				if total == sqsize {
					break
				}
				exit := false
				select {
				case req = <-active:
				default:
					exit = true
				}
				if exit {
					break
				}
			}
			_, err := q.ring.Submit(total, 0)
			if err != nil {
				panic(err)
			}
		case <-q.quit:
			return
		}
	}
}

func (q *Queue) Close() {
	close(q.quit)
	q.wg.Wait()
}
