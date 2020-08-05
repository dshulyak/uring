package queue

import (
	"sync"
	"syscall"

	"github.com/dshulyak/uring"
)

// ShardedQueue distributes submissions over several shards, each shard running
// on its own ring. Completions are reaped using epoll on eventfd of the every ring.
// Benchmarks are inconclusive, with higher throughpout Sharded is somewhat faster than the
// regualr Queue, but with submissions that spend more time in kernel - regualr Queue
// can be as twice as fast as this one.
type ShardedQueue struct {
	shards    []int32
	byEventfd map[int32]*Queue
	poll      *poll
	rings     []*uring.Ring

	wg sync.WaitGroup
}

// NewSharded setups infra required for sharded queue (registers eventds, create epoll instance)
// and returns the pointer to the new instance of the sharded queue.
// TODO consider returning errors, instead of panicking
func NewSharded(rings ...*uring.Ring) *ShardedQueue {
	shards := make([]int32, len(rings))
	byEventfd := make(map[int32]*Queue, len(rings))
	pl, err := newPoll(len(rings))
	if err != nil {
		panic(err)
	}
	for i, ring := range rings {
		if err := ring.SetupEventfd(); err != nil {
			panic(err)
		}
		shards[i] = int32(ring.Eventfd())
		if err := pl.addRead(int32(ring.Eventfd())); err != nil {
			panic(err)
		}
		byEventfd[int32(ring.Eventfd())] = newQueue(ring)
	}
	q := &ShardedQueue{
		shards:    shards,
		byEventfd: byEventfd,
		poll:      pl,
		rings:     rings,
	}
	q.wg.Add(1)
	go q.completionLoop()
	return q
}

func (q *ShardedQueue) completionLoop() {
	defer q.wg.Done()
	for {
		exit := false
		if err := q.poll.wait(func(evt syscall.EpollEvent) {
			queue := q.byEventfd[evt.Fd]
			for i := uint32(0); i < evt.Events; i++ {
				if !queue.TryComplete() {
					exit = true
					return
				}
			}
		}); err != nil {
			panic(err)
		}
		if exit {
			return
		}
	}
}

func (q *ShardedQueue) getQueue() *Queue {
	tid := syscall.Gettid()
	shard := tid % len(q.shards)
	return q.byEventfd[q.shards[shard]]
}

// Complete waits for completion of the sqe with one of the shards.
func (q *ShardedQueue) Complete(sqe uring.SQEntry) (uring.CQEntry, error) {
	return q.getQueue().Complete(sqe)
}

// CompleteAsync returns future for waiting of the completion of the sqe with one of the shards.
func (q *ShardedQueue) CompleteAsync(sqe uring.SQEntry) (*request, error) {
	return q.getQueue().CompleteAsync(sqe)
}

// Close closes every shard queue, epoll instance and unregister eventfds.
func (q *ShardedQueue) Close() (err0 error) {
	// FIXME use multierr
	for _, queue := range q.byEventfd {
		if err := queue.Close(); err != nil && err0 == nil {
			err0 = err
		}
	}
	q.wg.Wait()
	if err := q.poll.close(); err != nil && err0 == nil {
		err0 = err
	}
	for _, ring := range q.rings {
		if err := ring.CloseEventfd(); err != nil && err0 == nil {
			err0 = err
		}
	}
	return err0
}
