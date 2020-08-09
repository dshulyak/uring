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

	wg sync.WaitGroup
}

// SetupSharded setups requested number of shards, with shared kernel worker pool.
func SetupSharded(shards, size uint, params *uring.IOUringParams) (*ShardedQueue, error) {
	var (
		queues     = make([]*Queue, shards)
		paramsCopy uring.IOUringParams
	)
	if params != nil {
		paramsCopy = *params
	}
	for i := range queues {
		use := paramsCopy
		if i > 0 {
			use.Flags |= uring.IORING_SETUP_ATTACH_WQ
			use.WQFd = uint32(queues[0].Ring().Fd())
		}
		ring, err := uring.Setup(size, &use)
		if err != nil {
			return nil, err
		}
		queues[i] = newQueue(ring)
	}
	return NewSharded(queues), nil
}

// NewSharded setups infra required for sharded queue (registers eventds, create epoll instance)
// and returns the pointer to the new instance of the sharded queue.
// TODO consider returning errors, instead of panicking
func NewSharded(queues []*Queue) *ShardedQueue {
	byEventfd := make(map[int32]*Queue, len(queues))
	pl, err := newPoll(len(queues))
	if err != nil {
		panic(err)
	}
	shards := make([]int32, len(queues))
	for i, qu := range queues {
		ring := qu.Ring()
		if err := ring.SetupEventfd(); err != nil {
			panic(err)
		}
		shards[i] = int32(ring.Eventfd())
		if err := pl.addRead(int32(ring.Eventfd())); err != nil {
			panic(err)
		}
		byEventfd[int32(ring.Eventfd())] = qu
	}
	q := &ShardedQueue{
		shards:    shards,
		byEventfd: byEventfd,
		poll:      pl,
	}
	q.wg.Add(1)
	go q.completionLoop()
	return q
}

func (q *ShardedQueue) completionLoop() {
	defer q.wg.Done()
	for {
		exit := false
		if err := q.poll.wait(func(efd int32) {
			if !q.byEventfd[efd].tryComplete() {
				exit = true
				return
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
	if len(q.shards) == 1 {
		return q.byEventfd[q.shards[0]]
	}
	tid := syscall.Gettid()
	shard := tid % len(q.shards)
	return q.byEventfd[q.shards[shard]]
}

// Complete waits for completion of the sqe with one of the shards.
func (q *ShardedQueue) Complete(f func(*uring.SQEntry)) (uring.CQEntry, error) {
	return q.getQueue().Complete(f)
}

// CompleteAsync returns future for waiting of the sqe completion with one of the shards.
func (q *ShardedQueue) CompleteAsync(f func(*uring.SQEntry)) (*Result, error) {
	return q.getQueue().CompleteAsync(f)
}

// Close closes every shard queue, epoll instance and unregister eventfds.
// Close works as follows:
// - request close on each queue
// - once any queue exits - completionLoop will be terminated
// - once completion loop terminated - unregister eventfd's and close rings
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
	for _, qu := range q.byEventfd {
		ring := qu.Ring()
		if err := ring.CloseEventfd(); err != nil && err0 == nil {
			err0 = err
		}
		if err := ring.Close(); err != nil && err0 == nil {
			err0 = err
		}
	}
	return err0
}
