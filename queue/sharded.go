package queue

import (
	"sync"
	"syscall"
	"unsafe"

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
		for {
			if err := ring.SetupEventfd(); err != nil {
				if err == syscall.EINTR {
					continue
				}
				panic(err)
			}
			break
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

// getQueue returns queue for current thread.
func (q *ShardedQueue) getQueue() *Queue {
	if len(q.shards) == 1 {
		return q.byEventfd[q.shards[0]]
	}
	tid := syscall.Gettid()
	shard := tid % len(q.shards)
	return q.byEventfd[q.shards[shard]]
}

//go:uintptrescapes

// Syscall ...
// Do not hide this call behind interface.
// https://github.com/golang/go/issues/16035#issuecomment-231107512.
func (q *ShardedQueue) Syscall(opts func(*uring.SQEntry), ptrs ...uintptr) (uring.CQEntry, error) {
	return q.getQueue().Syscall(opts, ptrs...)
}

// Complete waits for completion of the sqe with one of the shards.
func (q *ShardedQueue) Complete(f func(*uring.SQEntry)) (uring.CQEntry, error) {
	return q.getQueue().Complete(f)
}

// CompleteAsync returns future for waiting of the sqe completion with one of the shards.
func (q *ShardedQueue) CompleteAsync(f func(*uring.SQEntry)) (*Result, error) {
	return q.getQueue().CompleteAsync(f)
}

// CompleteAll completes request on each queue. Usefull for registers and tests.
func (q *ShardedQueue) CompleteAll(f func(*uring.SQEntry), c func(uring.CQEntry)) error {
	results := make([]*Result, 0, len(q.byEventfd))
	for _, qu := range q.byEventfd {
		result, err := qu.CompleteAsync(f)
		if err != nil {
			return err
		}
		results = append(results, result)
	}
	for _, result := range results {
		_, ok := <-result.Wait()
		if !ok {
			return ErrClosed
		}
		cqe := result.CQEntry
		result.Dispose()
		c(cqe)
	}
	return nil
}

// RegisterBuffers will register buffers on all rings (shards). Note that registration
// is done with syscall, and will have to wait until rings are idle.
// TODO test if IORING_OP_PROVIDE_BUFFERS is supported (5.7?)
func (q *ShardedQueue) RegisterBuffers(ptr unsafe.Pointer, len uint64) (err error) {
	for _, subq := range q.byEventfd {
		err = subq.Ring().RegisterBuffers(ptr, len)
		if err != nil {
			return
		}
	}
	return
}

// RegisterFiles ...
func (q *ShardedQueue) RegisterFiles(fds []int32) (err error) {
	for _, subq := range q.byEventfd {
		err = subq.Ring().RegisterFiles(fds)
		if err != nil {
			return
		}
	}
	return
}

// UpdateFiles ...
func (q *ShardedQueue) UpdateFiles(fds []int32, off uint32) (err error) {
	for _, subq := range q.byEventfd {
		err = subq.Ring().UpdateFiles(fds, off)
		if err != nil {
			return
		}
	}
	return
}

// UnregisterFiles ...
func (q *ShardedQueue) UnregisterFiles() (err error) {
	for _, subq := range q.byEventfd {
		err = subq.Ring().UnregisterFiles()
		if err != nil {
			return
		}
	}
	return
}

// UnregisterBuffers ...
func (q *ShardedQueue) UnregisterBuffers() (err error) {
	for _, qu := range q.byEventfd {
		if err := qu.Ring().UnregisterBuffers(); err != nil {
			return err
		}
	}
	return nil
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
