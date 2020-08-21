package queue

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/dshulyak/uring"
)

const (
	// ShardingThreadID shares work based on thread id.
	ShardingThreadID uint = iota
	// ShardingRoundRobin shares shares work in round robin.
	ShardingRoundRobin
)

const (
	// WaitPoll monitors completion queue by polling (or IO_URING_ENTER with minComplete=0 in case of IOPOLL)
	WaitPoll uint = iota
	// WaitEnter monitors completion queue by waiting on IO_URING_ENTER with minComplete=1
	// Registering files and buffers requires uring to become idle, with WaitEnter we are
	// blocking until the next event is completed. Even if queue is empty this
	// makes uring think that it is not idle. As a consequence Registering files
	// or buffers leads to deadlock.
	WaitEnter
	// WaitEventfd wathches eventfd of each queue in the shard.
	WaitEventfd
)

const (
	// FlagSharedWorkers shares worker pool from the first ring instance between all shards in the queue.
	FlagSharedWorkers = 1 << iota
)

func defaultParams() *Params {
	return &Params{
		Shards:           uint(runtime.NumCPU()),
		ShardingStrategy: ShardingThreadID,
		WaitMethod:       WaitEventfd,
		Flags:            FlagSharedWorkers,
	}
}

// Params ...
type Params struct {
	Shards           uint
	ShardingStrategy uint
	WaitMethod       uint
	Flags            uint
}

// Queue ...
type Queue struct {
	qparams *Params
	// fields are used only if sharding is enabled.
	queues    []*queue
	n         uint64
	byEventfd map[int32]*queue
	poll      *poll
	// order is used only if queue operates in round robin mode
	order uint64

	// queue should be used if sharding is disabled.
	queue *queue

	wg sync.WaitGroup
}

// Setup setups requested number of shards, with shared kernel worker pool.
func Setup(size uint, params *uring.IOUringParams, qp *Params) (*Queue, error) {
	if qp == nil {
		qp = defaultParams()
	}
	if qp.Shards > 1 && !(qp.WaitMethod == WaitEventfd || qp.WaitMethod == WaitEnter) {
		return nil, errors.New("completions can be reaped only by waiting on eventfd if sharding is enabled")
	}
	q := &Queue{qparams: qp}
	if qp.Shards > 0 {
		return q, setupSharded(q, size, params)
	}
	return q, setupSimple(q, size, params)
}

func setupSimple(q *Queue, size uint, params *uring.IOUringParams) error {
	ring, err := uring.Setup(size, params)
	if err != nil {
		return err
	}
	q.queue = newQueue(ring, q.qparams)
	q.queue.startCompletionLoop()
	return nil
}

func setupSharded(q *Queue, size uint, params *uring.IOUringParams) (err error) {
	var (
		queues     = make([]*queue, q.qparams.Shards)
		paramsCopy uring.IOUringParams
	)

	q.poll, err = newPoll(len(queues))
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = q.poll.close()
		}
	}()
	if params != nil {
		paramsCopy = *params
	}
	var ring *uring.Ring
	defer func() {
		if err != nil {
			for _, subq := range queues {
				if subq != nil {
					_ = subq.Ring().Close()
				}
			}
		}
	}()
	for i := range queues {
		use := paramsCopy
		if q.qparams.Flags&FlagSharedWorkers > 0 && i > 0 {
			use.Flags |= uring.IORING_SETUP_ATTACH_WQ
			use.WQFd = uint32(queues[0].Ring().Fd())
		}
		ring, err = uring.Setup(size, &use)
		if err != nil {
			err = fmt.Errorf("failed to setup ring %w", err)
			return
		}
		queues[i] = newQueue(ring, q.qparams)
	}
	q.queues = queues
	q.n = uint64(q.qparams.Shards)

	if q.qparams.WaitMethod == WaitEventfd {
		byEventfd := make(map[int32]*queue, len(queues))
		for _, qu := range queues {
			ring := qu.Ring()
			for {
				err = ring.SetupEventfd()
				if err != nil {
					if err == syscall.EINTR {
						continue
					}
					err = fmt.Errorf("failed to setup eventfd %w", err)
					return
				}
				break
			}
			err = q.poll.addRead(int32(ring.Eventfd()))
			if err != nil {
				return
			}
			byEventfd[int32(ring.Eventfd())] = qu
		}
		q.byEventfd = byEventfd
		q.wg.Add(1)
		go q.epollLoop()
	} else {
		for _, qu := range queues {
			qu.startCompletionLoop()
		}
	}
	return
}

func (q *Queue) epollLoop() {
	defer q.wg.Done()
	var exit uint64
	for {
		if err := q.poll.wait(func(efd int32) {
			if !q.byEventfd[efd].tryComplete() {
				exit++
				return
			}
		}); err != nil {
			panic(err)
		}
		if exit == q.n {
			return
		}
	}
}

// getQueue returns queue for current thread.
func (q *Queue) getQueue() *queue {
	if q.queue != nil {
		return q.queue
	}
	// TODO get rid of this condition
	if len(q.queues) == 1 {
		return q.queues[0]
	}
	var tid uint64
	if q.qparams.ShardingStrategy == ShardingThreadID {
		tid = uint64(syscall.Gettid())
	} else if q.qparams.ShardingStrategy == ShardingRoundRobin {
		tid = atomic.AddUint64(&q.order, 1)
	} else {
		panic("sharded queue must use ShardingThreadID or ShardingRoundRobin")
	}
	return q.queues[tid%q.n]
}

//go:uintptrescapes

// Syscall is a helper to lock pointers that are sent to uring in place. Otherwise this is identical to Complete.
// WARNING: don't use interface that hides this method.
// https://github.com/golang/go/issues/16035#issuecomment-231107512.
func (q *Queue) Syscall(opts func(*uring.SQEntry), ptrs ...uintptr) (uring.CQEntry, error) {
	return q.Complete(opts)
}

// Complete waits for completion of the sqe with one of the shards. Complete is safe to uuse as is only if pointers that are used in SQEntry are not allocated on heap or there are not pointers, like in Close operation.
// If unsure always use Syscall.
func (q *Queue) Complete(opts func(*uring.SQEntry)) (uring.CQEntry, error) {
	return q.getQueue().Complete(opts)
}

// CompleteAsync returns future for waiting of the sqe completion with one of the shards.
func (q *Queue) CompleteAsync(opts func(*uring.SQEntry)) (*Result, error) {
	return q.getQueue().CompleteAsync(opts)
}

// tests for Register* methods are in fixed and fs modules.

// RegisterBuffers will register buffers on all rings (shards). Note that registration
// is done with syscall, and will have to wait until rings are idle.
// TODO test if IORING_OP_PROVIDE_BUFFERS is supported (5.7?)
func (q *Queue) RegisterBuffers(iovec []syscall.Iovec) (err error) {
	if q.queue != nil {
		return q.queue.Ring().RegisterBuffers(iovec)
	}
	for _, subq := range q.byEventfd {
		err = subq.Ring().RegisterBuffers(iovec)
		if err != nil {
			return
		}
	}
	return
}

// RegisterFiles ...
func (q *Queue) RegisterFiles(fds []int32) (err error) {
	if q.queue != nil {
		return q.queue.Ring().RegisterFiles(fds)
	}
	for _, subq := range q.byEventfd {
		err = subq.Ring().RegisterFiles(fds)
		if err != nil {
			return
		}
	}
	return
}

// UpdateFiles ...
func (q *Queue) UpdateFiles(fds []int32, off uint32) (err error) {
	if q.queue != nil {
		return q.queue.Ring().UpdateFiles(fds, off)
	}
	for _, subq := range q.byEventfd {
		err = subq.Ring().UpdateFiles(fds, off)
		if err != nil {
			return
		}
	}
	return
}

// UnregisterFiles ...
func (q *Queue) UnregisterFiles() (err error) {
	if q.queue != nil {
		return q.queue.Ring().UnregisterFiles()
	}
	for _, subq := range q.byEventfd {
		err = subq.Ring().UnregisterFiles()
		if err != nil {
			return
		}
	}
	return
}

// UnregisterBuffers ...
func (q *Queue) UnregisterBuffers() (err error) {
	if q.queue != nil {
		return q.queue.Ring().UnregisterBuffers()
	}
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
func (q *Queue) Close() (err0 error) {
	if q.queue != nil {
		err0 = q.queue.Close()
		if err := q.queue.Ring().Close(); err != nil && err0 == nil {
			err0 = err
		}
		return
	}
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
