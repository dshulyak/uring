package loop

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"syscall"

	"github.com/dshulyak/uring"
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
		Rings:      runtime.NumCPU(),
		WaitMethod: WaitEventfd,
		Flags:      FlagSharedWorkers,
	}
}

// Params ...
type Params struct {
	Rings      int
	WaitMethod uint
	Flags      uint
}

// Loop ...
type Loop struct {
	qparams *Params
	// fields are used only if sharding is enabled.
	queues    []*queue
	n         uint64
	byEventfd map[int32]*queue
	poll      *poll

	wg sync.WaitGroup
}

// Setup setups requested number of shards, with shared kernel worker pool.
func Setup(size uint, params *uring.IOUringParams, qp *Params) (*Loop, error) {
	if qp == nil {
		qp = defaultParams()
	}
	if qp.Rings > 1 && !(qp.WaitMethod == WaitEventfd || qp.WaitMethod == WaitEnter) {
		return nil, errors.New("completions can be reaped only by waiting on eventfd if sharding is enabled")
	}
	q := &Loop{qparams: qp}
	if qp.Rings > 0 {
		return q, setupSharded(q, size, params)
	}
	return q, setupSimple(q, size, params)
}

func setupSimple(q *Loop, size uint, params *uring.IOUringParams) error {
	ring, err := uring.Setup(size, params)
	if err != nil {
		return err
	}
	q.queues = []*queue{newQueue(ring, q.qparams)}
	q.queues[0].startCompletionLoop()
	return nil
}

func setupSharded(q *Loop, size uint, params *uring.IOUringParams) (err error) {
	var (
		queues     = make([]*queue, q.qparams.Rings)
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
	q.n = uint64(q.qparams.Rings)

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

func (q *Loop) epollLoop() {
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

// getLoop returns queue for current thread.
func (q *Loop) getQueue() *queue {
	if len(q.queues) == 1 {
		return q.queues[0]
	}
	tid := uint64(syscall.Gettid())
	return q.queues[tid%q.n]
}

//go:uintptrescapes

// Syscall executes operation on one of the internal queues. Additionaly it prevents ptrs from being moved to another location while Syscall is in progress.
// WARNING: don't use interface that hides this method.
// https://github.com/golang/go/issues/16035#issuecomment-231107512.
func (q *Loop) Syscall(opt SQOperation, ptrs ...uintptr) (uring.CQEntry, error) {
	return q.getQueue().Complete(opt)
}

//go:uintptrescapes

// BatchSyscall ...
func (q *Loop) BatchSyscall(cqes []uring.CQEntry, opts []SQOperation, ptrs ...uintptr) ([]uring.CQEntry, error) {
	return q.getQueue().Batch(cqes, opts)
}

// tests for Register* methods are in fixed and fs modules.

// RegisterBuffers will register buffers on all rings (shards). Note that registration
// is done with syscall, and will have to wait until rings are idle.
// TODO test if IORING_OP_PROVIDE_BUFFERS is supported (5.7?)
func (q *Loop) RegisterBuffers(iovec []syscall.Iovec) (err error) {
	for _, subq := range q.queues {
		err = subq.Ring().RegisterBuffers(iovec)
		if err != nil {
			return
		}
	}
	return
}

// RegisterFiles ...
func (q *Loop) RegisterFiles(fds []int32) (err error) {
	for _, subq := range q.queues {
		err = subq.Ring().RegisterFiles(fds)
		if err != nil {
			return
		}
	}
	return
}

// UpdateFiles ...
func (q *Loop) UpdateFiles(fds []int32, off uint32) (err error) {
	for _, subq := range q.queues {
		err = subq.Ring().UpdateFiles(fds, off)
		if err != nil {
			return
		}
	}
	return
}

// UnregisterFiles ...
func (q *Loop) UnregisterFiles() (err error) {
	for _, subq := range q.queues {
		err = subq.Ring().UnregisterFiles()
		if err != nil {
			return
		}
	}
	return
}

// UnregisterBuffers ...
func (q *Loop) UnregisterBuffers() (err error) {
	for _, qu := range q.queues {
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
func (q *Loop) Close() (err0 error) {
	// FIXME use multierr
	for _, queue := range q.queues {
		if err := queue.Close(); err != nil && err0 == nil {
			err0 = err
		}
	}
	q.wg.Wait()
	if q.poll != nil {
		if err := q.poll.close(); err != nil && err0 == nil {
			err0 = err
		}
	}
	for _, qu := range q.queues {
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
