package fs

import (
	"os"
	"syscall"

	"github.com/dshulyak/uring"
	"github.com/dshulyak/uring/queue"
)

const _AT_FDCWD int32 = -0x64

func NewFilesystem(queue *queue.Queue, ring *uring.Ring) *Filesystem {
	return &Filesystem{
		queue: queue,
		ring:  ring,
	}
}

// Filesystem is a facade for all fs-related functionality.
type Filesystem struct {
	queue *queue.Queue
	ring  *uring.Ring
}

func (fs *Filesystem) Open(name string, flags int, mode os.FileMode) (*File, error) {
	_p0, err := syscall.BytePtrFromString(name)
	if err != nil {
		return nil, err
	}
	cqe, err := fs.queue.Complete(func(sqe *uring.SQEntry) {
		uring.Openat(sqe, _AT_FDCWD, _p0, uint32(flags), uint32(mode))
	})
	if err != nil {
		return nil, err
	}
	if cqe.Result() < 0 {
		return nil, syscall.Errno(-cqe.Result())
	}
	return &File{fd: uintptr(cqe.Result()), queue: fs.queue}, nil
}
