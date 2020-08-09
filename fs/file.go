package fs

import (
	"sync"
	"syscall"

	"github.com/dshulyak/uring"
	"github.com/dshulyak/uring/queue"
)

func ioRst(cqe uring.CQEntry, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	if cqe.Result() < 0 {
		return 0, syscall.Errno(-cqe.Result())
	}
	return int(cqe.Result()), nil
}

type File struct {
	mu   sync.Mutex
	fd   uintptr
	name string

	queue *queue.ShardedQueue
}

func (f *File) Name() string {
	return f.name
}

func (f *File) Fd() uintptr {
	return f.fd
}

func (f *File) Close() error {
	cqe, err := f.queue.Complete(func(sqe *uring.SQEntry) {
		uring.Close(sqe, f.fd)
	})
	if err != nil {
		return err
	}
	if cqe.Result() < 0 {
		return syscall.Errno(-cqe.Result())
	}
	return nil
}

func (f *File) Read(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return ioRst(f.queue.Complete(func(sqe *uring.SQEntry) {
		uring.Read(sqe, f.fd, b)
	}))
}

func (f *File) Write(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return ioRst(f.queue.Complete(func(sqe *uring.SQEntry) {
		uring.Write(sqe, f.fd, b)
	}))
}

func (f *File) ReadAt(b []byte, off int64) (int, error) {
	vector := []syscall.Iovec{
		{
			Base: &b[0],
			Len:  uint64(len(b)),
		},
	}
	return ioRst(f.queue.Complete(func(sqe *uring.SQEntry) {
		uring.Readv(sqe, f.fd, vector, uint64(off), 0)
	}))
}

func (f *File) WriteAt(b []byte, off int64) (int, error) {
	vector := []syscall.Iovec{
		{
			Base: &b[0],
			Len:  uint64(len(b)),
		},
	}
	return ioRst(f.queue.Complete(func(sqe *uring.SQEntry) {
		uring.Writev(sqe, f.fd, vector, uint64(off), 0)
	}))
}

func (f *File) Sync() error {
	cqe, err := f.queue.Complete(func(sqe *uring.SQEntry) {
		uring.Fsync(sqe, f.fd)
	})
	if err != nil {
		return err
	}
	if cqe.Result() < 0 {
		return syscall.Errno(-cqe.Result())
	}
	return nil
}

func (f *File) Datasync() error {
	cqe, err := f.queue.Complete(func(sqe *uring.SQEntry) {
		uring.Fdatasync(sqe, f.fd)
	})
	if err != nil {
		return err
	}
	if cqe.Result() < 0 {
		return syscall.Errno(-cqe.Result())
	}
	return nil
}

func (f *File) Statx(flags, mask uint32) (sx uring.StatxS, err error) {
	_p0, err := syscall.BytePtrFromString(f.name)
	if err != nil {
		return sx, err
	}
	cqe, err := f.queue.Complete(func(sqe *uring.SQEntry) {
		uring.Statx(sqe, _AT_FDCWD, _p0, flags, mask, &sx)
	})
	if err != nil {
		return sx, err
	}
	if cqe.Result() < 0 {
		return sx, syscall.Errno(-cqe.Result())
	}
	return sx, nil
}
