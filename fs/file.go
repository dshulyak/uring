package fs

import (
	"syscall"

	"github.com/dshulyak/uring"
	"github.com/dshulyak/uring/queue"
)

type File struct {
	fd uintptr

	queue *queue.Queue
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

func (f *File) ReadAt(b []byte, off int64) (int, error) {
	vector := []syscall.Iovec{
		{
			Base: &b[0],
			Len:  uint64(len(b)),
		},
	}
	cqe, err := f.queue.Complete(func(sqe *uring.SQEntry) {
		uring.Readv(sqe, f.fd, vector, uint64(off), 0)
	})
	if err != nil {
		return 0, err
	}
	if cqe.Result() < 0 {
		return 0, syscall.Errno(-cqe.Result())
	}
	return int(cqe.Result()), nil
}

func (f *File) WriteAt(b []byte, off int64) (int, error) {
	vector := []syscall.Iovec{
		{
			Base: &b[0],
			Len:  uint64(len(b)),
		},
	}
	cqe, err := f.queue.Writev(f.fd, vector, uint64(off), 0)
	if err != nil {
		return 0, err
	}
	if cqe.Result() < 0 {
		return 0, syscall.Errno(-cqe.Result())
	}
	return int(cqe.Result()), nil
}
