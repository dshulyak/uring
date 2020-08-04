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
	var sqe uring.SQEntry
	uring.Close(&sqe, f.fd)
	cqe, err := f.queue.Complete(sqe)
	if err != nil {
		return err
	}
	if cqe.Result() < 0 {
		return syscall.Errno(-cqe.Result())
	}
	return nil
}

func (f *File) ReadAt(b []byte, off int64) (int, error) {
	var sqe uring.SQEntry
	vector := []syscall.Iovec{
		{
			Base: &b[0],
			Len:  uint64(len(b)),
		},
	}
	uring.Readv(&sqe, f.fd, vector, uint64(off), 0)
	cqe, err := f.queue.Complete(sqe)
	if err != nil {
		return 0, err
	}
	if cqe.Result() < 0 {
		return 0, syscall.Errno(-cqe.Result())
	}
	return int(cqe.Result()), nil
}

func (f *File) WriteAt(b []byte, off int64) (int, error) {
	var sqe uring.SQEntry
	vector := []syscall.Iovec{
		{
			Base: &b[0],
			Len:  uint64(len(b)),
		},
	}
	uring.Writev(&sqe, f.fd, vector, uint64(off), 0)
	cqe, err := f.queue.Complete(sqe)
	if err != nil {
		return 0, err
	}
	if cqe.Result() < 0 {
		return 0, syscall.Errno(-cqe.Result())
	}
	return int(cqe.Result()), nil
}
