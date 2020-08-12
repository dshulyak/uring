package fs

import (
	"io"
	"sync"
	"syscall"

	"github.com/dshulyak/uring"
	"github.com/dshulyak/uring/fixed"
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

func (f *File) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err = ioRst(f.queue.Complete(func(sqe *uring.SQEntry) {
		uring.Read(sqe, f.fd, b)
	}))
	if n < len(b) && err == nil {
		return n, io.EOF
	}
	return n, err
}

func (f *File) Write(b []byte) (n int, err error) {
	if len(b) == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return ioRst(f.queue.Complete(func(sqe *uring.SQEntry) {
		uring.Write(sqe, f.fd, b)
	}))
}

func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	if len(b) == 0 {
		return
	}
	vector := []syscall.Iovec{
		{
			Base: &b[0],
			Len:  uint64(len(b)),
		},
	}
	n, err = ioRst(f.queue.Complete(func(sqe *uring.SQEntry) {
		uring.Readv(sqe, f.fd, vector, uint64(off), 0)
	}))
	if n < len(b) && err == nil {
		return n, io.EOF
	}
	return n, err
}

func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	if len(b) == 0 {
		return
	}
	vector := []syscall.Iovec{
		{
			Base: &b[0],
			Len:  uint64(len(b)),
		},
	}
	n, err = ioRst(f.queue.Complete(func(sqe *uring.SQEntry) {
		uring.Writev(sqe, f.fd, vector, uint64(off), 0)
	}))
	return
}

func (f *File) WriteAtF(b *fixed.Buffer, off int64) (int, error) {
	lth := b.Len()
	if lth == 0 {
		return 0, nil
	}
	return ioRst(f.queue.Complete(func(sqe *uring.SQEntry) {
		uring.WriteFixed(sqe, f.fd, b.Base(), lth, uint64(off), 0, b.Index())
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
