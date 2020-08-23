package fs

import (
	"context"
	"sync"
	"syscall"
	"unsafe"

	"github.com/dshulyak/uring"
	"github.com/dshulyak/uring/fixed"
	"github.com/dshulyak/uring/loop"
	"golang.org/x/sys/unix"
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

// File ...
type File struct {
	mu   sync.Mutex
	fd   uintptr
	name string
	// ufd is used for uring operations.
	// will be equal to fd is fd is not registered, otherwise will be an index in the array
	// with all registered fds
	ufd uintptr
	// additional sqe flags
	flags uint8

	lp         *loop.Loop
	fixedFiles *fixedFiles
}

// Name ...
func (f *File) Name() string {
	return f.name
}

// Fd ...
func (f *File) Fd() uintptr {
	return f.fd
}

// Close ...
func (f *File) Close() error {
	if f.fixedFiles != nil {
		_ = f.fixedFiles.unregister(f.ufd)
	}
	cqe, err := f.lp.Syscall(func(sqe *uring.SQEntry) {
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

// WriteAt ...
func (f *File) WriteAt(buf []byte, off int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	iovec := []syscall.Iovec{{Base: &buf[0], Len: uint64(len(buf))}}
	return ioRst(f.lp.Syscall(func(sqe *uring.SQEntry) {
		uring.Writev(sqe, f.ufd, iovec, uint64(off), 0)
		sqe.SetFlags(f.flags)
	}, uintptr(unsafe.Pointer(&iovec[0]))))
}

// ReadAt ...
func (f *File) ReadAt(buf []byte, off int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	iovec := []syscall.Iovec{{Base: &buf[0], Len: uint64(len(buf))}}
	return ioRst(f.lp.Syscall(func(sqe *uring.SQEntry) {
		uring.Readv(sqe, f.ufd, iovec, uint64(off), 0)
		sqe.SetFlags(f.flags)
	}, uintptr(unsafe.Pointer(&iovec[0]))))
}

// WriteAtContext will cancel write operation if it wasn't started until deadline or timeout in context.
func (f *File) WriteAtContext(ctx context.Context, buf []byte, off int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return f.WriteAt(buf, off)
	}
	timeout := unix.Timespec{Nsec: int64(deadline.Nanosecond())}
	iovec := []syscall.Iovec{{Base: &buf[0], Len: uint64(len(buf))}}
	cqes, err := f.lp.BatchSyscall(nil, []loop.SQOperation{
		func(sqe *uring.SQEntry) {
			uring.Writev(sqe, f.ufd, iovec, uint64(off), 0)
			sqe.SetFlags(uring.IOSQE_IO_LINK)
		},
		func(sqe *uring.SQEntry) {
			uring.LinkTimeout(sqe, &timeout, true)
		},
	},
		uintptr(unsafe.Pointer(&iovec[0])), uintptr(unsafe.Pointer(&timeout)),
	)
	return ioRst(cqes[0], err)
}

// ReadAtContext will cancel read operation if it wasn't started until deadline or timeout in context.
func (f *File) ReadAtContext(ctx context.Context, buf []byte, off int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return f.ReadAt(buf, off)
	}
	timeout := unix.Timespec{Nsec: int64(deadline.Nanosecond())}
	iovec := []syscall.Iovec{{Base: &buf[0], Len: uint64(len(buf))}}
	cqes, err := f.lp.BatchSyscall(nil, []loop.SQOperation{
		func(sqe *uring.SQEntry) {
			uring.Readv(sqe, f.ufd, iovec, uint64(off), 0)
			sqe.SetFlags(uring.IOSQE_IO_LINK)
		},
		func(sqe *uring.SQEntry) {
			uring.LinkTimeout(sqe, &timeout, true)
		},
	},
		uintptr(unsafe.Pointer(&iovec[0])), uintptr(unsafe.Pointer(&timeout)),
	)
	return ioRst(cqes[0], err)
}

// WriteAtFixed ...
func (f *File) WriteAtFixed(b *fixed.Buffer, off int64) (int, error) {
	if b.Len() == 0 {
		return 0, nil
	}
	return ioRst(f.lp.Syscall(func(sqe *uring.SQEntry) {
		uring.WriteFixed(sqe, f.ufd, b.Base(), b.Len(), uint64(off), 0, b.Index())
		sqe.SetFlags(f.flags)
	}))
}

// ReadAtFixed ...
func (f *File) ReadAtFixed(b *fixed.Buffer, off int64) (int, error) {
	if b.Len() == 0 {
		return 0, nil
	}
	return ioRst(f.lp.Syscall(func(sqe *uring.SQEntry) {
		uring.ReadFixed(sqe, f.ufd, b.Base(), b.Len(), uint64(off), 0, b.Index())
		sqe.SetFlags(f.flags)
	}))

}

// Sync ...
func (f *File) Sync() error {
	cqe, err := f.lp.Syscall(func(sqe *uring.SQEntry) {
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

// Datasync ...
func (f *File) Datasync() error {
	cqe, err := f.lp.Syscall(func(sqe *uring.SQEntry) {
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
