package fs

import (
	"os"
	"syscall"
	"unsafe"

	"github.com/dshulyak/uring"
	"github.com/dshulyak/uring/queue"
	"golang.org/x/sys/unix"
)

const _AT_FDCWD int32 = -0x64

// FilesystemOption ...
type FilesystemOption func(*Filesystem)

// RegisterFiles enables file registration in uring when file is opened.
// n is a hint to for fds slice allocation. When fds slice needs to grow
// registration module will have to perform two syscalls (unregister files, register files).
func RegisterFiles(n int) FilesystemOption {
	return func(fsm *Filesystem) {
		fsm.fixedFiles = newFixedFiles(fsm.queue, n)
	}
}

// NewFilesystem returns facade for interacting with uring-based filesystem functionality.
func NewFilesystem(queue *queue.Queue, opts ...FilesystemOption) *Filesystem {
	fsm := &Filesystem{queue: queue}
	for _, opt := range opts {
		opt(fsm)
	}
	return fsm
}

// Filesystem is a facade for all fs-related functionality.
type Filesystem struct {
	queue *queue.Queue

	fixedFiles *fixedFiles
}

// Open a file.
func (fsm *Filesystem) Open(name string, flags int, mode os.FileMode) (*File, error) {
	_p0, err := syscall.BytePtrFromString(name)
	if err != nil {
		return nil, err
	}
	cqe, err := fsm.queue.Syscall(func(sqe *uring.SQEntry) {
		uring.Openat(sqe, _AT_FDCWD, _p0, uint32(flags), uint32(mode))
	}, uintptr(unsafe.Pointer(_p0)))

	if err != nil {
		return nil, err
	}
	if cqe.Result() < 0 {
		return nil, syscall.Errno(-cqe.Result())
	}

	fd := uintptr(cqe.Result())
	f := &File{
		fd:         fd,
		ufd:        fd,
		name:       name,
		queue:      fsm.queue,
		fixedFiles: fsm.fixedFiles,
	}
	if flags&os.O_SYNC > 0 || flags&unix.O_DSYNC > 0 {
		f.flags |= uring.IOSQE_ASYNC
	}
	if fsm.fixedFiles != nil {
		idx, err := fsm.fixedFiles.register(f.Fd())
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		f.ufd = idx
		f.flags |= uring.IOSQE_FIXED_FILE
	}
	return f, nil
}
