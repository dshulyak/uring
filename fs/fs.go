package fs

import (
	"os"

	"github.com/dshulyak/uring"
	"github.com/dshulyak/uring/queue"
)

const _AT_FDCWD int32 = -0x64

type FilesystemOption func(*Filesystem)

func RegisterFiles(n int) FilesystemOption {
	return func(fsm *Filesystem) {
		fsm.fixedFiles = newFixedFiles(fsm.queue, n)
	}
}

func NewFilesystem(queue *queue.ShardedQueue, opts ...FilesystemOption) *Filesystem {
	fsm := &Filesystem{queue: queue}
	for _, opt := range opts {
		opt(fsm)
	}
	return fsm
}

// Filesystem is a facade for all fs-related functionality.
type Filesystem struct {
	queue *queue.ShardedQueue

	fixedFiles *fixedFiles
}

func (fsm *Filesystem) Open(name string, flags int, mode os.FileMode) (*File, error) {
	f, err := os.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}

	uf := &File{
		f:          f,
		fd:         f.Fd(),
		ufd:        f.Fd(),
		name:       name,
		queue:      fsm.queue,
		fixedFiles: fsm.fixedFiles,
	}
	if fsm.fixedFiles != nil {
		idx, err := fsm.fixedFiles.register(f.Fd())
		if err != nil {
			f.Close()
			return nil, err
		}
		uf.ufd = idx
		uf.flags = uring.IOSQE_FIXED_FILE
	}
	return uf, nil
}
