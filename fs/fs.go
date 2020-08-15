package fs

import (
	"os"

	"github.com/dshulyak/uring"
	"github.com/dshulyak/uring/queue"
)

const _AT_FDCWD int32 = -0x64

func NewFilesystem(queue *queue.ShardedQueue) *Filesystem {
	return &Filesystem{
		queue:      queue,
		fixedFiles: newFixedFiles(queue, 32),
	}
}

// Filesystem is a facade for all fs-related functionality.
type Filesystem struct {
	queue *queue.ShardedQueue

	fixedFiles *fixedFiles
}

func (fs *Filesystem) Open(name string, flags int, mode os.FileMode) (*File, error) {
	f, err := os.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}

	idx, err := fs.fixedFiles.register(f.Fd())
	if err != nil {
		f.Close()
		return nil, err
	}
	return &File{
		f:          f,
		fd:         f.Fd(),
		queue:      fs.queue,
		name:       name,
		ufd:        idx,
		flags:      uring.IOSQE_FIXED_FILE,
		fixedFiles: fs.fixedFiles,
	}, nil
}
