package fs

import (
	"os"
	"sync"

	"github.com/dshulyak/uring/queue"
)

const _AT_FDCWD int32 = -0x64

func NewFilesystem(queue *queue.ShardedQueue) *Filesystem {
	return &Filesystem{
		queue: queue,
	}
}

// Filesystem is a facade for all fs-related functionality.
type Filesystem struct {
	queue *queue.ShardedQueue

	mu  sync.Mutex
	fds []int32
}

func (fs *Filesystem) Open(name string, flags int, mode os.FileMode) (*File, error) {
	f, err := os.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	// using FILES_UPDATE operation
	// on close set fd to -1 and replace it with newly opened file
	fs.fds = append(fs.fds, int32(f.Fd()))
	if err := fs.queue.RegisterFiles(fs.fds); err != nil {
		f.Close()
		return nil, err
	}
	return &File{
		f:      f,
		fd:     f.Fd(),
		queue:  fs.queue,
		name:   name,
		regIdx: uintptr(len(fs.fds) - 1),
	}, nil
}
