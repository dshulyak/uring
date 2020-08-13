package fixed

import (
	"sync"

	"github.com/dshulyak/uring/queue"
)

// New will initialize mmap'ed memory region, of the total size 16 bytes + size*bufsize
// and register mmap'ed memory as buffer in io_uring.
func New(queue *queue.ShardedQueue, bufsize, size int) (*Pool, error) {
	alloc := &allocator{
		max:        size,
		bufferSize: bufsize,
		queue:      queue,
	}
	if err := alloc.init(); err != nil {
		return nil, err
	}

	var mu sync.Mutex
	return &Pool{
		cond:    sync.NewCond(&mu),
		alloc:   alloc,
		buffers: make([]int, 0, size),
	}, nil
}

// Pool manages registered offheap buffers. Allocated with MAP_ANON.
// TODO this has terrible performance. either shard or use lock free queue to reduce locking.
type Pool struct {
	cond    *sync.Cond
	alloc   *allocator
	buffers []int
}

// Get buffer.
func (p *Pool) Get() *Buffer {
	p.cond.L.Lock()
	if p.buffers == nil {
		return nil
	}
	defer p.cond.L.Unlock()
	if len(p.buffers) == 0 {
		next, ok := p.alloc.next()
		if !ok {
			p.cond.Wait()
			if p.buffers == nil {
				return nil
			}
		} else {
			buf := p.alloc.bufAt(next)
			return &Buffer{buf: buf, poolIndex: next, Len: uint64(len(buf))}
		}
	}
	idx := p.buffers[0]
	buf := p.alloc.bufAt(idx)
	copy(p.buffers, p.buffers[1:])
	p.buffers = p.buffers[:len(p.buffers)-1]
	return &Buffer{buf: buf, poolIndex: idx, Len: uint64(len(buf))}
}

// Put buffer into the pool. Note that if caller won't put used buffer's into the pool
// Get operation will block indefinitely.
func (p *Pool) Put(b *Buffer) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	if p.buffers == nil {
		return
	}
	empty := len(p.buffers) == 0
	p.buffers = append(p.buffers, b.poolIndex)
	if empty {
		p.cond.Signal()
	}
}

// Close prefents future Get's from the pool and munmap's allocated memory.
func (p *Pool) Close() error {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	p.cond.Broadcast()
	p.buffers = nil
	return p.alloc.close()
}
