package fixed

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/dshulyak/uring/queue"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &Buffer{}
	},
}

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
	var (
		head *node
	)
	for i := size - 1; i >= 0; i-- {
		head = &node{
			next:  head,
			index: i,
		}
	}
	return &Pool{
		alloc: alloc,
		head:  head,
	}, nil
}

// Pool manages registered offheap buffers. Allocated with MAP_ANON.
// TODO performance is not really excellent, several ideas to try:
// - backoff on contention (note that runtime.Gosched achieves same purpose)
// - elimitation array
// This is still better than mutex-based version (3.5 times faster), but much more worse
// than simple sync.Pool (15 times slower).
type Pool struct {
	alloc *allocator
	head  *node
}

// Get buffer.
func (p *Pool) Get() *Buffer {
	for {
		old := (*node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&p.head))))
		if old != nil {
			index := old.index
			next := old.next
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&p.head)), unsafe.Pointer(old), unsafe.Pointer(next)) {
				buf := bufferPool.Get().(*Buffer)
				buf.B = p.alloc.bufAt(index)
				buf.poolIndex = index
				buf.index = 0 // placeholder, until i will have more pages
				return buf
			}
		}
		runtime.Gosched()
	}
}

// Put buffer into the pool. Note that if caller won't put used buffer's into the pool
// Get operation will block indefinitely.
func (p *Pool) Put(b *Buffer) {
	next := &node{index: b.poolIndex}
	for {
		head := (*node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&p.head))))
		next.next = head
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&p.head)), unsafe.Pointer(head), unsafe.Pointer(next)) {
			bufferPool.Put(b)
			return
		}
		runtime.Gosched()
	}
}

// Close prefents future Get's from the pool and munmap's allocated memory.
// Caller must ensure that all users of the pool exited before calling Close.
// Otherwise program will crash referencing to an invalid memory region.
func (p *Pool) Close() error {
	return p.alloc.close()
}

type node struct {
	next  *node
	index int
}
