package fixed

import (
	"errors"
	"syscall"
	"unsafe"

	"github.com/dshulyak/uring/queue"
)

var (
	// ErrOverflow returned if requested buffer number larget then max number.
	ErrOverflow = errors.New("buffer number overflow")
)

var iovecSize = int(unsafe.Sizeof(syscall.Iovec{}))

type allocator struct {
	allocated  int // number of currently registered buffers
	max        int // max number of buffers
	bufferSize int // requested size of the buffer

	buffers int // start of the buffers region in the allocated mem

	// mem is splitted in two parts
	// header - list of iovec structs.
	// starts at mem[0]. current length is iovecSz*allocated
	// buffers - list of buffers of the same size.
	mem []byte

	queue *queue.ShardedQueue
}

func (a *allocator) init() error {
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	flags := syscall.MAP_ANON | syscall.MAP_PRIVATE
	header := iovecSize
	size := a.bufferSize*a.max + header
	mem, err := syscall.Mmap(-1, 0, size, prot, flags)
	if err != nil {
		return err
	}
	a.buffers = header
	a.mem = mem
	iovec := (*syscall.Iovec)(unsafe.Pointer(&a.mem[0]))
	iovec.Base = &mem[header]
	iovec.Len = uint64(size - header)
	return a.queue.RegisterBuffers(unsafe.Pointer(&a.mem[0]), 1)
}

func (a *allocator) close() error {
	return syscall.Munmap(a.mem)
}

func (a *allocator) next() (int, bool) {
	if a.allocated == a.max {
		return -1, false
	}
	return a.allocated, true
}

func (a *allocator) bufAt(pos int) []byte {
	if pos > a.max {
		return nil
	}
	start := a.buffers + pos*a.bufferSize
	buf := a.mem[start : start+a.bufferSize]
	if pos < a.allocated {
		return buf
	}
	a.allocated++
	return buf
}
