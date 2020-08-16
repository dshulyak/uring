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
	max        int // max number of buffers
	bufferSize int // requested size of the buffer

	// mem is splitted in two parts
	// header - list of iovec structs.
	// starts at mem[0]. current length is iovecSz*allocated
	// buffers - list of buffers of the same size.
	mem []byte

	reg *queue.Queue
}

func (a *allocator) init() error {
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	flags := syscall.MAP_ANON | syscall.MAP_PRIVATE
	size := a.bufferSize * a.max
	mem, err := syscall.Mmap(-1, 0, size, prot, flags)
	if err != nil {
		return err
	}
	a.mem = mem
	iovec := syscall.Iovec{Base: &mem[0], Len: uint64(size)}
	return a.reg.RegisterBuffers(unsafe.Pointer(&iovec), 1)
}

func (a *allocator) close() error {
	return syscall.Munmap(a.mem)
}

func (a *allocator) bufAt(pos int) []byte {
	start := pos * a.bufferSize
	return a.mem[start : start+a.bufferSize]
}
