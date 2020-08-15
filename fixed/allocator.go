package fixed

import (
	"errors"
	"syscall"
	"unsafe"
)

var (
	// ErrOverflow returned if requested buffer number larget then max number.
	ErrOverflow = errors.New("buffer number overflow")
)

// Registry ...
type Registry interface {
	RegisterBuffers(unsafe.Pointer, uint64) error
}

var iovecSize = int(unsafe.Sizeof(syscall.Iovec{}))

type allocator struct {
	max        int // max number of buffers
	bufferSize int // requested size of the buffer

	buffers int // start of the buffers region in the allocated mem

	// mem is splitted in two parts
	// header - list of iovec structs.
	// starts at mem[0]. current length is iovecSz*allocated
	// buffers - list of buffers of the same size.
	mem []byte

	reg Registry
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
	return a.reg.RegisterBuffers(unsafe.Pointer(&a.mem[0]), 1)
}

func (a *allocator) close() error {
	return syscall.Munmap(a.mem)
}

func (a *allocator) bufAt(pos int) []byte {
	start := a.buffers + pos*a.bufferSize
	buf := a.mem[start : start+a.bufferSize]
	return buf
}
