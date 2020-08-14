package fixed

// Buffer ...
type Buffer struct {
	// if single iovec is splitted between several buffers - index and poolIndex will have
	// different values.
	// index refers to bufIndex in io_uring.
	// poolIndex refers to the index in local queue.
	index, poolIndex int
	B                []byte
}

// Index returns bufIndex that must be used for fixed write io_uring operations.
func (b *Buffer) Index() uint16 {
	return uint16(b.index)
}

// Len ...
func (b *Buffer) Len() uint64 {
	return uint64(len(b.B))
}

// Base returns pointer to the first byte of the buffer.
func (b *Buffer) Base() *byte {
	return &b.B[0]
}

// Bytes ...
func (b *Buffer) Bytes() []byte {
	return b.B
}
