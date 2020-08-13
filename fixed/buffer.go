package fixed

// TODO provide standart buffers Read/Write API.
type Buffer struct {
	// if single iovec is splitted between several buffers - index and poolIndex will have
	// different values.
	// index refers to bufIndex in io_uring.
	// poolIndex refers to the index in local queue.
	index, poolIndex int
	buf              []byte
	Len              uint64
}

// Index return bufIndex in the io_uring instance.
func (b *Buffer) Index() uint16 {
	return uint16(b.index)
}

func (b *Buffer) Base() *byte {
	return &b.buf[0]
}

func (b *Buffer) Bytes() []byte {
	return b.buf
}
