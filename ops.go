package uring

import (
	"syscall"
	"unsafe"
)

// Nop ...
func Nop(sqe *SQEntry) {
	sqe.opcode = IORING_OP_NOP
}

// Write ...
func Write(sqe *SQEntry, fd uintptr, buf []byte) {
	sqe.opcode = IORING_OP_WRITE
	sqe.fd = int32(fd)
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(&buf[0])))
	sqe.len = uint32(len(buf))
}

// Read ...
func Read(sqe *SQEntry, fd uintptr, buf []byte) {
	sqe.opcode = IORING_OP_READ
	sqe.fd = int32(fd)
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(&buf[0])))
	sqe.len = uint32(len(buf))
}

// Writev ...
func Writev(sqe *SQEntry, fd uintptr, iovec []syscall.Iovec, offset uint64, flags uint32) {
	sqe.opcode = IORING_OP_WRITEV
	sqe.fd = int32(fd)
	sqe.len = uint32(len(iovec))
	sqe.offset = offset
	sqe.opcodeFlags = flags
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(&iovec[0])))
}

// Readv
func Readv(sqe *SQEntry, fd uintptr, iovec []syscall.Iovec, offset uint64, flags uint32) {
	sqe.opcode = IORING_OP_READV
	sqe.fd = int32(fd)
	sqe.len = uint32(len(iovec))
	sqe.offset = offset
	sqe.opcodeFlags = flags
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(&iovec[0])))
}

// WriteFixed ...
func WriteFixed(sqe *SQEntry, fd uintptr, base *byte, len, offset uint64, flags uint32, bufIndex uint16) {
	sqe.opcode = IORING_OP_WRITE_FIXED
	sqe.fd = int32(fd)
	sqe.len = uint32(len)
	sqe.offset = offset
	sqe.opcodeFlags = flags
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(base)))
	sqe.SetBufIndex(bufIndex)
}

// ReadFixed ...
func ReadFixed(sqe *SQEntry, fd uintptr, base *byte, len, offset uint64, flags uint32, bufIndex uint16) {
	sqe.opcode = IORING_OP_READ_FIXED
	sqe.fd = int32(fd)
	sqe.len = uint32(len)
	sqe.offset = offset
	sqe.opcodeFlags = flags
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(base)))
	sqe.SetBufIndex(bufIndex)
}

// Fsync ...
func Fsync(sqe *SQEntry, fd uintptr) {
	sqe.opcode = IORING_OP_FSYNC
	sqe.fd = int32(fd)
}

// Fdatasync ...
func Fdatasync(sqe *SQEntry, fd uintptr) {
	sqe.opcode = IORING_OP_FSYNC
	sqe.fd = int32(fd)
	sqe.opcodeFlags = IORING_FSYNC_DATASYNC
}

// Openat
func Openat(sqe *SQEntry, dfd int32, pathptr *byte, flags uint32, mode uint32) {
	sqe.opcode = IORING_OP_OPENAT
	sqe.fd = dfd
	sqe.opcodeFlags = flags
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(pathptr)))
	sqe.len = mode
}

// Close ...
func Close(sqe *SQEntry, fd uintptr) {
	sqe.opcode = IORING_OP_CLOSE
	sqe.fd = int32(fd)
}

// Send ...
func Send(sqe *SQEntry, fd uintptr, buf []byte, flags uint32) {
	sqe.SetOpcode(IORING_OP_SEND)
	sqe.SetFD(int32(fd))
	sqe.SetAddr((uint64)(uintptr(unsafe.Pointer(&buf[0]))))
	sqe.SetLen(uint32(len(buf)))
	sqe.SetOpcodeFlags(flags)
}

// Recv ...
func Recv(sqe *SQEntry, fd uintptr, buf []byte, flags uint32) {
	sqe.SetOpcode(IORING_OP_RECV)
	sqe.SetFD(int32(fd))
	sqe.SetAddr((uint64)(uintptr(unsafe.Pointer(&buf[0]))))
	sqe.SetLen(uint32(len(buf)))
	sqe.SetOpcodeFlags(flags)
}
