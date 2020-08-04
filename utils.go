package uring

import (
	"syscall"
	"unsafe"
)

func Nop(sqe *SQEntry) {
	sqe.opcode = IORING_OP_NOP
}

func Writev(sqe *SQEntry, fd uintptr, iovec []syscall.Iovec, offset uint64, flags uint32) {
	sqe.opcode = IORING_OP_WRITEV
	sqe.fd = int32(fd)
	sqe.len = uint32(len(iovec))
	sqe.offset = offset
	sqe.opcodeFlags = flags
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(&iovec[0])))
}

func Readv(sqe *SQEntry, fd uintptr, iovec []syscall.Iovec, offset uint64, flags uint32) {
	sqe.opcode = IORING_OP_READV
	sqe.fd = int32(fd)
	sqe.len = uint32(len(iovec))
	sqe.offset = offset
	sqe.opcodeFlags = flags
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(&iovec[0])))
}

func Openat(sqe *SQEntry, dfd int32, pathptr *byte, flags uint32, mode uint32) {
	sqe.opcode = IORING_OP_OPENAT
	sqe.fd = dfd
	sqe.opcodeFlags = flags
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(pathptr)))
	sqe.len = mode
}

func Close(sqe *SQEntry, fd uintptr) {
	sqe.opcode = IORING_OP_CLOSE
	sqe.fd = int32(fd)
}
