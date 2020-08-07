package uring

import (
	"syscall"
	"unsafe"
)

func Nop(sqe *SQEntry) {
	sqe.opcode = IORING_OP_NOP
}

func Write(sqe *SQEntry, fd uintptr, buf []byte) {
	sqe.opcode = IORING_OP_WRITE
	sqe.fd = int32(fd)
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(&buf[0])))
	sqe.len = uint32(len(buf))
}

func Read(sqe *SQEntry, fd uintptr, buf []byte) {
	sqe.opcode = IORING_OP_READ
	sqe.fd = int32(fd)
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(&buf[0])))
	sqe.len = uint32(len(buf))
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

func WriteFixed(sqe *SQEntry, fd uintptr, iovec syscall.Iovec, offset uint64, flags uint32, bufIndex uint16) {
	sqe.opcode = IORING_OP_WRITE_FIXED
	sqe.fd = int32(fd)
	sqe.len = uint32(iovec.Len)
	sqe.offset = offset
	sqe.opcodeFlags = flags
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(iovec.Base)))
	sqe.SetBufIndex(bufIndex)
}

func ReadFixed(sqe *SQEntry, fd uintptr, iovec syscall.Iovec, offset uint64, flags uint32, bufIndex uint16) {
	sqe.opcode = IORING_OP_READ_FIXED
	sqe.fd = int32(fd)
	sqe.len = uint32(iovec.Len)
	sqe.offset = offset
	sqe.opcodeFlags = flags
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(iovec.Base)))
	sqe.SetBufIndex(bufIndex)
}

func Fsync(sqe *SQEntry, fd uintptr) {
	sqe.opcode = IORING_OP_FSYNC
	sqe.fd = int32(fd)
}

func Fdatasync(sqe *SQEntry, fd uintptr) {
	sqe.opcode = IORING_OP_FSYNC
	sqe.fd = int32(fd)
	sqe.opcodeFlags = IORING_FSYNC_DATASYNC
}

func Openat(sqe *SQEntry, dfd int32, pathptr *byte, flags uint32, mode uint32) {
	sqe.opcode = IORING_OP_OPENAT
	sqe.fd = dfd
	sqe.opcodeFlags = flags
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(pathptr)))
	sqe.len = mode
}

func Statx(sqe *SQEntry, dfd int32, pathptr *byte, flags uint32, mask uint32, statx *StatxS) {
	sqe.opcode = IORING_OP_STATX
	sqe.fd = dfd
	sqe.opcodeFlags = flags
	sqe.addr = (uint64)(uintptr(unsafe.Pointer(pathptr)))
	sqe.len = mask
	sqe.offset = (uint64)(uintptr(unsafe.Pointer(statx)))
}

func Close(sqe *SQEntry, fd uintptr) {
	sqe.opcode = IORING_OP_CLOSE
	sqe.fd = int32(fd)
}
