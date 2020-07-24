package uring

import "unsafe"

// syscalls
const (
	IO_URING_SETUP uintptr = 425 + iota
	IO_URING_ENTER
	IO_URING_REGISTER
)

// operations
const (
	IORING_OP_NOP uint8 = iota
	IORING_OP_READV
	IORING_OP_WRITEV
	IORING_OP_FSYNC
	IORING_OP_READ_FIXED
	IORING_OP_WRITE_FIXED
	IORING_OP_POLL_ADD
	IORING_OP_POLL_REMOVE
	IORING_OP_SYNC_FILE_RANGE
	IORING_OP_SENDMSG
	IORING_OP_RECVMSG
	IORING_OP_TIMEOUT
	IORING_OP_TIMEOUT_REMOVE
	IORING_OP_ACCEPT
	IORING_OP_ASYNC_CANCEL
	IORING_OP_LINK_TIMEOUT
	IORING_OP_CONNECT
	IORING_OP_FALLOCATE
	IORING_OP_OPENAT
	IORING_OP_CLOSE
	IORING_OP_FILES_UPDATE
	IORING_OP_STATX
	IORING_OP_READ
	IORING_OP_WRITE
	IORING_OP_FADVISE
	IORING_OP_MADVISE
	IORING_OP_SEND
	IORING_OP_RECV
	IORING_OP_OPENAT2
	IORING_OP_EPOLL_CTL
	IORING_OP_SPLICE
	IORING_OP_PROVIDE_BUFFERS
	IORING_OP_REMOVE_BUFFERS
	IORING_OP_TEE
	IORING_OP_LAST
)

// submission queue entry flags
const (
	IOSQE_FIXED_FILE uint8 = 1 << iota
	IOSQE_IO_DRAIN
	IOSQE_IO_LINK
	IOSQE_IO_HARDLINK
	IOSQE_ASYNC
	IOSQE_BUFFER_SELECT
)

// sqe fsync flags
const IORING_FSYNC_DATASYNC uint32 = 1 << 0

// sqe timeout flags
const IORING_TIMEOUT_ABS uint32 = 1 << 0

// sqe splice flags
const SPLICE_F_FD_IN_FIXED uint32 = 1 << 31

// cqe flags
const IORING_CQE_F_BUFFER uint32 = 1 << 0

const IORING_CQE_BUFFER_SHIFT uint32 = 16

// cqe ring flags
const IORING_CQ_EVENTFD_DISABLED uint32 = 1 << 0

// setup flags
const (
	IORING_SETUP_IOPOLL uint32 = 1 << iota
	IORING_SETUP_SQPOLL
	IORING_SETUP_SQ_AFF
	IORING_SETUP_CQSIZE
	IORING_SETUP_CLAMP
	IORING_SETUP_ATTACH_WQ
)

// offsets for mmap
const (
	IORING_OFF_SQ_RING int64 = 0
	IORING_OFF_CQ_RING int64 = 0x8000000
	IORING_OFF_SQES    int64 = 0x10000000
)

// sq ring flags
const (
	IORING_SQ_NEED_WAKEUP uint32 = 1 << iota
	IORING_SQ_CQ_OVERFLOW
)

// enter flags
const (
	IORING_ENTER_GETEVENTS uint32 = 1 << iota
	IORING_ENTER_SQ_WAKEUP
)

// params feature flags
const (
	IORING_FEAT_SINGLE_MMAP uint32 = 1 << iota
	IORING_FEAT_NODROP
	IORING_FEAT_SUBMIT_STABLE
	IORING_FEAT_RW_CUR_POS
	IORING_FEAT_CUR_PERSONALITY
	IORING_FEAT_FAST_POLL
	IORING_FEAT_POLL_32BITS
)

var (
	sqeSize = unsafe.Sizeof(SQEntry{})
	cqeSize = unsafe.Sizeof(CQEntry{})
)

type IOUringParams struct {
	SQEntries    uint32
	CQEntries    uint32
	Flags        uint32
	SQThreadCPU  uint32
	SQThreadIdle uint32
	Features     uint32
	// reserved 16 bytes
	resv  [4]uint32
	SQOff SQRingOffsets
	CQOff CQRingOffsets
}

type SQRingOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Flags       uint32
	Dropped     uint32
	Array       uint32
	Resv1       uint32
	Resv2       uint64
}

type CQRingOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Overflow    uint32
	CQEs        uint32
	Flags       uint32
	Resv1       uint32
	Resv2       uint64
}

type SQEntry struct {
	opcode      uint8
	flags       uint8
	ioprio      uint16
	fd          int32
	offset      uint64 // union {off,addr2}
	addr        uint64 // union {addr,splice_off_in}
	len         uint32
	opcodeFlags uint32 // union for opcode specific flags
	userData    uint64

	bufIG       uint16
	personality uint16
	spliceFdIn  uint32
	pad2        [2]uint64
}

func (e *SQEntry) SetOpcode(opcode uint8) {
	e.opcode = opcode
}

func (e *SQEntry) SetFlags(flags uint8) {
	e.flags = flags
}

func (e *SQEntry) SetIOPrio(ioprio uint16) {
	e.ioprio = ioprio
}

func (e *SQEntry) SetFD(fd int32) {
	e.fd = fd
}

func (e *SQEntry) SetUserData(ud uint64) {
	e.userData = ud
}

func (e *SQEntry) SetOffset(off uint64) {
	e.offset = off
}

func (e *SQEntry) SetAddr(addr uint64) {
	e.addr = addr
}

func (e *SQEntry) SetLen(len uint32) {
	e.len = len
}

func (e *SQEntry) SetOpcodeFlags(flags uint32) {
	e.opcodeFlags = flags
}

func (e *SQEntry) SetBufIndex(index uint16) {
	e.bufIG = index
}

func (e *SQEntry) SetBufGroup(group uint16) {
	e.bufIG = group
}

func (e *SQEntry) SetPersonality(personality uint16) {
	e.personality = personality
}

func (e *SQEntry) SetSpliceOffIn(val uint64) {
	e.addr = val
}

func (e *SQEntry) SetSpliceFdIn(val uint32) {
	e.spliceFdIn = val
}

func (e *SQEntry) SetAddr2(addr2 uint64) {
	e.offset = addr2
}

type CQEntry struct {
	userData uint64
	res      int32
	flags    uint32
}

func (e CQEntry) Result() int32 {
	return e.res
}

func (e CQEntry) Flags() uint32 {
	return e.flags
}

func (e CQEntry) UserData() uint64 {
	return e.userData
}

type Sigset_t struct {
	Val [16]uint64
}
