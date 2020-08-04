package uring

import (
	"syscall"
	"unsafe"
)

const (
	IORING_REGISTER_BUFFERS uintptr = iota
	IORING_UNREGISTER_BUFFERS
	IORING_REGISTER_FILES
	IORING_UNREGISTER_FILES
	IORING_REGISTER_EVENTFD
	IORING_UNREGISTER_EVENTFD
	IORING_REGISTER_FILES_UPDATE
	IORING_REGISTER_EVENTFD_ASYNC
	IORING_REGISTER_PROBE
	IORING_REGISTER_PERSONALITY
	IORING_UNREGISTER_PERSONALITY
)

const (
	IO_URING_OP_SUPPORTED uint16 = 1 << 0
)

const (
	probeOpsSize = uintptr(IORING_OP_LAST) + 1
)

type Probe struct {
	LastOp uint8
	OpsLen uint8
	resv   uint16
	resv2  [3]uint32
	Ops    [probeOpsSize]ProbeOp
}

func (p Probe) IsSupported(op uint8) bool {
	for i := uint8(0); i < p.OpsLen; i++ {
		if p.Ops[i].Op != op {
			continue
		}
		return p.Ops[i].Flags&IO_URING_OP_SUPPORTED > 0
	}
	return false
}

type ProbeOp struct {
	Op    uint8
	resv  uint8
	Flags uint16
	resv2 uint32
}

func (r *Ring) RegisterProbe(probe *Probe) error {
	_, _, errno := syscall.Syscall6(
		IO_URING_REGISTER,
		uintptr(r.fd),
		IORING_REGISTER_PROBE,
		uintptr(unsafe.Pointer(probe)),
		probeOpsSize, 0, 0)
	if errno > 0 {
		return error(errno)
	}
	return nil
}

func (r *Ring) RegisterFiles(fds []int32) error {
	_, _, errno := syscall.Syscall6(
		IO_URING_REGISTER,
		uintptr(r.fd),
		IORING_REGISTER_FILES,
		uintptr(unsafe.Pointer(&fds[0])),
		uintptr(len(fds)), 0, 0)
	if errno > 0 {
		return error(errno)
	}
	return nil
}

func (r *Ring) UnregisterFiles() error {
	_, _, errno := syscall.Syscall(
		IO_URING_REGISTER,
		uintptr(r.fd),
		IORING_UNREGISTER_FILES,
		0)
	if errno > 0 {
		return error(errno)
	}
	return nil
}

func (r *Ring) SetupEventfd() error {
	if r.eventfd != 0 {
		return nil
	}
	r0, _, errno := syscall.Syscall(syscall.SYS_EVENTFD2, 0, 0, 0)
	if errno > 0 {
		return error(errno)
	}
	r.eventfd = r0
	_, _, errno = syscall.Syscall6(IO_URING_REGISTER, uintptr(r.fd), IORING_REGISTER_EVENTFD, uintptr(unsafe.Pointer(&r0)), 1, 0, 0)
	if errno > 0 {
		_ = r.CloseEventfd()
		return error(errno)
	}
	return nil
}

func (r *Ring) CloseEventfd() error {
	if r.eventfd != 0 {
		return nil
	}
	_, _, errno := syscall.Syscall(IO_URING_REGISTER, uintptr(r.fd), IORING_UNREGISTER_EVENTFD, 0)
	if err := syscall.Close(int(r.eventfd)); err != nil {
		return err
	}
	r.eventfd = 0
	if errno > 0 {
		return error(errno)
	}
	return nil
}
