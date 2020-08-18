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
	// IO_URING_OP_SUPPORTED ...
	IO_URING_OP_SUPPORTED uint16 = 1 << 0
)

const (
	// probeOpsSize is uintptr so that it can be passed to syscall without casting
	probeOpsSize = uintptr(IORING_OP_LAST) + 1
)

// Probe ...
type Probe struct {
	LastOp uint8
	OpsLen uint8
	resv   uint16
	resv2  [3]uint32
	Ops    [probeOpsSize]ProbeOp
}

// IsSupported returns true if operation is supported.
func (p Probe) IsSupported(op uint8) bool {
	for i := uint8(0); i < p.OpsLen; i++ {
		if p.Ops[i].Op != op {
			continue
		}
		return p.Ops[i].Flags&IO_URING_OP_SUPPORTED > 0
	}
	return false
}

// ProbeOp ...
type ProbeOp struct {
	Op    uint8
	resv  uint8
	Flags uint16
	resv2 uint32
}

// RegisterProbe ...
func (r *Ring) RegisterProbe(probe *Probe) error {
	for {
		_, _, errno := syscall.Syscall6(
			IO_URING_REGISTER,
			uintptr(r.fd),
			IORING_REGISTER_PROBE,
			uintptr(unsafe.Pointer(probe)),
			probeOpsSize, 0, 0)
		if errno > 0 {
			if errno == syscall.EINTR {
				continue
			}
			return errno
		}
		return nil
	}
}

// RegisterFiles ...
func (r *Ring) RegisterFiles(fds []int32) error {
	for {
		_, _, errno := syscall.Syscall6(
			IO_URING_REGISTER,
			uintptr(r.fd),
			IORING_REGISTER_FILES,
			uintptr(unsafe.Pointer(&fds[0])),
			uintptr(len(fds)), 0, 0)
		if errno > 0 {
			if errno == syscall.EINTR {
				continue
			}
			return errno
		}
		return nil
	}
}

// UnregisterFiles ...
func (r *Ring) UnregisterFiles() error {
	for {
		_, _, errno := syscall.Syscall6(
			IO_URING_REGISTER,
			uintptr(r.fd),
			IORING_UNREGISTER_FILES,
			0, 0, 0, 0)
		if errno > 0 {
			if errno == syscall.EINTR {
				continue
			}
			return errno
		}
		return nil
	}
}

// UpdateFiles ...
func (r *Ring) UpdateFiles(fds []int32, off uint32) error {
	update := IOUringFilesUpdate{
		Offset: off,
		Fds:    &fds[0],
	}
	for {
		_, _, errno := syscall.Syscall6(
			IO_URING_REGISTER,
			uintptr(r.fd),
			IORING_REGISTER_FILES_UPDATE,
			uintptr(unsafe.Pointer(&update)),
			uintptr(len(fds)), 0, 0)
		if errno > 0 {
			if errno == syscall.EINTR {
				continue
			}
			return errno
		}
		return nil
	}
}

// RegisterBuffers ...
func (r *Ring) RegisterBuffers(iovec []syscall.Iovec) error {
	if len(iovec) == 0 {
		return nil
	}
	for {
		_, _, errno := syscall.Syscall6(
			IO_URING_REGISTER,
			uintptr(r.fd),
			IORING_REGISTER_BUFFERS,
			uintptr(unsafe.Pointer(&iovec[0])),
			uintptr(len(iovec)), 0, 0)
		if errno > 0 {
			if errno == syscall.EINTR {
				continue
			}
			return errno
		}
		return nil
	}
}

// UnregisterBuffers ...
func (r *Ring) UnregisterBuffers() error {
	for {
		_, _, errno := syscall.Syscall6(
			IO_URING_REGISTER,
			uintptr(r.fd),
			IORING_UNREGISTER_BUFFERS,
			0, 0, 0, 0)
		if errno > 0 {
			if errno == syscall.EINTR {
				continue
			}
			return errno
		}
		return nil
	}
}

// SetupEventfd creates eventfd and registers it with current uring instance.
func (r *Ring) SetupEventfd() error {
	if r.eventfd == 0 {
		r0, _, errno := syscall.Syscall(syscall.SYS_EVENTFD2, 0, 0, 0)
		if errno > 0 {
			return errno
		}
		r.eventfd = r0
	}
	for {
		_, _, errno := syscall.Syscall6(IO_URING_REGISTER, uintptr(r.fd), IORING_REGISTER_EVENTFD, uintptr(unsafe.Pointer(&r.eventfd)), 1, 0, 0)
		if errno > 0 {
			if errno == syscall.EINTR {
				continue
			}
			_ = r.CloseEventfd()
			return errno
		}
		return nil
	}
}

// CloseEventfd unregsiters eventfd from uring istance and closes associated fd.
func (r *Ring) CloseEventfd() error {
	if r.eventfd == 0 {
		return nil
	}
	var errno syscall.Errno
	for {
		_, _, errno = syscall.Syscall6(IO_URING_REGISTER, uintptr(r.fd), IORING_UNREGISTER_EVENTFD, 0, 0, 0, 0)
		if errno != syscall.EINTR {
			break
		}
	}
	if err := syscall.Close(int(r.eventfd)); err != nil {
		return err
	}
	r.eventfd = 0
	if errno > 0 {
		return errno
	}
	return nil
}
