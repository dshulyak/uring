package uring

import (
	"sync/atomic"
	"syscall"
	"unsafe"
)

// sqRing ...
type sqRing struct {
	head        *uint32
	tail        *uint32
	ringMask    *uint32
	ringEntries *uint32
	dropped     *uint32
	flags       *uint32
	array       uint32Array

	sqes    sqeArray
	sqeHead uint32
	sqeTail uint32
}

type uint32Array uintptr

func (a uint32Array) get(idx uint32) uint32 {
	return *(*uint32)(unsafe.Pointer(uintptr(a) + uintptr(idx*4)))
}

func (a uint32Array) set(idx uint32, value uint32) {
	*(*uint32)(unsafe.Pointer(uintptr(a) + uintptr(idx*4))) = value
}

type sqeArray uintptr

func (a sqeArray) get(idx uint32) *SQEntry {
	return (*SQEntry)(unsafe.Pointer(uintptr(a) + uintptr(idx)*sqeSize))
}

func (a sqeArray) set(idx uint32, value SQEntry) {
	*(*SQEntry)(unsafe.Pointer(uintptr(a) + uintptr(idx)*sqeSize)) = value
}

type cqRing struct {
	head        *uint32
	tail        *uint32
	ringmask    *uint32
	ringEntries *uint32
	overflow    *uint32
	flags       *uint32
	cqes        cqeArray
}

type cqeArray uintptr

func (a cqeArray) get(idx uint32) CQEntry {
	return *(*CQEntry)(unsafe.Pointer(uintptr(a) + uintptr(idx)*cqeSize))
}

func (a cqeArray) set(idx uint32, value CQEntry) {
	*(*CQEntry)(unsafe.Pointer(uintptr(a) + uintptr(idx)*cqeSize)) = value
}

// Ring is an interface to io_uring kernel framework.
// Not safe to use from multiple goroutines without additional synchronization.
// API is inspired mostly by liburing.
type Ring struct {
	// fd returned by IO_URING_SETUP
	fd     int
	params IOUringParams

	sq sqRing
	cq cqRing

	// pointers returned by mmap calls, used only for munmap
	// sqData ...
	sqData []byte
	// cqData can be nil if kernel supports IORING_FEAT_SINGLE_MMAP
	cqData []byte
	// sqArrayData array mapped with Ring.fd at IORING_OFF_SQES offset
	sqArrayData []byte

	eventfd uintptr
}

// Fd is a io_uring fd returned from IO_URING_SETUP syscall.
func (r *Ring) Fd() uintptr {
	return uintptr(r.fd)
}

// Eventfd is a eventfd for this uring instance. Call ring.Setupeventfd() to setup one.
func (r *Ring) Eventfd() uintptr {
	return r.eventfd
}

func (r *Ring) CQSize() uint32 {
	return r.params.CQEntries
}

func (r *Ring) SQSize() uint32 {
	return r.params.SQEntries
}

func (r *Ring) SQSlots() uint32 {
	return *r.sq.ringEntries - (r.sq.sqeTail - atomic.LoadUint32(r.sq.head))
}

func (r *Ring) SQSlotsAvailable() bool {
	return *r.sq.ringEntries > (r.sq.sqeTail - atomic.LoadUint32(r.sq.head))
}

func (r *Ring) CQSlots() uint32 {
	return *r.cq.ringEntries - (atomic.LoadUint32(r.cq.tail) - *r.cq.head)
}

// GetSQEntry returns earliest available SQEntry. May return nil if there are
// not available entries.
// Entry can be reused after Submit or Enter.
// Correct usage:
//   sqe := ring.GetSQEntry()
//   ring.Submit(0)
// ... or ...
//   sqe := ring.GetSQEntry()
//   ring.Flush()
//   ring.Enter(0, 0)
func (r *Ring) GetSQEntry() *SQEntry {
	head := atomic.LoadUint32(r.sq.head)
	next := r.sq.sqeTail + 1
	if next-head <= *r.sq.ringEntries {
		idx := r.sq.sqeTail & *r.sq.ringMask
		r.sq.sqeTail = next
		sqe := r.sq.sqes.get(idx)
		sqe.Reset()
		return sqe
	}
	return nil
}

// Flush submission queue.
func (r *Ring) Flush() uint32 {
	toSubmit := r.sq.sqeTail - r.sq.sqeHead
	if toSubmit == 0 {
		return 0
	}

	tail := *r.sq.tail
	mask := *r.sq.ringMask
	for i := toSubmit; i > 0; i-- {
		r.sq.array.set(tail&mask, r.sq.sqeHead&mask)
		tail++
		r.sq.sqeHead++
	}
	atomic.StoreUint32(r.sq.tail, tail)
	return toSubmit
}

// Enter io_uring instance. submited and minComplete will be passed as is.
func (r *Ring) Enter(submitted uint32, minComplete uint32) (uint32, error) {
	var flags uint32
	if r.sqNeedsEnter(submitted, &flags) || minComplete > 0 {
		if minComplete > 0 || (r.params.Flags&IORING_SETUP_IOPOLL) > 0 {
			flags |= IORING_ENTER_GETEVENTS
		}
		return r.enter(submitted, minComplete, flags)
	}
	return 0, nil
}

// Submit and wait for specified number of entries.
func (r *Ring) Submit(minComplete uint32) (uint32, error) {
	return r.Enter(r.Flush(), minComplete)
}

// GetCQEntry returns entry from completion queue, performing IO_URING_ENTER syscall if necessary.
// CQE is copied from mmaped region to avoid additional sync step after CQE was consumed.
// syscall.EAGAIN will be returned if there are no completed entries and minComplete is 0.
// syscall.EINTR will be returned if IO_URING_ENTER was interrupted while waiting for completion.
func (r *Ring) GetCQEntry(minComplete uint32) (CQEntry, error) {
	needs := r.cqNeedsEnter()
	if needs {
		if _, err := r.enter(0, minComplete, 0); err != nil {
			return CQEntry{}, err
		}
	}
	exit := needs
	for {
		cqe, found := r.peekCQEntry()
		if found {
			return cqe, nil
		}
		if exit {
			break
		}
		if minComplete > 0 {
			if _, err := r.enter(0, minComplete, IORING_ENTER_GETEVENTS); err != nil {
				return CQEntry{}, err
			}
		}
		exit = true
	}
	return CQEntry{}, syscall.EAGAIN
}

func (r *Ring) enter(submitted, minComplete, flags uint32) (uint32, error) {
	r1, _, errno := syscall.Syscall6(IO_URING_ENTER, uintptr(r.fd), uintptr(submitted), uintptr(minComplete), uintptr(flags), 0, 0)
	if errno == 0 {
		return uint32(r1), nil
	}
	return uint32(r1), error(errno)
}

// peekCQEntry returns cqe is available and updates head
func (r *Ring) peekCQEntry() (CQEntry, bool) {
	next := *r.cq.head
	if next < atomic.LoadUint32(r.cq.tail) {
		cqe := r.cq.cqes.get(next & *r.cq.ringmask)
		atomic.StoreUint32(r.cq.head, next+1)
		return cqe, true
	}
	return CQEntry{}, false
}

func (r *Ring) cqNeedsEnter() bool {
	if r.cq.flags != nil {
		if atomic.LoadUint32(r.cq.flags)&IORING_SQ_CQ_OVERFLOW > 0 {
			return true
		}
	}
	return r.params.Flags&IORING_SETUP_IOPOLL > 0
}

func (r *Ring) sqNeedsEnter(submitted uint32, flags *uint32) bool {
	if (r.params.Flags&IORING_SETUP_SQPOLL) == 0 && submitted > 0 {
		return true
	}
	if (atomic.LoadUint32(r.sq.flags) & IORING_SQ_NEED_WAKEUP) > 0 {
		*flags |= IORING_ENTER_SQ_WAKEUP
		return true
	}
	return false
}
