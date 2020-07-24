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

func (a sqeArray) get(idx uint32) SQEntry {
	return *(*SQEntry)(unsafe.Pointer(uintptr(a) + uintptr(idx)*sqeSize))
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
}

func (r *Ring) CQSize() int {
	return int(r.params.CQEntries)
}

func (r *Ring) SQSize() int {
	return int(r.params.SQEntries)
}

func (r *Ring) SQSlots() int {
	head := atomic.LoadUint32(r.sq.head)
	return int(*r.sq.ringEntries) - int(r.sq.sqeTail-head)
}

func (r *Ring) CQSlots() int {
	head := *r.cq.head
	tail := atomic.LoadUint32(r.cq.tail)
	delta := tail - head
	return int(*r.cq.ringEntries - delta)
}

func (r *Ring) Push(sqes ...SQEntry) uint32 {
	head := atomic.LoadUint32(r.sq.head)
	var i uint32
	for i = 0; i < uint32(len(sqes)); i++ {
		next := r.sq.sqeTail + 1
		if next-head <= *r.sq.ringEntries {
			idx := r.sq.sqeTail & *r.sq.ringMask
			r.sq.sqes.set(idx, sqes[i])
			r.sq.sqeTail = next
		} else {
			break
		}
	}
	return i
}

// Submit and wait for specified number of entries.
func (r *Ring) Submit(submitted uint32, minComplete uint32) (int, error) {
	// TODO get the actual number of submitted records from flushed sq
	r.flushSq()
	var flags uint32
	if r.sqNeedsEnter(submitted, &flags) || minComplete > 0 {
		if minComplete > 0 || (r.params.Flags&IORING_SETUP_IOPOLL) > 0 {
			flags |= IORING_ENTER_GETEVENTS
		}
		return r.enter(submitted, minComplete, flags)
	}
	return 0, nil
}

// GetCQEntry returns entry from completion queue, performing IO_URING_ENTER syscall if necessary.
// CQE is copied from mmaped region to avoid additional sync step after CQE was consumed.
func (r *Ring) GetCQEntry(minComplete uint32) (CQEntry, error) {
	for {
		var flags uint32
		cqe, found := r.peekCQEntry()
		if found {
			return cqe, nil
		}
		var enter bool
		if r.cqNeedsEnter() {
			flags |= IORING_ENTER_GETEVENTS
			enter = true
		} else if r.sqNeedsEnter(0, &flags) {
			enter = true
		} else if minComplete > 0 {
			enter = true
		}
		if enter {
			if _, err := r.enter(0, minComplete, flags); err != nil {
				return CQEntry{}, err
			}
			continue
		}
		return CQEntry{}, syscall.EAGAIN
	}
	panic("unreachable")
}

func (r *Ring) enter(submitted, minComplete, flags uint32) (int, error) {
	r1, _, errno := syscall.Syscall6(IO_URING_ENTER, uintptr(r.fd), uintptr(submitted), uintptr(minComplete), uintptr(flags), 0, 0)
	if errno == 0 {
		return int(r1), nil
	}
	return int(r1), error(errno)
}

func (r *Ring) flushSq() {
	if r.sq.sqeTail == r.sq.sqeHead {
		return
	}

	tail := *r.sq.tail
	mask := *r.sq.ringMask
	toSubmit := r.sq.sqeTail - r.sq.sqeHead
	for ; toSubmit > 0; toSubmit-- {
		r.sq.array.set(tail&mask, r.sq.sqeHead&mask)
		tail++
		r.sq.sqeHead++
	}
	atomic.StoreUint32(r.sq.tail, tail)
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
	return atomic.LoadUint32(r.cq.flags)&IORING_SQ_CQ_OVERFLOW > 0
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
