package loop

import (
	"unsafe"
)

type notifyList struct {
	wait   uint32
	notify uint32
	lock   uintptr
	head   unsafe.Pointer
	tail   unsafe.Pointer
}

//go:linkname notifyListAdd sync.runtime_notifyListAdd
func notifyListAdd(*notifyList) uint32

//go:linkname notifyListWait sync.runtime_notifyListWait
func notifyListWait(*notifyList, uint32)

//go:linkname notifyListNotifyOne sync.runtime_notifyListNotifyOne
func notifyListNotifyOne(*notifyList)

type waiter struct {
	notify notifyList
	ticket uint32
}

func (w *waiter) add() {
	w.ticket = notifyListAdd(&w.notify)
}

func (w *waiter) wait() {
	notifyListWait(&w.notify, w.ticket)
}

func (w *waiter) signal() {
	notifyListNotifyOne(&w.notify)
}
