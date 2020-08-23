package loop

import (
	"syscall"
	"unsafe"
)

func newPoll(n int) (*poll, error) {
	p, err := syscall.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &poll{fd: p, events: make([]syscall.EpollEvent, n)}, nil
}

type poll struct {
	fd int // epoll fd

	buf    [8]byte
	events []syscall.EpollEvent
}

func (p *poll) addRead(fd int32) error {
	return syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_ADD, int(fd),
		&syscall.EpollEvent{
			Fd:     fd,
			Events: syscall.EPOLLIN,
		},
	)
}

func (p *poll) wait(iter func(int32)) error {
	for {
		n, err := syscall.EpollWait(p.fd, p.events, -1)
		if err == syscall.EINTR {
			continue
		}
		for i := 0; i < n; i++ {
			_, err := syscall.Read(int(p.events[i].Fd), p.buf[:])
			if err != nil {
				panic(err)
			}
			// uint64 in the machine native order
			cnt := *(*uint64)(unsafe.Pointer(&p.buf))
			for j := uint64(0); j < cnt; j++ {
				iter(p.events[i].Fd)
			}
		}
		return err
	}
}

func (p *poll) close() error {
	return syscall.Close(p.fd)
}
