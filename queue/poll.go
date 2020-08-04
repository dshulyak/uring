package queue

import (
	"syscall"

	"github.com/dshulyak/uring"
)

func newPoll(ring *uring.Ring) (*poll, error) {
	p, err := syscall.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	if err := ring.SetupEventfd(); err != nil {
		return nil, err
	}
	pl := &poll{fd: p, ring: ring}
	if err := pl.addRead(int(ring.Eventfd())); err != nil {
		_ = pl.close()
		return nil, err
	}
	return pl, err
}

type poll struct {
	fd int // epoll fd

	ring *uring.Ring
}

func (p *poll) addRead(fd int) error {
	return syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_ADD, fd,
		&syscall.EpollEvent{Fd: int32(fd),
			Events: syscall.EPOLLIN,
		},
	)
}

func (p *poll) wait() (int, error) {
	events := make([]syscall.EpollEvent, 1)
	for {
		n, err := syscall.EpollWait(p.fd, events, -1)
		if err == syscall.EINTR {
			continue
		}
		return n, err
	}
}

func (p *poll) close() error {
	e1 := syscall.Close(p.fd)
	e2 := p.ring.CloseEventfd()
	if e1 != nil {
		return e1
	}
	return e2
}
