package uring

import "syscall"

func RetryEINTR(f func() error) {
	var err error
	for {
		err = f()
		if err != syscall.EINTR {
			return
		}
	}
}
