package fs

import (
	"sync"
)

// FileRegistry ...
type FileRegistry interface {
	RegisterFiles([]int32) error
	UnregisterFiles() error
	UpdateFiles([]int32, uint32) error
}

func newFixedFiles(reg FileRegistry, n int) *fixedFiles {
	fds := make([]int32, n)
	for i := range fds {
		fds[i] = -1
	}
	return &fixedFiles{
		fds:         fds,
		reg:         reg,
		requiresReg: true,
	}
}

type fixedFiles struct {
	mu          sync.Mutex
	requiresReg bool
	fds         []int32
	// registered might be lower than len(fds) if some of the fds are -1
	registered int

	reg FileRegistry
}

func (f *fixedFiles) register(fd uintptr) (uintptr, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.fds) == f.registered {
		err := f.reg.UnregisterFiles()
		if err != nil {
			return 0, err
		}
		fds := make([]int32, len(f.fds))
		for i := range fds {
			if fds[i] == 0 {
				fds[i] = -1
			}
		}
		f.requiresReg = true
		f.fds = append(f.fds, fds...)
	}
	var i int
	for i = range f.fds {
		if f.fds[i] == -1 {
			f.fds[i] = int32(fd)
			break
		}
	}
	f.registered++
	if f.requiresReg {
		err := f.reg.RegisterFiles(f.fds)
		if err != nil {
			return 0, err
		}
		f.requiresReg = false
	} else {
		err := f.reg.UpdateFiles(f.fds, 0)
		if err != nil {
			return 0, err
		}
	}
	return uintptr(i), nil
}

func (f *fixedFiles) unregister(idx uintptr) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fds[idx] = -1
	f.registered--
	return f.reg.UpdateFiles(f.fds, 0)
}
