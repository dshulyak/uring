package fs

import (
	"io/ioutil"
	"os"
)

func TempFile(fsm *Filesystem, dir, pattern string) (*File, error) {
	f, err := ioutil.TempFile(dir, pattern)
	if err == nil {
		err = f.Close()
		if err != nil {
			return nil, err
		}
	}
	return fsm.Open(f.Name(), os.O_RDWR, 0644)
}
