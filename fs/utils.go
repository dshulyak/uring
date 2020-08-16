package fs

import (
	"io/ioutil"
	"os"
)

func TempFile(fsm *Filesystem, pattern string, flags int) (*File, error) {
	f, err := ioutil.TempFile("", pattern)
	if err == nil {
		err = f.Close()
		if err != nil {
			return nil, err
		}
	}
	return fsm.Open(f.Name(), os.O_RDWR|flags, 0644)
}
