package fs

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/dshulyak/uring"
	"github.com/dshulyak/uring/queue"
	"github.com/stretchr/testify/require"
)

func TestReadAtWriteAt(t *testing.T) {
	ring, err := uring.Setup(1024, nil)
	require.NoError(t, err)
	defer ring.Close()
	queue := queue.New(ring)
	defer queue.Close()

	fsm := NewFilesystem(queue, ring)

	f, err := ioutil.TempFile("", "testing-fs-file-")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	uf, err := fsm.Open(f.Name(), os.O_RDWR, 0644)
	require.NoError(t, err)

	in, out := make([]byte, 4), []byte("ping")
	_, err = f.Write(out)
	require.NoError(t, err)

	n, err := uf.ReadAt(in, 0)
	require.NoError(t, err)
	require.Equal(t, len(out), n)
	require.Equal(t, out, in)

	out = []byte("pong")
	n, err = uf.WriteAt(out, 0)
	require.NoError(t, err)
	require.Equal(t, len(out), n)

	n, err = f.ReadAt(in, 0)
	require.NoError(t, err)
	require.Equal(t, len(out), n)
	require.Equal(t, out, in)

	require.NoError(t, uf.Close())
}

func TestReadWrite(t *testing.T) {
	ring, err := uring.Setup(1024, nil)
	require.NoError(t, err)
	defer ring.Close()
	queue := queue.New(ring)
	defer queue.Close()

	fsm := NewFilesystem(queue, ring)

	f, err := ioutil.TempFile("", "testing-fs-file-")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	uf, err := fsm.Open(f.Name(), os.O_RDWR, 0644)
	require.NoError(t, err)

	in := []byte("ping")
	n, err := uf.Write(in)
	require.NoError(t, err)
	require.Equal(t, 4, n)

	out := make([]byte, 4)
	n, err = uf.Read(out)
	require.NoError(t, err)
	require.Len(t, out, 4)
	require.Equal(t, in, out)
}
