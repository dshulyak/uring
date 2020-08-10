package fs

import (
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"

	"github.com/dshulyak/uring/queue"
	"github.com/stretchr/testify/require"
)

func TestReadAtWriteAt(t *testing.T) {
	queue, err := queue.SetupSharded(1, 1024, nil)
	require.NoError(t, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

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
	queue, err := queue.SetupSharded(1, 1024, nil)
	require.NoError(t, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

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

func BenchmarkWriteAt(b *testing.B) {
	queue, err := queue.SetupSharded(8, 1024, nil)
	require.NoError(b, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := ioutil.TempFile("", "testing-fs-file-")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	uf, err := fsm.Open(f.Name(), os.O_RDWR, 0644)
	require.NoError(b, err)

	size := int64(256 << 10)
	data := make([]byte, size)
	offset := int64(0)

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := uf.WriteAt(data, atomic.LoadInt64(&offset))
			if err != nil {
				b.Error(err)
			}
			atomic.AddInt64(&offset, size)
		}
	})
}

func BenchmarkReadAt(b *testing.B) {
	queue, err := queue.SetupSharded(8, 1024, nil)
	require.NoError(b, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := ioutil.TempFile("", "testing-fs-file-")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	uf, err := fsm.Open(f.Name(), os.O_RDWR, 0644)
	require.NoError(b, err)

	size := int64(256 << 10)
	data := make([]byte, size)
	offset := int64(0)

	for i := 0; i < b.N; i++ {
		_, err := f.Write(data)
		require.NoError(b, err)
	}

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := uf.ReadAt(data, atomic.LoadInt64(&offset))
			if err != nil {
				b.Error(err)
			}
			atomic.AddInt64(&offset, size)
		}
	})
}

func TestEmptyWrite(t *testing.T) {
	queue, err := queue.SetupSharded(8, 1024, nil)
	require.NoError(t, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := ioutil.TempFile("", "testing-fs-file-")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	uf, err := fsm.Open(f.Name(), os.O_RDWR, 0644)
	require.NoError(t, err)

	n, err := uf.WriteAt([]byte{}, 0)
	require.Equal(t, 0, n)
	require.NoError(t, err)
}
