package fs

import (
	"io"
	"os"
	"sync/atomic"
	"testing"

	"github.com/dshulyak/uring/fixed"
	"github.com/dshulyak/uring/queue"
	"github.com/stretchr/testify/require"
)

func TestReadAtWriteAt(t *testing.T) {
	queue, err := queue.SetupSharded(1, 1024, nil)
	require.NoError(t, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "", "testing-fs-file-")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	pool, err := fixed.New(queue, 4, 2)
	require.NoError(t, err)
	defer pool.Close()

	in, out := pool.Get(), pool.Get()
	copy(out.Bytes(), []byte("ping"))
	_, err = f.WriteAt(out, 0)
	require.NoError(t, err)

	n, err := f.ReadAt(in, 0)
	require.NoError(t, err)
	require.Equal(t, int(out.Len), n)
	require.Equal(t, out.Bytes(), in.Bytes())

	copy(out.Bytes(), []byte("pong"))
	n, err = f.WriteAt(out, 0)
	require.NoError(t, err)
	require.Equal(t, int(out.Len), n)

	n, err = f.ReadAt(in, 0)
	require.NoError(t, err)
	require.Equal(t, int(out.Len), n)
	require.Equal(t, out.Bytes(), in.Bytes())

	require.NoError(t, f.Close())
}

func TestReadWrite(t *testing.T) {
	queue, err := queue.SetupSharded(1, 1024, nil)
	require.NoError(t, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	// oppened with O_APPEND
	f, err := TempFile(fsm, "", "testing-fs-file-")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	pool, err := fixed.New(queue, 5, 3)
	require.NoError(t, err)
	in1, in2, out := pool.Get(), pool.Get(), pool.Get()

	copy(in1.Bytes(), []byte("ping1"))
	n, err := f.Write(in1.Bytes())
	require.NoError(t, err)
	require.Equal(t, 5, n)

	copy(in2.Bytes(), []byte("ping2"))
	n, err = f.Write(in2.Bytes())
	require.NoError(t, err)
	require.Equal(t, 5, n)

	_, err = f.Read(out.Bytes())
	require.NoError(t, err)
	require.Equal(t, in1.Bytes(), out.Bytes())

	_, err = f.ReadAt(out, 5)
	require.NoError(t, err)
	require.Equal(t, in2.Bytes(), out.Bytes())
}

func BenchmarkWriteAt(b *testing.B) {
	queue, err := queue.SetupSharded(8, 4096, nil)
	require.NoError(b, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "", "testing-fs-file-")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	size := int64(8 << 10)
	pool, err := fixed.New(queue, int(size), 1)
	require.NoError(b, err)
	defer pool.Close()
	offset := int64(0)

	buf := pool.Get()

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := f.WriteAt(buf, atomic.AddInt64(&offset, size)-size)
			if err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkReadAt(b *testing.B) {
	queue, err := queue.SetupSharded(8, 1024, nil)
	require.NoError(b, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "", "testing-fs-file-")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	size := int64(8 << 10)
	offset := int64(0)

	pool, err := fixed.New(queue, int(size), 1)
	require.NoError(b, err)
	defer pool.Close()

	buf := pool.Get()
	for i := 0; i < b.N; i++ {
		_, err := f.WriteAt(buf, int64(i)*size)
		require.NoError(b, err)
	}

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := f.ReadAt(buf, atomic.AddInt64(&offset, size)-size)
			if err != nil {
				b.Error(err)
			}
		}
	})
}

func TestEmptyWrite(t *testing.T) {
	queue, err := queue.SetupSharded(8, 1024, nil)
	require.NoError(t, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "", "testing-fs-file-")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	pool, err := fixed.New(queue, 10, 1)
	require.NoError(t, err)
	defer pool.Close()
	buf := pool.Get()
	buf.Len = 0

	n, err := f.WriteAt(buf, 0)
	require.Equal(t, 0, n)
	require.NoError(t, err)
}

func TestEOF(t *testing.T) {
	queue, err := queue.SetupSharded(8, 1024, nil)
	require.NoError(t, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "", "testing-fs-file-")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	_, err = f.Write([]byte{1})
	require.NoError(t, err)
	n, err := f.Read(make([]byte, 2))
	require.Equal(t, 1, n)
	require.Equal(t, io.EOF, err)
}
