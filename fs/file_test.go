package fs

import (
	"encoding/binary"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dshulyak/uring"
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
	_, err = f.WriteAtFixed(out, 0)
	require.NoError(t, err)

	n, err := f.ReadAtFixed(in, 0)
	require.NoError(t, err)
	require.Equal(t, int(out.Len()), n)
	require.Equal(t, out.Bytes(), in.Bytes())

	copy(out.Bytes(), []byte("pong"))
	n, err = f.WriteAtFixed(out, 0)
	require.NoError(t, err)
	require.Equal(t, int(out.Len()), n)

	n, err = f.ReadAtFixed(in, 0)
	require.NoError(t, err)
	require.Equal(t, int(out.Len()), n)
	require.Equal(t, string(out.Bytes()), string(in.Bytes()))

	require.NoError(t, f.Close())
}

func BenchmarkWriteAt(b *testing.B) {
	queue, err := queue.SetupSharded(8, 4096, &uring.IOUringParams{
		CQEntries: 8 * 2048,
		Flags:     uring.IORING_SETUP_CQSIZE,
	})
	require.NoError(b, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "", "testing-fs-file-")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	size := int64(256 << 10)
	pool, err := fixed.New(queue, int(size), 1)
	require.NoError(b, err)
	defer pool.Close()
	offset := int64(0)

	buf := pool.Get()

	workers := 10000
	n := b.N / workers

	var wg sync.WaitGroup

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				_, err := f.WriteAtFixed(buf, atomic.AddInt64(&offset, size)-size)
				if err != nil {
					b.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkReadAt(b *testing.B) {
	queue, err := queue.SetupSharded(8, 512, &uring.IOUringParams{
		CQEntries: 8 * 1024,
		Flags:     uring.IORING_SETUP_CQSIZE,
	})
	require.NoError(b, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "", "testing-fs-file-")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	size := int64(256 << 10)
	offset := int64(0)

	pool, err := fixed.New(queue, int(size), 1)
	require.NoError(b, err)
	defer pool.Close()

	buf := pool.Get()
	for i := 0; i < b.N; i++ {
		_, err := f.WriteAtFixed(buf, int64(i)*size)
		require.NoError(b, err)
	}

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	workers := 10000
	n := b.N / workers

	var wg sync.WaitGroup

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				_, err := f.ReadAtFixed(buf, atomic.AddInt64(&offset, size)-size)
				if err != nil {
					b.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}

func TestEmptyWrite(t *testing.T) {
	queue, err := queue.SetupSharded(8, 1024, nil)
	require.NoError(t, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "", "testing-fs-file-")
	require.NoError(t, err)

	n, err := f.WriteAt(nil, 0)
	require.Equal(t, 0, n)
	require.NoError(t, err)
}

func TestConcurrentWritesIntegrity(t *testing.T) {
	queue, err := queue.SetupSharded(8, 1024, nil)
	require.NoError(t, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "", "test-concurrent-writes-")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	var wg sync.WaitGroup
	var n int64 = 10000

	for i := int64(0); i < n; i++ {
		wg.Add(1)
		go func(i uint64) {
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, i)
			_, _ = f.WriteAt(buf, int64(i*8))
			wg.Done()
		}(uint64(i))
	}
	wg.Wait()

	buf2 := make([]byte, 8)
	for i := int64(0); i < n; i++ {
		_, err := f.ReadAt(buf2, i*8)
		require.NoError(t, err)
		rst := binary.BigEndian.Uint64(buf2[:])
		require.Equal(t, i, int64(rst))
	}
}
