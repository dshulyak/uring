package fs

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dshulyak/uring"
	"github.com/dshulyak/uring/fixed"
	"github.com/dshulyak/uring/queue"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestReadAtWriteAt(t *testing.T) {
	queue, err := queue.SetupSharded(1, 1024, nil)
	require.NoError(t, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "testing-fs-file-", 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(f.Name())
	})

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

func BenchmarkWriteAtOS(b *testing.B) {
	f, err := ioutil.TempFile("", "testing-write-os-")
	require.NoError(b, err)
	require.NoError(b, f.Close())
	f, err = os.OpenFile(f.Name(), os.O_RDWR|unix.O_DSYNC, 06444)
	require.NoError(b, err)
	b.Cleanup(func() {
		os.Remove(f.Name())
	})

	size := int64(256 << 10)
	buf := make([]byte, size)
	offset := int64(0)

	b.SetBytes(size)
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

func BenchmarkReadAtOS(b *testing.B) {
	f, err := ioutil.TempFile("", "testing-write-os-")
	require.NoError(b, err)
	b.Cleanup(func() {
		os.Remove(f.Name())
	})

	size := int64(256 << 10)
	buf := make([]byte, size)
	offset := int64(0)

	for i := 0; i < b.N; i++ {
		_, err := f.WriteAt(buf, int64(i)*size)
		require.NoError(b, err)
	}

	b.SetBytes(size)
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

func BenchmarkWriteAtDsync(b *testing.B) {
	queue, err := queue.SetupSharded(50, 128, &uring.IOUringParams{
		CQEntries: 4096,
		Flags:     uring.IORING_SETUP_CQSIZE,
	}, queue.OptRoundRobin)
	require.NoError(b, err)
	b.Cleanup(func() { queue.Close() })

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "testing-fs-file-", unix.O_DSYNC)
	require.NoError(b, err)
	b.Cleanup(func() { os.Remove(f.Name()) })

	size := int64(256 << 10)
	offset := int64(0)
	buf := make([]byte, size)

	var wg sync.WaitGroup

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	for w := 0; w < b.N; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.WriteAt(buf, atomic.AddInt64(&offset, size)-size)
			if err != nil {
				b.Error(err)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkReadAt(b *testing.B) {
	queue, err := queue.SetupSharded(8, 1024, &uring.IOUringParams{
		CQEntries: 8 * 1024,
		Flags:     uring.IORING_SETUP_CQSIZE,
	})
	require.NoError(b, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "testing-fs-file-", 0)
	require.NoError(b, err)
	b.Cleanup(func() {
		os.Remove(f.Name())
	})

	size := int64(256 << 10)
	offset := int64(0)
	buf := make([]byte, size)
	for i := 0; i < b.N; i++ {
		_, err := f.WriteAt(buf, int64(i)*size)
		require.NoError(b, err)
	}

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	for w := 0; w < b.N; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.ReadAt(buf, atomic.AddInt64(&offset, size)-size)
			if err != nil {
				b.Error(err)
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

	f, err := TempFile(fsm, "testing-fs-file-", 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(f.Name())
	})

	n, err := f.WriteAt(nil, 0)
	require.Equal(t, 0, n)
	require.NoError(t, err)
}

func TestConcurrentWritesIntegrity(t *testing.T) {
	queue, err := queue.SetupSharded(8, 1024, nil)
	require.NoError(t, err)
	defer queue.Close()

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "test-concurrent-writes", 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(f.Name())
	})

	var wg sync.WaitGroup
	var n int64 = 30000

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
