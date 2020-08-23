package fs

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"unsafe"

	"github.com/dshulyak/uring"
	"github.com/dshulyak/uring/fixed"
	"github.com/dshulyak/uring/queue"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func aligned(bsize int) []byte {
	if bsize == 0 {
		return nil
	}
	if bsize%512 != 0 {
		panic("block size must be multiple of 512")
	}
	block := make([]byte, bsize+512)
	a := uintptr(unsafe.Pointer(&block[0])) & uintptr(511)
	var offset uintptr
	if a != 0 {
		offset = 512 - a
	}
	return block[offset : int(offset)+bsize]
}

func TestFixedBuffersIO(t *testing.T) {
	tester := func(t *testing.T, fsm *Filesystem, pool *fixed.Pool) {
		t.Helper()
		f, err := TempFile(fsm, "testing-io-", 0)
		require.NoError(t, err)
		t.Cleanup(func() { os.Remove(f.Name()) })

		in, out := pool.Get(), pool.Get()
		rand.Read(out.B)

		_, err = f.WriteAtFixed(out, 0)
		require.NoError(t, err)
		require.NoError(t, f.Datasync())
		_, err = f.ReadAtFixed(in, 0)
		require.NoError(t, err)
		require.Equal(t, in.B, out.B)
	}
	t.Run("registered", func(t *testing.T) {
		q, err := queue.Setup(32, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { q.Close() })
		fsm := NewFilesystem(q, RegisterFiles(32))
		pool, err := fixed.New(q, 100, 2)
		require.NoError(t, err)
		t.Cleanup(func() { pool.Close() })
		tester(t, fsm, pool)
	})
	t.Run("unregistered", func(t *testing.T) {
		q, err := queue.Setup(32, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { q.Close() })
		fsm := NewFilesystem(q)
		pool, err := fixed.New(q, 100, 2)
		require.NoError(t, err)
		t.Cleanup(func() { pool.Close() })
		tester(t, fsm, pool)
	})
}

func TestRegularIO(t *testing.T) {
	tester := func(t *testing.T, fsm *Filesystem) {
		t.Helper()
		f, err := TempFile(fsm, "testing-io-", 0)
		require.NoError(t, err)
		t.Cleanup(func() { os.Remove(f.Name()) })

		in, out := make([]byte, 100), make([]byte, 100)
		rand.Read(out)

		_, err = f.WriteAt(out, 0)
		require.NoError(t, err)
		require.NoError(t, f.Sync())
		_, err = f.ReadAt(in, 0)
		require.NoError(t, err)
		require.Equal(t, in, out)
	}
	t.Run("registered", func(t *testing.T) {
		q, err := queue.Setup(32, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { q.Close() })
		fsm := NewFilesystem(q, RegisterFiles(32))
		tester(t, fsm)
	})
	t.Run("unregistered", func(t *testing.T) {
		q, err := queue.Setup(32, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { q.Close() })
		fsm := NewFilesystem(q)
		tester(t, fsm)
	})
}

func benchmarkOSWriteAt(b *testing.B, size int64, fflags int) {
	f, err := ioutil.TempFile("", "testing-write-os-")
	require.NoError(b, err)
	require.NoError(b, f.Close())
	f, err = os.OpenFile(f.Name(), os.O_RDWR|fflags, 0644)
	require.NoError(b, err)
	b.Cleanup(func() {
		os.Remove(f.Name())
	})

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

func benchmarkOSReadAt(b *testing.B, size int64) {
	f, err := ioutil.TempFile("", "testing-write-os-")
	require.NoError(b, err)
	require.NoError(b, f.Close())
	b.Cleanup(func() {
		os.Remove(f.Name())
	})
	f, err = os.OpenFile(f.Name(), os.O_RDWR|unix.O_DIRECT, 0644)
	require.NoError(b, err)

	buf := aligned(int(size))
	offset := int64(0)

	wbuf := aligned(int(size) * (b.N + 1))
	_, err = f.WriteAt(wbuf, 0)
	require.NoError(b, err)

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

func BenchmarkWriteAt(b *testing.B) {
	for _, size := range []int64{512, 4 << 10, 8 << 10, 16 << 10, 64 << 10, 256 << 10} {
		b.Run(fmt.Sprintf("uring %d", size), func(b *testing.B) {
			q, err := queue.Setup(
				512,
				&uring.IOUringParams{
					CQEntries: 8 * 4096,
					Flags:     uring.IORING_SETUP_CQSIZE,
				},
				nil,
			)
			require.NoError(b, err)
			benchmarkWriteAt(b, q, size, 0)
		})
		b.Run(fmt.Sprintf("os %d", size), func(b *testing.B) {
			benchmarkOSWriteAt(b, size, 0)
		})
	}
}

func BenchmarkReadAt(b *testing.B) {
	for _, size := range []int64{512, 4 << 10, 8 << 10, 16 << 10, 64 << 10, 256 << 10} {
		b.Run(fmt.Sprintf("uring %d", size), func(b *testing.B) {
			q, err := queue.Setup(
				512,
				&uring.IOUringParams{
					CQEntries: 8 * 4096,
					Flags:     uring.IORING_SETUP_CQSIZE,
				},
				nil,
			)
			require.NoError(b, err)
			benchmarkReadAt(b, q, size)
		})
		b.Run(fmt.Sprintf("os %d", size), func(b *testing.B) {
			benchmarkOSReadAt(b, size)
		})
	}
}

func benchmarkWriteAt(b *testing.B, q *queue.Queue, size int64, fflags int) {
	b.Cleanup(func() { q.Close() })
	fsm := NewFilesystem(q)

	f, err := TempFile(fsm, "testing-fs-file-", fflags)
	require.NoError(b, err)
	b.Cleanup(func() { os.Remove(f.Name()) })

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

func benchmarkReadAt(b *testing.B, q *queue.Queue, size int64) {
	b.Cleanup(func() {
		q.Close()
	})

	fsm := NewFilesystem(q)

	f, err := TempFile(fsm, "testing-fs-file-", unix.O_DIRECT)
	require.NoError(b, err)
	b.Cleanup(func() {
		os.Remove(f.Name())
	})

	total := 0
	wbuf := aligned(int(size) * (b.N + 1))
	for total != len(wbuf) {
		n, err := f.WriteAt(wbuf[total:], int64(total))
		require.NoError(b, err)
		total += n
	}

	offset := int64(0)
	buf := aligned(int(size))

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
			off := atomic.AddInt64(&offset, size) - size
			n, err := f.ReadAt(buf, off)
			if err != nil {
				b.Error(err)
			}
			if n != int(size) {
				b.Errorf("short read %v off %v", n, off)
			}
		}()
	}
	wg.Wait()
}

func TestEmptyWrite(t *testing.T) {
	queue, err := queue.Setup(1024, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { queue.Close() })

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

func TestClose(t *testing.T) {
	queue, err := queue.Setup(8, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { queue.Close() })

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "test-concurrent-writes", 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(f.Name())
	})
	require.NoError(t, f.Close())
	buf := []byte{1, 2}
	_, err = f.WriteAt(buf, 0)
	require.Equal(t, syscall.EBADF, err)
}

func TestConcurrentWritesIntegrity(t *testing.T) {
	queue, err := queue.Setup(1024, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { queue.Close() })

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
