package fs

import (
	"encoding/binary"
	"fmt"
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
	queue, err := queue.Setup(1024, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { queue.Close() })

	fsm := NewFilesystem(queue)

	f, err := TempFile(fsm, "testing-fs-file-", 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(f.Name())
	})

	pool, err := fixed.New(queue, 4, 2)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })

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
	b.Cleanup(func() {
		os.Remove(f.Name())
	})

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

func BenchmarkWriteAt(b *testing.B) {
	for _, size := range []int64{8 << 10, 256 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("uring sharded rr dsync %d", size), func(b *testing.B) {
			q, err := queue.Setup(
				128,
				&uring.IOUringParams{
					CQEntries: 4096,
					Flags:     uring.IORING_SETUP_CQSIZE,
				},
				&queue.Params{
					// in this case 50 is the number of workers used by uring
					Shards:           50,
					ShardingStrategy: queue.ShardingRoundRobin,
					WaitMethod:       queue.WaitEventfd,
				},
			)
			require.NoError(b, err)
			benchmarkWriteAt(b, q, size, unix.O_DSYNC)
		})
		b.Run(fmt.Sprintf("uring sharded default %d", size), func(b *testing.B) {
			q, err := queue.Setup(
				128,
				&uring.IOUringParams{
					CQEntries: 4096,
					Flags:     uring.IORING_SETUP_CQSIZE,
				},
				nil,
			)
			require.NoError(b, err)
			benchmarkWriteAt(b, q, size, 0)
		})
		b.Run(fmt.Sprintf("uring enter %d", size), func(b *testing.B) {
			q, err := queue.Setup(
				4096,
				&uring.IOUringParams{
					CQEntries: 4 * 4096,
					Flags:     uring.IORING_SETUP_CQSIZE,
				},
				&queue.Params{
					WaitMethod: queue.WaitEnter,
				},
			)
			require.NoError(b, err)
			benchmarkWriteAt(b, q, size, 0)
		})
		b.Run(fmt.Sprintf("uring poll %d", size), func(b *testing.B) {
			q, err := queue.Setup(
				4096,
				&uring.IOUringParams{
					CQEntries: 4 * 4096,
					Flags:     uring.IORING_SETUP_CQSIZE,
				},
				&queue.Params{
					WaitMethod: queue.WaitPoll,
				},
			)
			require.NoError(b, err)
			benchmarkWriteAt(b, q, size, 0)
		})
		b.Run(fmt.Sprintf("os dsync %d", size), func(b *testing.B) {
			benchmarkOSWriteAt(b, size, unix.O_DSYNC)
		})
		b.Run(fmt.Sprintf("os %d", size), func(b *testing.B) {
			benchmarkOSWriteAt(b, size, 0)
		})
	}
}

func BenchmarkReadAt(b *testing.B) {
	for _, size := range []int64{8 << 10, 256 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("uring sharded rr %d", size), func(b *testing.B) {
			q, err := queue.Setup(
				128,
				&uring.IOUringParams{
					CQEntries: 4096,
					Flags:     uring.IORING_SETUP_CQSIZE,
				},
				&queue.Params{
					// in this case 50 is the number of workers created by uring
					Shards:           50,
					ShardingStrategy: queue.ShardingRoundRobin,
					WaitMethod:       queue.WaitEventfd,
				},
			)
			require.NoError(b, err)
			benchmarkReadAt(b, q, size)
		})
		b.Run(fmt.Sprintf("uring sharded default %d", size), func(b *testing.B) {
			q, err := queue.Setup(
				128,
				&uring.IOUringParams{
					CQEntries: 4096,
					Flags:     uring.IORING_SETUP_CQSIZE,
				},
				nil,
			)
			require.NoError(b, err)
			benchmarkReadAt(b, q, size)
		})
		b.Run(fmt.Sprintf("uring enter %d", size), func(b *testing.B) {
			q, err := queue.Setup(
				4096,
				&uring.IOUringParams{
					CQEntries: 4 * 4096,
					Flags:     uring.IORING_SETUP_CQSIZE,
				},
				&queue.Params{
					WaitMethod: queue.WaitEnter,
				},
			)
			require.NoError(b, err)
			benchmarkReadAt(b, q, size)
		})
		b.Run(fmt.Sprintf("uring poll %d", size), func(b *testing.B) {
			q, err := queue.Setup(
				4096,
				&uring.IOUringParams{
					CQEntries: 4 * 4096,
					Flags:     uring.IORING_SETUP_CQSIZE,
				},
				&queue.Params{
					WaitMethod: queue.WaitPoll,
				},
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

	f, err := TempFile(fsm, "testing-fs-file-", 0)
	require.NoError(b, err)
	b.Cleanup(func() {
		os.Remove(f.Name())
	})

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
