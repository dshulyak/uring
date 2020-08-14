package queue

import (
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/dshulyak/uring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComplete(t *testing.T) {
	queue, err := Setup(32, nil)
	require.NoError(t, err)
	defer queue.Close()

	var wg sync.WaitGroup
	results := make(chan uring.CQEntry, 10000)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				cqe, err := queue.Complete(func(sqe *uring.SQEntry) {
					uring.Nop(sqe)
				})
				if assert.NoError(t, err) {
					return
				}
				results <- cqe
			}
		}()
	}
	wg.Wait()
	close(results)
	data := make([]int, 0, 10000)
	for cqe := range results {
		data = append(data, int(cqe.UserData()))
	}
	sort.Ints(data)
	for i := range data {
		require.Equal(t, i, int(data[i]))
	}

}

func runConcurrent(b *testing.B, concurrency int, f func()) {
	n := b.N / concurrency
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				f()
			}
		}()
	}
	wg.Wait()
}

func BenchmarkWriteOS(b *testing.B) {
	f, err := ioutil.TempFile("", "test-os-")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	size := int64(256 << 10)
	data := make([]byte, size)

	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(size)

	offset := int64(0)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := f.WriteAt(data, atomic.AddInt64(&offset, size)-size)
			if err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkWriteQueue(b *testing.B) {
	queue, err := Setup(1024, &uring.IOUringParams{
		CQEntries: 8 * 1024,
		Flags:     uring.IORING_SETUP_CQSIZE,
	}, WAIT)
	require.NoError(b, err)
	defer queue.Close()

	f, err := ioutil.TempFile("", "test-queue-")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	size := uint64(256 << 10)
	data := make([]byte, size)
	vector := []syscall.Iovec{
		{
			Base: &data[0],
			Len:  size,
		},
	}

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	offset := uint64(0)

	runConcurrent(b, 10000, func() {
		cqe, err := queue.Complete(func(sqe *uring.SQEntry) {
			uring.Writev(sqe, f.Fd(), vector, atomic.AddUint64(&offset, size)-size, 0)
		})
		if err != nil {
			b.Error(err)
		}
		if cqe.Result() < 0 {
			b.Errorf("failed with %v", syscall.Errno(-cqe.Result()))
		}
	})
}
