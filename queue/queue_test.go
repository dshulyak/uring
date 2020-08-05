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
	ring, err := uring.Setup(32, nil)
	require.NoError(t, err)
	defer ring.Close()
	queue := New(ring)
	defer queue.Close()

	var wg sync.WaitGroup
	results := make(chan uring.CQEntry, 10000)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				var sqe uring.SQEntry
				uring.Nop(&sqe)
				cqe, err := queue.Complete(sqe)
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

func BenchmarkParallelOS(b *testing.B) {
	f, err := ioutil.TempFile("", "test")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	size := int64(1 << 20)
	data := make([]byte, size)

	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(size)

	offset := int64(0)
	cnt := int64(0)
	b.RunParallel(func(pb *testing.PB) {
		atomic.AddInt64(&cnt, 1)
		for pb.Next() {
			_, err := f.WriteAt(data, atomic.LoadInt64(&offset))
			if err != nil {
				b.Error(err)
			}
			atomic.AddInt64(&offset, size)
		}
	})
}

func BenchmarkParallelQueue(b *testing.B) {
	ring, err := uring.Setup(1024, nil)
	require.NoError(b, err)
	defer ring.Close()
	queue := New(ring)
	defer queue.Close()

	f, err := ioutil.TempFile("", "test")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	size := uint64(1 << 20)
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
	b.RunParallel(func(pb *testing.PB) {
		completions := make([]*request, 0, b.N)
		for pb.Next() {
			var sqe uring.SQEntry
			uring.Writev(&sqe, f.Fd(), vector, atomic.LoadUint64(&offset), 0)
			req, err := queue.CompleteAsync(sqe)
			if err != nil {
				b.Error(err)
			}
			atomic.AddUint64(&offset, size)
			completions = append(completions, req)
		}
		for _, req := range completions {
			<-req.Wait()
			cqe := req.CQEntry
			req.Dispose()
			if cqe.Result() < 0 {
				b.Errorf("failed with %v", syscall.Errno(-cqe.Result()))
			}
		}
	})
}
