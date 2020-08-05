package queue

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
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

func benchmarkWrite(b *testing.B, size uint64, n int) {
	ring, err := uring.Setup(1024, nil)
	require.NoError(b, err)
	defer ring.Close()
	queue := New(ring)
	defer queue.Close()

	f, err := ioutil.TempFile("", "test")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	data := make([]byte, size)
	vector := []syscall.Iovec{
		{
			Base: &data[0],
			Len:  size,
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				var sqe uring.SQEntry
				uring.Writev(&sqe, f.Fd(), vector, uint64(i)*size, 0)
				cqe, err := queue.Complete(sqe)
				if err != nil {
					b.Error(err)
				}
				if cqe.Result() < 0 {
					b.Errorf("failed with %v", syscall.Errno(-cqe.Result()))
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkWrite(b *testing.B) {
	for _, w := range []int{1, 2, 4, 8, 16, 32, 512, 1024, 2048} {
		for _, size := range []uint64{4 << 10, 1 << 20, 10 << 20} {
			w := w
			size := size
			b.Run(fmt.Sprintf("w%d_%d", w, size), func(b *testing.B) {
				benchmarkWrite(b, size, w)
			})
		}
	}
}

func benchmarkOSWrite(b *testing.B, size int64, n int) {
	f, err := ioutil.TempFile("", "test")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	data := make([]byte, size)

	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				_, err := f.WriteAt(data, int64(i)*size)
				if err != nil {
					b.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkOSWrite(b *testing.B) {
	for _, w := range []int{1, 2, 4, 8, 16, 32, 512, 1024, 2048} {
		for _, size := range []int64{4 << 10, 1 << 20, 10 << 20} {
			w := w
			size := size
			b.Run(fmt.Sprintf("w%d_%d", w, size), func(b *testing.B) {
				benchmarkOSWrite(b, size, w)
			})
		}
	}
}

func BenchmarkSingleWriter(b *testing.B) {
	ring, err := uring.Setup(1024, nil)
	require.NoError(b, err)
	defer ring.Close()
	queue := New(ring)
	defer queue.Close()

	f, err := ioutil.TempFile("", "test")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	size := uint64(4096)
	data := make([]byte, size)
	vector := []syscall.Iovec{
		{
			Base: &data[0],
			Len:  size,
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	requests := make([]*request, b.N)
	for i := 0; i < b.N; i++ {
		var sqe uring.SQEntry
		uring.Writev(&sqe, f.Fd(), vector, uint64(i)*size, 0)
		req, err := queue.CompleteAsync(sqe)
		if err != nil {
			b.Error(err)
		}
		requests[i] = req
	}
	for _, req := range requests {
		<-req.Wait()
		if req.Result() < 0 {
			b.Errorf("failed with %v", req.Result())
		}
		req.Dispose()
	}
}
