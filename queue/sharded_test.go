package queue

import (
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

func TestSharded(t *testing.T) {
	rings := make([]*uring.Ring, 8)
	var err error
	for i := range rings {
		rings[i], err = uring.Setup(64, nil)
		require.NoError(t, err)
		defer rings[i].Close()
	}
	queue := NewSharded(rings...)
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

func BenchmarkParallelSharded(b *testing.B) {
	rings := make([]*uring.Ring, 8)
	var err error
	for i := range rings {
		rings[i], err = uring.Setup(1024, nil)
		require.NoError(b, err)
		defer rings[i].Close()
	}
	queue := NewSharded(rings...)
	defer queue.Close()

	f, err := ioutil.TempFile("", "test")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	var size uint64 = 4096
	data := make([]byte, size)
	vector := []syscall.Iovec{
		{
			Base: &data[0],
			Len:  size,
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		completions := make([]*request, 0, b.N)
		for pb.Next() {
			var sqe uring.SQEntry
			uring.Writev(&sqe, f.Fd(), vector, 0, 0)
			req, err := queue.CompleteAsync(sqe)
			if err != nil {
				b.Error(err)
			}
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
