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
				if !assert.NoError(t, err) {
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
	require.Len(t, data, 10000)
	for i := range data {
		require.Equal(t, i, int(data[i]))
	}
}

func BenchmarkParallelSharded(b *testing.B) {
	rings := make([]*uring.Ring, 8)
	var err error
	for i := range rings {
		var params *uring.IOUringParams
		if i > 0 {
			params = &uring.IOUringParams{
				Flags: uring.IORING_SETUP_ATTACH_WQ,
				WQFd:  uint32(rings[0].Fd()),
			}
		}
		rings[i], err = uring.Setup(1024, params)
		require.NoError(b, err)
		defer rings[i].Close()
	}
	queue := NewSharded(rings...)
	defer queue.Close()

	f, err := ioutil.TempFile("", "test")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	var size uint64 = 256 << 10
	data := make([]byte, size)
	vector := []syscall.Iovec{
		{
			Base: &data[0],
			Len:  size,
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(int64(size))

	offset := uint64(0)
	b.RunParallel(func(pb *testing.PB) {
		var sqe uring.SQEntry
		for pb.Next() {
			uring.Writev(&sqe, f.Fd(), vector, atomic.LoadUint64(&offset), 0)
			cqe, err := queue.Complete(sqe)
			if err != nil {
				b.Error(err)
			}
			if cqe.Result() < 0 {
				b.Errorf("failed with %v", syscall.Errno(-cqe.Result()))
			}

			atomic.AddUint64(&offset, size)
		}
	})
}
