package queue

import (
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"unsafe"

	"github.com/dshulyak/uring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSharded(t *testing.T) {
	queue, err := SetupSharded(1, 1024, nil)
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

func TestRegisterBuffers(t *testing.T) {
	queue, err := SetupSharded(8, 1024, nil)
	defer queue.Close()

	f, err := ioutil.TempFile("", "test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	var size uint64 = 4 << 10
	data := make([]byte, size)
	iovec := []syscall.Iovec{
		{
			Base: &data[0],
			Len:  size,
		},
	}
	require.NoError(t, queue.RegisterBuffers(unsafe.Pointer(&iovec[0]), 1))

	require.NoError(t, queue.CompleteAll(func(sqe *uring.SQEntry) {
		uring.WriteFixed(sqe, f.Fd(), iovec[0].Base, iovec[0].Len, 0, 0, 0)
	}, func(cqe uring.CQEntry) {
		require.Equal(t, int32(size), cqe.Result(), syscall.Errno(-cqe.Result()))
	}))
}

func BenchmarkParallelSharded(b *testing.B) {
	queue, err := SetupSharded(8, 1024, nil)
	defer queue.Close()

	f, err := ioutil.TempFile("", "test")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	var size uint64 = 8 << 10
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
		for pb.Next() {
			cqe, err := queue.Complete(func(sqe *uring.SQEntry) {
				uring.Writev(sqe, f.Fd(), vector, atomic.AddUint64(&offset, size)-size, 0)
			})
			if err != nil {
				b.Error(err)
			}
			if cqe.Result() < 0 {
				b.Errorf("failed with %v", syscall.Errno(-cqe.Result()))
			}
		}
	})
}
