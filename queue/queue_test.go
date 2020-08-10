package queue

import (
	"encoding/binary"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/dshulyak/uring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"net/http"
	_ "net/http/pprof"
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

func BenchmarkParallelOS(b *testing.B) {
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
			_, err := f.WriteAt(data, atomic.LoadInt64(&offset))
			if err != nil {
				b.Error(err)
			}
			atomic.AddInt64(&offset, size)
		}
	})
}

func BenchmarkParallelQueue(b *testing.B) {
	ring, err := uring.Setup(1024, &uring.IOUringParams{
		CQEntries: 8 * 1024,
		Flags:     uring.IORING_SETUP_CQSIZE,
	})
	require.NoError(b, err)
	defer ring.Close()
	queue := New(ring)
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
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cqe, err := queue.Complete(func(sqe *uring.SQEntry) {
				uring.Writev(sqe, f.Fd(), vector, atomic.LoadUint64(&offset), 0)
			})
			if err != nil {
				b.Error(err)
			}
			atomic.AddUint64(&offset, size)
			if cqe.Result() < 0 {
				b.Errorf("failed with %v", syscall.Errno(-cqe.Result()))
			}
		}
	})
}

var running = false

func TestConcurrentWrites(t *testing.T) {
	go func() {
		if !running {
			log.Println(http.ListenAndServe("localhost:6060", nil))
			running = true
		}
	}()
	q, err := SetupSharded(8, 4096, &uring.IOUringParams{
		Flags:     uring.IORING_SETUP_CQSIZE,
		CQEntries: 8 * 4096,
	})
	require.NoError(t, err)
	defer q.Close()

	f, err := ioutil.TempFile("", "test-concurrent-writes-")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	var wg sync.WaitGroup
	var n int64 = 10000
	// just to ensure that buffer is in the same place on the stack
	//buf := make([]byte, n*8)

	// why it works if vectors are only declared here?
	vectors := make([]syscall.Iovec, n)
	for i := range vectors {
		vectors[i] = syscall.Iovec{Len: 8}
	}
	for i := int64(0); i < n; i++ {
		wg.Add(1)
		go func(i uint64) {
			buf1 := make([]byte, 8)
			vectors[i].Base = &buf1[0]
			binary.BigEndian.PutUint64(buf1[:], uint64(i))
			_, _ = q.Complete(func(sqe *uring.SQEntry) {
				uring.Writev(sqe, f.Fd(), vectors[i:i+1], i*8, 0)
			})
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
