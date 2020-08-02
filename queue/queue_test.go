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
	/*
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		go func() {
			<-sig
			pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		}()
	*/
	ring, err := uring.Setup(2, nil)
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

// TODO move it to the file submodule once it exists
func BenchmarkWrite(b *testing.B) {
	b.Run("w32_4kb", func(b *testing.B) {
		benchmarkWrite(b, 4<<10, 32)
	})
	b.Run("w2_4kb", func(b *testing.B) {
		benchmarkWrite(b, 4<<10, 2)
	})
	b.Run("w32_10mb", func(b *testing.B) {
		benchmarkWrite(b, 10<<20, 32)
	})
	b.Run("w1_4kb", func(b *testing.B) {
		benchmarkWrite(b, 4<<10, 1)
	})
}

func BenchmarkOSWrite(b *testing.B) {
	f, err := ioutil.TempFile("", "test")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	size := uint64(10 << 20)
	data := make([]byte, size)

	b.ReportAllocs()
	b.ResetTimer()

	n := 32
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				_, err := f.WriteAt(data, 0)
				if err != nil {
					b.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}
