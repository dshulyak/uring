package queue

import (
	"io/ioutil"
	"os"
	"syscall"
	"testing"

	"github.com/dshulyak/uring"
	"github.com/stretchr/testify/require"
)

func BenchmarkSWQueue(b *testing.B) {
	ring, err := uring.Setup(1024, nil)
	require.NoError(b, err)
	defer ring.Close()
	queue := NewSWQueue(ring)
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
