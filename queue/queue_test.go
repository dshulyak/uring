package queue

import (
	"io/ioutil"
	"os"
	"syscall"
	"testing"

	"github.com/dshulyak/uring"
	"github.com/stretchr/testify/require"
)

func BenchmarkWrite(b *testing.B) {
	ring, err := uring.Setup(32, nil)
	require.NoError(b, err)
	defer ring.Close()
	queue := New(ring)
	defer queue.Close()

	f, err := ioutil.TempFile("", "test")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	size := uint64(10 << 20)
	data := make([]byte, size)

	vector := []syscall.Iovec{
		{
			Base: &data[0],
			Len:  size,
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < 8; i++ {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				var sqe uring.SQEntry
				uring.Writev(&sqe, f.Fd(), vector, 0, 0)
				cqe, err := queue.Complete(sqe)
				if err != nil {
					b.Error(err)
				}
				if cqe.Result() < 0 {
					b.Errorf("failed with %v", syscall.Errno(-cqe.Result()))
				}
			}
		})
	}
}

func BenchmarkOSWrite(b *testing.B) {
	f, err := ioutil.TempFile("", "test")
	require.NoError(b, err)
	defer os.Remove(f.Name())

	size := uint64(10 << 20)
	data := make([]byte, size)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < 8; i++ {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := f.WriteAt(data, 0)
				if err != nil {
					b.Error(err)
				}
			}
		})
	}
}
