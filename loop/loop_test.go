package loop

import (
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/dshulyak/uring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestLoop(t *testing.T) {
	tester := func(t *testing.T, q *Loop) {
		t.Cleanup(func() {
			q.Close()
		})
		var wg sync.WaitGroup
		results := make(chan uring.CQEntry, 10000)
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					cqe, err := q.Syscall(func(sqe *uring.SQEntry) {
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
		count := 0
		for _ = range results {
			count++
		}
		require.Equal(t, count, 10000)
	}

	t.Run("default", func(t *testing.T) {
		q, err := Setup(1024, nil, nil)
		require.NoError(t, err)
		tester(t, q)
	})
	t.Run("simple poll", func(t *testing.T) {
		q, err := Setup(1024, nil, &Params{
			WaitMethod: WaitPoll,
		})
		require.NoError(t, err)
		tester(t, q)
	})
	t.Run("simple enter", func(t *testing.T) {
		q, err := Setup(1024, nil, &Params{
			WaitMethod: WaitEnter,
		})
		require.NoError(t, err)
		tester(t, q)
	})
	t.Run("simple eventfd", func(t *testing.T) {
		q, err := Setup(1024, nil, &Params{
			Rings:      1,
			WaitMethod: WaitEventfd,
		})
		require.NoError(t, err)
		tester(t, q)
	})
	t.Run("sharded enter", func(t *testing.T) {
		q, err := Setup(1024, nil, &Params{
			Rings:      runtime.NumCPU(),
			WaitMethod: WaitEnter,
		})
		require.NoError(t, err)
		tester(t, q)
	})
}

func TestBatch(t *testing.T) {
	tester := func(t *testing.T, q *Loop) {
		t.Cleanup(func() {
			q.Close()
		})
		iter := 10000
		size := 4
		var wg sync.WaitGroup
		results := make(chan uring.CQEntry, iter*size)
		for i := 0; i < iter; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				batch := make([]SQOperation, size)
				for i := range batch {
					batch[i] = uring.Nop
				}
				cqes, err := q.BatchSyscall(nil, batch)
				if !assert.NoError(t, err) {
					return
				}
				for _, cqe := range cqes {
					results <- cqe
				}
			}()
		}
		wg.Wait()
		close(results)
		count := 0
		for _ = range results {
			count++
		}
		require.Equal(t, count, iter*size)
	}

	t.Run("default", func(t *testing.T) {
		q, err := Setup(1024, nil, nil)
		require.NoError(t, err)
		tester(t, q)
	})
	t.Run("sharded enter", func(t *testing.T) {
		q, err := Setup(1024, nil, &Params{
			Rings:      runtime.NumCPU(),
			WaitMethod: WaitEnter,
		})
		require.NoError(t, err)
		tester(t, q)
	})
}

func BenchmarkLoop(b *testing.B) {
	bench := func(b *testing.B, q *Loop) {
		b.Cleanup(func() { q.Close() })
		var wg sync.WaitGroup
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := q.Syscall(func(sqe *uring.SQEntry) {
					uring.Nop(sqe)
				})
				if err != nil {
					b.Error(err)
				}
			}()
		}
		wg.Wait()
	}
	b.Run("sharded default", func(b *testing.B) {
		q, err := Setup(128, &uring.IOUringParams{
			CQEntries: 2 * 4096,
			Flags:     uring.IORING_SETUP_CQSIZE,
		}, nil)
		require.NoError(b, err)
		bench(b, q)
	})
	b.Run("sharded enter", func(b *testing.B) {
		q, err := Setup(128, &uring.IOUringParams{
			CQEntries: 2 * 4096,
			Flags:     uring.IORING_SETUP_CQSIZE,
		}, &Params{
			Rings:      runtime.NumCPU(),
			WaitMethod: WaitEnter,
			Flags:      FlagSharedWorkers,
		})
		require.NoError(b, err)
		bench(b, q)
	})
}

func BenchmarkBatch(b *testing.B) {
	bench := func(b *testing.B, q *Loop, size int) {
		b.Cleanup(func() { q.Close() })
		var wg sync.WaitGroup
		b.ResetTimer()

		for i := 0; i < b.N/size; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cqes := make([]uring.CQEntry, 0, size)
				batch := make([]SQOperation, size)
				for i := range batch {
					batch[i] = uring.Nop
				}
				_, err := q.BatchSyscall(cqes, batch)
				if err != nil {
					b.Error(err)
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			cqes := make([]uring.CQEntry, 0, b.N%size)
			batch := make([]SQOperation, b.N%size)
			for i := range batch {
				batch[i] = uring.Nop
			}
			_, err := q.BatchSyscall(cqes, batch)
			if err != nil {
				b.Error(err)
			}
		}()
		wg.Wait()
	}
	b.Run("default 16", func(b *testing.B) {
		q, err := Setup(128, &uring.IOUringParams{
			CQEntries: 2 * 4096,
			Flags:     uring.IORING_SETUP_CQSIZE,
		}, nil)
		require.NoError(b, err)
		bench(b, q, 16)
	})
}

func TestTimeoutNoOverwrite(t *testing.T) {
	q, err := Setup(2, nil, &Params{Rings: 1, WaitMethod: WaitEventfd})
	require.NoError(t, err)
	t.Cleanup(func() { q.Close() })
	// we are testing here that the result we used for timeout will not be overwritten by nop.
	// timeout operation executes long enough (10ms), for results array to wrap around
	tchan := make(chan struct{})
	go func() {
		ts := unix.Timespec{Nsec: 10_000_000}
		cqe, err := q.Syscall(func(sqe *uring.SQEntry) {
			uring.Timeout(sqe, &ts, false, 0)
		}, uintptr(unsafe.Pointer(&ts)))
		require.NoError(t, err)
		require.Equal(t, syscall.ETIME, syscall.Errno(-cqe.Result()))
		close(tchan)
	}()
	for i := 0; i < 100; i++ {
		_, err := q.Syscall(uring.Nop)
		require.NoError(t, err)
	}
	select {
	case <-tchan:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timed out")
	}
}

func TestLinkedBatch(t *testing.T) {
	q, err := Setup(64, nil, &Params{Rings: 1, WaitMethod: WaitEventfd})
	require.NoError(t, err)
	t.Cleanup(func() { q.Close() })

	result := make(chan []uring.CQEntry)
	go func() {
		wait := unix.Timespec{Sec: 10}
		timeout := unix.Timespec{Nsec: 10_000}
		cqes, err := q.BatchSyscall(nil, []SQOperation{
			func(sqe *uring.SQEntry) {
				uring.Timeout(sqe, &wait, false, 0)
				sqe.SetFlags(uring.IOSQE_IO_LINK)
			},
			func(sqe *uring.SQEntry) {
				uring.LinkTimeout(sqe, &timeout, false)
			},
		}, uintptr(unsafe.Pointer(&wait)), uintptr(unsafe.Pointer(&timeout)))
		require.NoError(t, err)
		result <- cqes
	}()
	select {
	case <-time.After(time.Second):
		require.FailNow(t, "failed to interrupt waiter")
	case cqes := <-result:
		assert.Equal(t, syscall.ECANCELED.Error(), syscall.Errno(-cqes[0].Result()).Error())
		assert.Equal(t, syscall.ETIME.Error(), syscall.Errno(-cqes[1].Result()).Error())
	}
}
