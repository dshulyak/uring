package queue

import (
	"runtime"
	"sync"
	"testing"

	"github.com/dshulyak/uring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueue(t *testing.T) {
	tester := func(t *testing.T, q *Queue) {
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
					cqe, err := q.Complete(func(sqe *uring.SQEntry) {
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
	t.Run("sharded enter", func(t *testing.T) {
		q, err := Setup(1024, nil, &Params{
			Shards:           uint(runtime.NumCPU()),
			ShardingStrategy: ShardingThreadID,
			WaitMethod:       WaitEnter,
		})
		require.NoError(t, err)
		tester(t, q)
	})
	t.Run("round robin", func(t *testing.T) {
		q, err := Setup(1024, nil, &Params{
			Shards:           6,
			WaitMethod:       WaitEventfd,
			ShardingStrategy: ShardingRoundRobin,
		})
		require.NoError(t, err)
		tester(t, q)
	})
	t.Run("round robin shared", func(t *testing.T) {
		q, err := Setup(1024, nil, &Params{
			Shards:           6,
			WaitMethod:       WaitEventfd,
			ShardingStrategy: ShardingRoundRobin,
			Flags:            FlagSharedWorkers,
		})
		require.NoError(t, err)
		tester(t, q)
	})
}
