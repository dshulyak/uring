package queue

import (
	"sort"
	"sync"
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
