package queue

import (
	"testing"

	"github.com/dshulyak/uring"
	"github.com/stretchr/testify/require"
)

func TestPoll(t *testing.T) {
	ring, err := uring.Setup(64, nil)
	require.NoError(t, err)
	defer ring.Close()
	pl, err := newPoll(ring)
	require.NoError(t, err)
	defer pl.close()

	for i := uint64(1); i < 100; i++ {
		var sqe uring.SQEntry
		uring.Nop(&sqe)
		sqe.SetUserData(i)
		ring.Push(sqe)
		_, err = ring.Submit(1, 0)
		require.NoError(t, err)

		n, err := pl.wait()
		require.NoError(t, err)
		require.Equal(t, 1, n)

		cqe, err := ring.GetCQEntry(0)
		require.NoError(t, err)
		require.Equal(t, i, cqe.UserData())
	}
}
