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
	pl, err := newPoll(1)
	require.NoError(t, err)
	defer pl.close()

	require.NoError(t, ring.SetupEventfd())
	defer ring.CloseEventfd()
	require.NoError(t, pl.addRead(int32(ring.Eventfd())))

	for i := uint64(1); i < 100; i++ {
		for j := 0; j < 3; j++ {
			sqe := ring.GetSQEntry()
			uring.Nop(sqe)
			sqe.SetUserData(i)
		}
		_, err = ring.Submit(0)
		require.NoError(t, err)
		require.NoError(t, pl.wait(func(efd int32) {
			require.Equal(t, int32(ring.Eventfd()), efd)
		}))

		for j := 0; j < 3; j++ {
			cqe, err := ring.GetCQEntry(0)
			require.NoError(t, err)
			require.Equal(t, i, cqe.UserData())
		}
	}
}
