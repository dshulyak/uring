package uring

import (
	"io/ioutil"
	"os"
	"syscall"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestRegisterProbe(t *testing.T) {
	ring, err := Setup(4, nil)
	require.NoError(t, err)
	defer ring.Close()

	var probe Probe
	require.NoError(t, ring.RegisterProbe(&probe))
	require.True(t, probe.IsSupported(IORING_OP_NOP))
}

func TestRegisterUpdateFiles(t *testing.T) {
	ring, err := Setup(4, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		ring.Close()
	})

	f1, err := ioutil.TempFile("", "testing-reg-update-")
	require.NoError(t, err)
	defer os.Remove(f1.Name())
	f2, err := ioutil.TempFile("", "testing-reg-update-")
	require.NoError(t, err)
	defer os.Remove(f1.Name())

	for r := 0; r < 10; r++ {
		fds := []int32{int32(f1.Fd()), -1}
		require.NoError(t, ring.RegisterFiles(fds))
		fds[1] = int32(f2.Fd())
		require.NoError(t, ring.UpdateFiles(fds, 0))
		require.NoError(t, ring.UnregisterFiles())
	}
}

func TestRegisterBuffers(t *testing.T) {
	ring, err := Setup(32, nil)
	require.NoError(t, err)
	t.Cleanup(func() { ring.Close() })

	n := 10
	buf := make([][]byte, n)
	iovec := make([]syscall.Iovec, n)
	for i := range iovec {
		buf[i] = make([]byte, 10)
		iovec[i] = syscall.Iovec{Base: &buf[i][0], Len: 10}
	}
	require.NoError(t, ring.RegisterBuffers(iovec))
	require.NoError(t, ring.UnregisterBuffers())
}

func TestSetupEventfd(t *testing.T) {
	ring, err := Setup(32, nil)
	require.NoError(t, err)
	t.Cleanup(func() { ring.Close() })

	require.NoError(t, ring.SetupEventfd())

	buf := [8]byte{}

	n := 10
	for i := 0; i < n; i++ {
		sqe := ring.GetSQEntry()
		Nop(sqe)
		_, err := ring.Submit(1)
		require.NoError(t, err)
	}
	rn, err := syscall.Read(int(ring.Eventfd()), buf[:])
	require.NoError(t, err)
	require.Equal(t, len(buf), rn)

	cnt := *(*uint64)(unsafe.Pointer(&buf))
	require.Equal(t, n, int(cnt))
	require.NoError(t, ring.CloseEventfd())
}
