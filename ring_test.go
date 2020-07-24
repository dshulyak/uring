package uring

import (
	"io/ioutil"
	"math/rand"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWritev(t *testing.T) {
	f, err := ioutil.TempFile("", "writev-tests-")
	require.NoError(t, err)
	defer f.Close()

	ring, err := Setup(4, nil)
	require.NoError(t, err)
	defer ring.Close()

	var offset uint64
	sqes := [4]SQEntry{}
	bufs := [4][8]byte{}
	vectors := [4][]syscall.Iovec{}

	for round := 0; round < 10; round++ {
		for i := range sqes {
			buf := bufs[i]
			_, _ = rand.Read(buf[:])
			bufs[i] = buf
			vectors[i] = []syscall.Iovec{
				{
					Base: &buf[0],
					Len:  uint64(len(buf)),
				},
			}
			Writev(&sqes[i], f.Fd(), vectors[i], offset, 0)
			offset += uint64(len(buf))
		}

		ring.Push(sqes[:]...)
		_, err = ring.Submit(4, 4)
		require.NoError(t, err)

		for i := 0; i < 4; i++ {
			cqe, err := ring.GetCQEntry(0)
			require.NoError(t, err)
			require.True(t, cqe.Result() >= 0, "failed with %v", syscall.Errno(-cqe.Result()))
		}

		buf := [8]byte{}
		for i := 0; i < 4; i++ {
			n, err := f.Read(buf[:])
			require.NoError(t, err)
			require.Equal(t, len(buf), n)
			require.Equal(t, bufs[i], buf)
		}
	}
}

func TestReadv(t *testing.T) {
	f, err := ioutil.TempFile("", "readv-tests-")
	require.NoError(t, err)
	defer f.Close()

	ring, err := Setup(4, nil)
	require.NoError(t, err)
	defer ring.Close()

	var offset uint64
	const num = 3
	sqes := [num]SQEntry{}
	bufs := [num][8]byte{}
	vectors := [num][]syscall.Iovec{}

	for round := 0; round < 10; round++ {

		wbuf := [num * 8]byte{}

		_, _ = rand.Read(wbuf[:])
		n, err := f.Write(wbuf[:])
		require.NoError(t, err)
		require.Equal(t, len(wbuf), n)

		for i := range sqes {
			vectors[i] = []syscall.Iovec{
				{
					Base: &bufs[i][0],
					Len:  uint64(len(bufs[i])),
				},
			}
			Readv(&sqes[i], f.Fd(), vectors[i], offset, 0)
			offset += uint64(len(bufs[i]))
		}

		ring.Push(sqes[:]...)
		_, err = ring.Submit(num, num)
		require.NoError(t, err)

		for i := 0; i < num; i++ {
			cqe, err := ring.GetCQEntry(0)
			require.NoError(t, err)
			require.Equal(t, len(bufs[i]), int(cqe.Result()), "failed with %v", syscall.Errno(-cqe.Result()))
			require.Equal(t, wbuf[i*8:(i+1)*8], bufs[i][:])
		}
	}
}

func TestCopy(t *testing.T) {
	from, err := ioutil.TempFile("", "copy-from-")
	require.NoError(t, err)
	defer from.Close()

	to, err := ioutil.TempFile("", "copy-to-")
	require.NoError(t, err)
	defer to.Close()

	ring, err := Setup(4, nil)
	require.NoError(t, err)
	defer ring.Close()

	buf := make([]byte, 4096)
	_, _ = rand.Read(buf)
	_, err = from.Write(buf)
	require.NoError(t, err)
	off, err := from.Seek(0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), off)

	reuse := [32]byte{}
	rlth := uint64(len(reuse))
	vector := []syscall.Iovec{
		{
			Base: &reuse[0],
			Len:  rlth,
		},
	}
	var (
		offset      uint64
		read, write SQEntry
	)
	for {
		// TODO Readv with IOCQE_IO_LINK makes Writev fail with 'file too large'
		// maybe this is not a valid sequence of sqe's
		Readv(&read, from.Fd(), vector, offset, 0)
		//read.SetFlags(IOSQE_IO_LINK)
		Writev(&write, to.Fd(), vector, offset, 0)

		ring.Push(read, write)
		_, err := ring.Submit(1, 1)
		require.NoError(t, err)
		rcqe, err := ring.GetCQEntry(0)
		require.NoError(t, err)
		require.True(t, rcqe.Result() >= 0, "%d ('%v')", rcqe.Result(), syscall.Errno(-rcqe.Result()))

		ret := rcqe.Result()
		if ret == 0 {
			break
		}

		_, err = ring.Submit(1, 1)
		require.NoError(t, err)
		wcqe, err := ring.GetCQEntry(0)
		require.NoError(t, err)
		require.Equal(t, ret, wcqe.Result(), "%d ('%v')", wcqe.Result(), syscall.Errno(-wcqe.Result()))

		offset += rlth
	}

	fromData, err := ioutil.ReadAll(from)
	toData, err := ioutil.ReadAll(to)
	require.NoError(t, err, "failed to read 'from'")
	require.NoError(t, err, "failed to read 'to'")
	require.Equal(t, len(fromData), len(toData))
	require.Equal(t, fromData, toData)
}

func TestRegisterProbe(t *testing.T) {
	// TODO any way to assert probe support?
	t.SkipNow()

	ring, err := Setup(4, nil)
	require.NoError(t, err)
	defer ring.Close()

	var probe Probe
	require.NoError(t, ring.RegisterProbe(&probe))
	require.True(t, probe.IsSupported(IORING_OP_NOP))
}

func TestNoEnter(t *testing.T) {
	ring, err := Setup(4, nil)
	require.NoError(t, err)
	defer ring.Close()

	var nop SQEntry
	Nop(&nop)
	ring.Push(nop)
	_, err = ring.Submit(1, 0)
	require.NoError(t, err)

	start := time.Now()
	for time.Since(start) < time.Second {
		_, err := ring.GetCQEntry(0)
		if err == nil {
			return
		}
	}
	require.FailNow(t, "nop operation wasn't completed")
}
