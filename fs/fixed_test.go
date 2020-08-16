package fs

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/dshulyak/uring/queue"
	"github.com/stretchr/testify/require"
)

func makeFiles(tb testing.TB, n int) []*os.File {
	tb.Helper()
	files := make([]*os.File, n)
	for i := range files {
		f, err := ioutil.TempFile("", "testing-fixed-")
		require.NoError(tb, err)
		tb.Cleanup(func() {
			os.Remove(f.Name())
		})
		files[i] = f
	}
	return files
}

func TestFixedFiles(t *testing.T) {
	t.Skip("UnregisterFiles fails unpredictably")
	queue, err := queue.Setup(32, nil, nil)
	require.NoError(t, err)
	defer queue.Close()

	ff := newFixedFiles(queue, 10)
	files := makeFiles(t, 10)

	for i, f := range files {
		idx, err := ff.register(f.Fd())
		require.NoError(t, err)
		require.Equal(t, i, int(idx))
	}
	for i := range files {
		require.NoError(t, ff.unregister(uintptr(i)))
	}
	for i, f := range files {
		idx, err := ff.register(f.Fd())
		require.NoError(t, err)
		require.Equal(t, i, int(idx))
	}
	files2 := makeFiles(t, 5)
	for i, f := range files2 {
		idx, err := ff.register(f.Fd())
		require.NoError(t, err)
		require.Equal(t, i+len(files), int(idx))
	}

}
