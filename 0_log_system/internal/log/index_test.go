package log

import (
	"context"
	"io"
	"os"
	"testing"

	"log_system/config/appconfig"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(".", "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	cfg, err := appconfig.LoadFromPath(context.Background(), "../../pkl/dev/config.pkl")
	require.NoError(t, err)
	require.Equal(t, uint32(1024), cfg.Segment.MaxIndexBytes)

	idx, err := newIndex(f, cfg)
	require.NoError(t, err)
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		offset uint32
		pos    uint64
	}{
		{
			offset: 0,
			pos:    0,
		},
		{
			offset: 1,
			pos:    10,
		},
	}

	for _, entry := range entries {
		err = idx.Write(entry.offset, entry.pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(entry.offset))
		require.NoError(t, err)
		require.Equal(t, entry.pos, pos)
	}

	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)

	_ = idx.Close()

	f, err = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	require.NoError(t, err)

	idx, err = newIndex(f, cfg)
	require.NoError(t, err)
	require.Equal(t, f.Name(), idx.Name())

	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].pos, pos)
}
