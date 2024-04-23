package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	pb "log_system/api/v1"
	"log_system/config/appconfig"
	"log_system/config/segmentconfig"

	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment_test")
	defer os.RemoveAll(dir)

	expectedRecord := &pb.Record{
		Value: []byte("hello world"),
	}

	cfg := &appconfig.AppConfig{
		Port: 8000,
		Segment: &segmentconfig.SegmentCofig{
			MaxIndexBytes: uint32(entWidth * 3),
			MaxStoreBytes: 1024,
			InitialOffset: 16,
		},
	}

	seg, err := newSegment(dir, 16, cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(16), seg.baseOffset, seg.nextOffset)
	require.False(t, seg.IsMaxed())

	for i := 0; i < 3; i++ {
		off, err := seg.Append(expectedRecord)
		require.NoError(t, err)
		require.Equal(t, uint64(16+i), off)

		record, err := seg.Read(off)
		require.NoError(t, err)
		require.Equal(t, expectedRecord.Value, record.Value)
	}

	_, err = seg.Append(expectedRecord)
	require.Equal(t, io.EOF, err)

	// maxed index
	require.True(t, seg.IsMaxed())
	cfg.Segment.MaxStoreBytes = uint32(len(expectedRecord.Value) * 3)
	cfg.Segment.MaxIndexBytes = 1024

	seg, err = newSegment(dir, 16, cfg)
	require.NoError(t, err)

	// maxed store
	require.True(t, seg.IsMaxed())

	err = seg.Remove()
	require.NoError(t, err)

	seg, err = newSegment(dir, 16, cfg)
	require.NoError(t, err)
	require.False(t, seg.IsMaxed())
}
