package log

import (
	"io"
	"os"

	"log_system/config/appconfig"

	"github.com/tysonmote/gommap"
)

const (
	offsetWidth   uint64 = 4
	positionWidth uint64 = 8
	entWidth             = offsetWidth + positionWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c *appconfig.AppConfig) (*index, error) {
	idx := &index{
		file: f,
	}

	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fileInfo.Size())

	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

func (idx *index) Read(in int64) (out uint32, pos uint64, err error) {
	if idx.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((idx.size / entWidth) - 1) // last offset
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if idx.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(idx.mmap[pos : pos+offsetWidth])
	pos = enc.Uint64(idx.mmap[pos+offsetWidth : pos+entWidth])

	return out, pos, nil
}

func (idx *index) Write(off uint32, pos uint64) error {
	if uint64(len(idx.mmap)) < idx.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(idx.mmap[idx.size:idx.size+offsetWidth], off)
	enc.PutUint64(idx.mmap[idx.size+offsetWidth:idx.size+entWidth], pos)

	idx.size += entWidth

	return nil
}

func (idx *index) Close() error {
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := idx.file.Sync(); err != nil {
		return err
	}

	if err := idx.file.Truncate(int64(idx.size)); err != nil {
		return err
	}

	return idx.file.Close()
}

func (idx *index) Name() string {
	return idx.file.Name()
}
