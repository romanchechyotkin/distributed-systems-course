package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	pb "log_system/api/v1"
	"log_system/config/appconfig"
)

type Log struct {
	mu sync.RWMutex

	Dir    string
	Config *appconfig.AppConfig

	activeSegment *segment
	segments      []*segment
}

func New(dir string, cfg *appconfig.AppConfig) (*Log, error) {
	if cfg.Segment.MaxStoreBytes == 0 {
		cfg.Segment.MaxStoreBytes = 1024
	}

	if cfg.Segment.MaxIndexBytes == 0 {
		cfg.Segment.MaxIndexBytes = 1024
	}

	l := &Log{
		Dir:    dir,
		Config: cfg,
	}

	return l, l.setup()
}

func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for _, offset := range baseOffsets {
		if err = l.newSegment(offset); err != nil {
			return err
		}
	}

	if l.segments == nil {
		if err = l.newSegment(uint64(l.Config.Segment.InitialOffset)); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Append(record *pb.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}

	return 0, err
}

func (l *Log) Read(off uint64) (*pb.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment

	for _, seg := range l.segments {
		if seg.baseOffset <= off && off < seg.nextOffset {
			s = seg
			break
		}
	}

	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	return s.Read(off)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, seg := range l.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}

	return off - 1, nil
}

func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment

	for _, seg := range l.segments {
		if seg.nextOffset < lowest+1 {
			if err := seg.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, seg)
	}

	l.segments = segments
	return nil
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, 0, len(l.segments))
	for _, seg := range l.segments {
		readers = append(readers, &originReader{
			store: seg.store,
			off:   0,
		})
	}

	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off uint64
}

func (or *originReader) Read(p []byte) (int, error) {
	n, err := or.store.ReadAt(p, int64(or.off))
	or.off += uint64(n)

	return n, err
}

func (l *Log) newSegment(off uint64) error {
	seg, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, seg)
	l.activeSegment = seg

	return nil
}
