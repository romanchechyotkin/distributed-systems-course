package log

import (
	"fmt"
	"os"
	"path"

	pb "log_system/api/v1"
	"log_system/config/appconfig"

	"google.golang.org/protobuf/proto"
)

type segment struct {
	index                  *index
	store                  *store
	cfg                    *appconfig.AppConfig
	baseOffset, nextOffset uint64
}

func newSegment(dir string, baseOffset uint64, cfg *appconfig.AppConfig) (*segment, error) {
	var err error

	s := &segment{
		cfg:        cfg,
		baseOffset: baseOffset,
	}

	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("\\%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("\\%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	if s.index, err = newIndex(indexFile, s.cfg); err != nil {
		return nil, err
	}

	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *segment) Append(record *pb.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur

	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	err = s.index.Write(uint32(s.nextOffset-s.baseOffset), pos)
	if err != nil {
		return 0, err
	}

	s.nextOffset++

	return cur, nil
}

func (s *segment) Read(off uint64) (*pb.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	record := &pb.Record{}
	err = proto.Unmarshal(p, record)
	if err != nil {
		return nil, err
	}

	return record, nil
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= uint64(s.cfg.Segment.MaxStoreBytes) || s.index.size >= uint64(s.cfg.Segment.MaxIndexBytes)
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.file.Name()); err != nil {
		return err
	}

	if err := os.Remove(s.store.file.Name()); err != nil {
		return err
	}

	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
