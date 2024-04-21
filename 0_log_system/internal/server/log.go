package server

import (
	"sync"
	"time"
)

type Log struct {
	mu      sync.Mutex
	records []*Record
}

func NewLog() *Log {
	return &Log{}
}

func (l *Log) Append(record *Record) (uint64, error) {
	l.mu.Lock()

	record.Offset = uint64(len(l.records))
	record.Timestamp = time.Now()
	l.records = append(l.records, record)

	l.mu.Unlock()

	return record.Offset, nil
}

func (l *Log) Read(offset uint64) (*Record, error) {
	l.mu.Lock()

	if offset >= uint64(len(l.records)) {
		return nil, ErrOffsetNotFound
	}

	l.mu.Unlock()

	return l.records[offset], nil
}

type Record struct {
	Value     []byte    `json:"value"`
	Offset    uint64    `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}
