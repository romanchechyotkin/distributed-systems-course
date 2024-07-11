package kvsrv

var clientID int64

type Error string

const (
	ErrNoKey Error = "no such key"
	OK       Error = "ok"
)

// PutAppendArgs request args for Put or Append methods
type PutAppendArgs struct {
	ClerkID   int64
	ReqNumber uint64
	Op        string // Op whether Put or Append

	Key   string
	Value string
}

// PutAppendReply reply args for Put or Append methods
type PutAppendReply struct {
	Value string

	Error Error
}

// GetArgs request args for Get method
type GetArgs struct {
	ClerkID   int64
	ReqNumber uint64

	Key string
}

// GetReply reply args for Get method
type GetReply struct {
	Value string

	Error Error
}
