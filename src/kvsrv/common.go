package kvsrv

// PutAppendArgs request args for Put or Append methods
type PutAppendArgs struct {
	Key   string
	Value string

	UniqueName string
}

// PutAppendReply reply args for Put or Append methods
type PutAppendReply struct {
	Value string
}

// GetArgs request args for Get method
type GetArgs struct {
	Key string

	UniqueName string
}

// GetReply reply args for Get method
type GetReply struct {
	Value string
}
