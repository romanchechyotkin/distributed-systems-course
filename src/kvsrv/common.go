package kvsrv

// PutAppendArgs request args for Put or Append methods
type PutAppendArgs struct {
	Key   string
	Value string

	// TODO
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

// PutAppendReply reply args for Put or Append methods
type PutAppendReply struct {
	Value string
}

// GetArgs request args for Get method
type GetArgs struct {
	Key string

	// TODO
	// You'll have to add definitions here.
}

// GetReply reply args for Get method
type GetReply struct {
	Value string
}
