package kvsrv

import (
	"fmt"
	mrand "math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

// Clerk is RPC client
type Clerk struct {
	id        int64
	reqNumber uint64

	server *labrpc.ClientEnd
	mu     sync.Mutex
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		server: server,
		id:     atomic.AddInt64(&clientID, 1),
	}

	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	reqNumber := atomic.AddUint64(&ck.reqNumber, 1)
	ck.reqNumber = reqNumber

	arg := &GetArgs{
		Key:       key,
		ClerkID:   ck.id,
		ReqNumber: reqNumber,
	}
	reply := &GetReply{}

	fmt.Printf("client %d; request number %d; GET key %s\n", ck.id, reqNumber, key)

	for {
		ok := ck.server.Call("KVServer.Get",
			arg,
			reply,
		)
		if ok && (reply.Error == ErrNoKey || reply.Error == OK) {
			return reply.Value
		}
	}
}

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	method := fmt.Sprintf("KVServer.%s", op)
	reqNumber := atomic.AddUint64(&ck.reqNumber, 1)
	ck.reqNumber = reqNumber

	arg := &PutAppendArgs{
		Key:       key,
		Value:     value,
		ClerkID:   ck.id,
		Op:        op,
		ReqNumber: reqNumber,
	}
	reply := &PutAppendReply{}

	fmt.Printf("client %d; request number %d; %s key [%s] value [%s]\n", ck.id, reqNumber, op, key, value)

	for {
		ok := ck.server.Call(method,
			arg,
			reply,
		)
		if ok && (reply.Error == ErrNoKey || reply.Error == OK) {
			return reply.Value
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

func uniqueName() string {
	str := "abcdefghijklmnopqrstuvwxyz"

	mrand.NewSource(time.Now().Unix())
	var n int
	var res string
	for range 10 {
		n = mrand.Intn(26)
		res += string(str[n])
	}

	return res + strconv.Itoa(time.Now().Nanosecond())
}
