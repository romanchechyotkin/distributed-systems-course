package kvsrv

import (
	"fmt"
	mrand "math/rand"
	"strconv"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd

	getFailChan    chan *GetArgs
	putFailChan    chan *PutAppendArgs
	appendFailChan chan *PutAppendArgs
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		server:         server,
		getFailChan:    make(chan *GetArgs),
		putFailChan:    make(chan *PutAppendArgs),
		appendFailChan: make(chan *PutAppendArgs),
	}

	go func() {
		for {
			select {
			case arg, ok := <-ck.getFailChan:
				if ok {
					continue
				}

				ck.Get(arg.Key)
			case arg, ok := <-ck.putFailChan:
				if ok {
					continue
				}

				ck.Put(arg.Key, arg.Value)

			case arg, ok := <-ck.appendFailChan:
				if ok {
					continue
				}

				ck.Append(arg.Key, arg.Value)
			}
		}
	}()

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
	reply := &GetReply{}
	arg := &GetArgs{Key: key, UniqueName: uniqueName()}

	if ok := ck.server.Call("KVServer.Get",
		arg,
		reply,
	); !ok {
		ck.server.Call("KVServer.Get",
			arg,
			reply,
		)
	}

	return reply.Value
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

	arg := &PutAppendArgs{
		Key:        key,
		Value:      value,
		UniqueName: uniqueName(),
	}

	reply := &PutAppendReply{}

	if ok := ck.server.Call(method, arg, reply); !ok {
		//if op == "Append" {
		//	ck.appendFailChan <- arg
		//} else {
		//	ck.putFailChan <- arg
		//}

		ck.server.Call(method, arg, reply)
	}

	return reply.Value
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
