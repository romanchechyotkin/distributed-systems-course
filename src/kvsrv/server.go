package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	store map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	key := args.Key

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if value, ok := kv.store[key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	argsKey := args.Key
	argsValue := args.Value

	kv.mu.Lock()
	kv.store[argsKey] = argsValue
	kv.mu.Unlock()

	reply.Value = argsValue

	log.Println(kv.store)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	argsKey := args.Key
	argsValue := args.Value

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if value, ok := kv.store[argsKey]; ok {
		reply.Value = value
		kv.store[argsKey] += argsValue

		//kv.store[argsKey] = argsValue
	} else {
		reply.Value = ""
		kv.store[argsKey] = argsValue

		//kv.store[argsKey] = argsValue
	}

	log.Println(kv.store)
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		store: make(map[string]string),
	}

	return kv
}
