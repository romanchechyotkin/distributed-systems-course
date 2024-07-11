package kvsrv

import (
	"sync"
)

type KVServer struct {
	mu sync.RWMutex

	store map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	key := args.Key

	reply.Value = kv.get(key)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	argsKey := args.Key
	argsValue := args.Value

	kv.put(argsKey, argsValue)

	reply.Value = argsValue
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	argsKey := args.Key
	argsValue := args.Value

	reply.Value = kv.append(argsKey, argsValue)
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		store: make(map[string]string),
	}

	return kv
}

func (kv *KVServer) put(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.store[key] = value
}

func (kv *KVServer) append(key, value string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if val, ok := kv.store[key]; ok {
		kv.store[key] += value
		return val
	} else {
		kv.store[key] = value
		return ""
	}
}

func (kv *KVServer) get(key string) string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if value, ok := kv.store[key]; ok {
		return value
	} else {
		return ""
	}
}
