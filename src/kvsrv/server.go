package kvsrv

import (
	"sync"
)

type KVServer struct {
	mu sync.RWMutex

	store      map[string]string
	duplicates map[string]struct{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	key := args.Key
	unique := args.UniqueName

	if _, ok := kv.readDuplicate(unique); !ok {
		kv.writeDuplicate(unique)
	} else {
		return
	}

	reply.Value = kv.get(key)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	argsKey := args.Key
	argsValue := args.Value
	unique := args.UniqueName

	if _, ok := kv.readDuplicate(unique); !ok {
		kv.writeDuplicate(unique)
	} else {
		return
	}

	kv.put(argsKey, argsValue)

	reply.Value = argsValue
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	argsKey := args.Key
	argsValue := args.Value
	unique := args.UniqueName

	if _, ok := kv.readDuplicate(unique); !ok {
		kv.writeDuplicate(unique)
	} else {
		return
	}

	reply.Value = kv.append(argsKey, argsValue)
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		store:      make(map[string]string),
		duplicates: make(map[string]struct{}),
	}

	return kv
}

func (kv *KVServer) readDuplicate(key string) (val struct{}, ok bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, ok = kv.duplicates[key]
	return val, ok
}

func (kv *KVServer) writeDuplicate(key string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.duplicates[key] = struct{}{}
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
