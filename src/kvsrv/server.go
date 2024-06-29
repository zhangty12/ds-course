package kvsrv

import (
	"log"
	"sync"
	"strings"
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

	// Your definitions here.
	data map[string]*strings.Builder
	ids map[int64]bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.data[args.Key] == nil {
		reply.Value = ""
	} else {
		reply.Value = kv.data[args.Key].String()
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.ids[args.ID] {
		kv.mu.Lock()

		kv.data[args.Key] = new(strings.Builder)	
		kv.data[args.Key].WriteString(args.Value)
		reply.Value = args.Value
		
		delete(kv.ids, args.PrevID)
		kv.ids[args.ID] = true

		kv.mu.Unlock()	
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.ids[args.ID] {
		kv.mu.Lock()
		if kv.data[args.Key] == nil {
			kv.data[args.Key] = new(strings.Builder)
		}
	
		reply.Value = kv.data[args.Key].String()
		kv.data[args.Key].WriteString(args.Value)

		delete(kv.ids, args.PrevID)
		kv.ids[args.ID] = true

		kv.mu.Unlock()
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.

	return kv
}
