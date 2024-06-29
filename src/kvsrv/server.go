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
	bufAppendAns map[int64]string
	ids map[int64]bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.data[args.Key] == nil {
		reply.Value = ""
	} else {
		reply.Value = kv.data[args.Key].String()
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.ids[args.ID] {

		kv.data[args.Key] = new(strings.Builder)	
		kv.data[args.Key].WriteString(args.Value)
		
		delete(kv.ids, args.PrevID)
		kv.ids[args.ID] = true
	}
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.ids[args.ID] {
		if kv.data[args.Key] == nil {
			kv.data[args.Key] = new(strings.Builder)
		}
	
		kv.bufAppendAns[args.ID] = kv.data[args.Key].String()
		kv.data[args.Key].WriteString(args.Value)

		delete(kv.ids, args.PrevID)
		delete(kv.bufAppendAns, args.PrevID)
		kv.ids[args.ID] = true
	}
	reply.Value = kv.bufAppendAns[args.ID]
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]*strings.Builder)
	kv.bufAppendAns = make(map[int64]string)
	kv.ids = make(map[int64]bool)

	return kv
}
