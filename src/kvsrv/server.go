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

type AppendBufReply struct {
	key string
	nput int
	idx int
}

type KeyNPut struct {
	key string
	nput int
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.

	// current nput
	nputs map[string]int
	// data and buffered reply
	data map[KeyNPut]*strings.Builder
	// data lengths of current data
	dataLen map[string]int

	// number of pending append ops on KeyIdx
	invCount map[KeyNPut]int

	putIDBuf map[int64]bool
	appendIDBuf map[int64]bool

	// pending append ops to buffered reply
	appendBuf map[int64]AppendBufReply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kp := KeyNPut{key:args.Key, nput:kv.nputs[args.Key]}
	if kv.data[kp] == nil {
		reply.Value = ""
	} else {
		reply.Value = kv.data[kp].String()
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.putIDBuf[args.ID] {
		kp := KeyNPut{key:args.Key, nput:kv.nputs[args.Key]}
		if kv.data[kp] != nil {
			if kv.invCount[kp] == 0 {
				delete(kv.data, kp)
			}
			kp.nput ++
		}
		kv.nputs[args.Key] = kp.nput

		kv.data[kp] = new(strings.Builder)
		kv.data[kp].WriteString(args.Value)
		kv.dataLen[args.Key] = len(args.Value)

		delete(kv.putIDBuf, args.PrevID)
		kv.putIDBuf[args.ID] = true
	}
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.appendIDBuf[args.ID] {
		kp := KeyNPut{key:args.Key, nput:kv.nputs[args.Key]}

		if kv.data[kp] == nil {
			kv.data[kp] = new(strings.Builder)
		}

		kv.appendBuf[args.ID] = AppendBufReply{key:args.Key, nput:kp.nput, idx:kv.dataLen[args.Key]}
		kv.invCount[kp]++
		kv.appendIDBuf[args.ID] = true

		kv.data[kp].WriteString(args.Value)
		kv.dataLen[args.Key] += len(args.Value)

		delete(kv.appendIDBuf, args.PrevID)

		prevKp := KeyNPut{kv.appendBuf[args.PrevID].key, kv.appendBuf[args.PrevID].nput}
		kv.invCount[prevKp]--
		if kv.invCount[prevKp] == 0 && prevKp.nput != kp.nput {
			// remove stale data
			delete(kv.data, prevKp)
		}
		delete(kv.appendBuf, args.PrevID)
	}
	kp := KeyNPut{kv.appendBuf[args.ID].key, kv.appendBuf[args.ID].nput}
	reply.Value = kv.data[kp].String()[:kv.appendBuf[args.ID].idx]
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.nputs = make(map[string]int)
	kv.data = make(map[KeyNPut]*strings.Builder)
	kv.dataLen = make(map[string]int)

	kv.invCount = make(map[KeyNPut]int)

	kv.putIDBuf = make(map[int64]bool)
	kv.appendIDBuf = make(map[int64]bool)

	kv.appendBuf = make(map[int64]AppendBufReply)

	return kv
}
