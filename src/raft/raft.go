package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

import "fmt"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term int
	Command interface{}
}

var debug = false


// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	log []LogEntry
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int
	term int
	
	leader int
	leaderIsActive bool
	followIdx int

	commitCnts map[int]int
	applyCh chan ApplyMsg
	commitIdx int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	isleader = rf.state == 2
	term = rf.term

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	ID int
	Term int
	LogTerm int
	LogLen int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Succ bool
	Term int
}

func (rf *Raft) compareLogs(term int, size int) bool {
	ret := false
	tail := len(rf.log)-1
	if tail >= 0 {
		if (term > rf.log[tail].Term) || (term == rf.log[tail].Term && size > tail) {
			ret = true
		}
	} else {
		ret = true
	}
	return ret
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("%v receives vote request from %v in term %v\n", rf.me, args.ID, args.Term)

	if args.Term > rf.term {
		if rf.compareLogs(args.LogTerm, args.LogLen) {

			rf.state = 0
			rf.term = args.Term
			rf.leader = args.ID

			reply.Succ = true
			reply.Term = rf.term
		}
	} else {
		reply.Succ = false
		reply.Term = rf.term
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	
	return ok
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	isLeader = rf.state == 2
	if isLeader {
		index = len(rf.log)
		term = rf.term

		entry := LogEntry{Term : term, Command : command}
		rf.log = append(rf.log, entry)
	}
	rf.mu.Unlock()

	return index+1, term, isLeader
}

func (rf *Raft) sendEntries(server int, command interface{}, nextIdx int) {
	isLeader := rf.state == 2
	tail := nextIdx+1
	curIdx := nextIdx

	for rf.killed() == false && isLeader && curIdx != tail {
		rf.mu.Lock()
		isLeader = rf.state == 2
		if !isLeader {
			rf.mu.Unlock()
			continue
		}

		args := AppendEntryArgs{ID : rf.me, Term : rf.term, CommitIdx : rf.commitIdx, IsHeartbeat : false, NextIdx : curIdx, Entry : rf.log[curIdx]}
		if curIdx > 0 {
			args.PrevTerm = rf.log[curIdx-1].Term
		}
		reply := AppendEntryReply{}
		
		rf.mu.Unlock()

		rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

		rf.mu.Lock()
		if reply.Ack {
			if reply.Term > rf.term {

				rf.state = 0
				rf.term = reply.Term
				isLeader = false
			} else {
				curIdx = reply.NextIdx
			}
		}
		
		rf.mu.Unlock()
	}

	if curIdx == tail {
		rf.mu.Lock()
		
		rf.commitCnts[nextIdx]++

		rf.mu.Unlock()
	}
}

func (rf *Raft) commit(index int) {
	for i:=rf.commitIdx+1; i<=index; i++ {
		if debug {
			fmt.Printf("%v commits at %v with term %v state %v followIdx %v commitIdx %v\n", rf.me, i+1, rf.log[i].Term, rf.state, rf.followIdx, rf.commitIdx)
		}
		rf.applyCh <- ApplyMsg{CommandValid : true, Command : rf.log[i].Command, CommandIndex : i+1}		
	}
	rf.commitIdx = index
}

func (rf *Raft) checkCommit(index int) {
	isLeader := rf.state == 2
	isFinish := false

	for rf.killed() == false && isLeader && !isFinish {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()

		isLeader = rf.state == 2
		if isLeader && rf.commitCnts[index] >= (len(rf.peers)-1)/2 {
			rf.commit(index)
			isFinish = true
		}

		rf.mu.Unlock()
	}
}

type AppendEntryArgs struct {
	ID int
	Term int
	CommitIdx int

	IsHeartbeat bool

	PrevTerm int
	NextIdx int
	Entry LogEntry
}

type AppendEntryReply struct {
	Ack bool
	Term int
	NextIdx int
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("%v receives append req idx %v from %v %v\n", rf.me, args.NextIdx, args.ID, args.IsHeartbeat)


	if args.Term >= rf.term {
		if rf.leader != args.ID {
			// from candidate to follower
			rf.state = 0
			rf.followIdx = rf.commitIdx
		}
		rf.leader = args.ID
		rf.leaderIsActive = true
		rf.term = args.Term

		if rf.followIdx > rf.commitIdx && args.CommitIdx > rf.commitIdx {
			if rf.followIdx > args.CommitIdx {
				rf.commit(args.CommitIdx)
			} else {
				rf.commit(rf.followIdx)
			}
		}

		if args.IsHeartbeat {
			reply.Ack = true
			return
		}

		loglen := len(rf.log)

		if args.NextIdx > loglen {
			reply.NextIdx = loglen
			reply.Ack = true
			return
		}

		if args.NextIdx == 0 {
			reply.NextIdx = 1
			if loglen == 0 {
				rf.log = append(rf.log, args.Entry)
			} else {
				rf.log[0] = args.Entry
			}
			rf.followIdx = 0
		} else {
			if rf.log[args.NextIdx-1].Term == args.PrevTerm {
				reply.NextIdx = args.NextIdx+1
				if loglen == args.NextIdx {
					rf.log = append(rf.log, args.Entry)
				} else {
					if rf.log[args.NextIdx].Term > args.Entry.Term {
						fmt.Printf("id %v term %v thisterm %v lasterm %v\n", rf.me, rf.term, rf.log[args.NextIdx].Term, rf.log[len(rf.log)-1].Term)
					}

					rf.log[args.NextIdx] = args.Entry
				}

				// if debug {
				//	fmt.Printf("id %v, followIdx %v, nextIdx %v, prevterm %v\n", rf.me, rf.followIdx, args.NextIdx, args.PrevTerm)
				// }

				rf.followIdx = args.NextIdx
			} else {
				reply.NextIdx = args.NextIdx-1
			}
		}
	} else {
		reply.Term = rf.term
	}

	reply.Ack = true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) elect(){
	for rf.killed() == false {
		// send vote requests
		rf.mu.Lock()

		votes := make([]RequestVoteReply, len(rf.peers))

		if rf.state == 1 {
			rf.term++
			for i, _ := range rf.peers {
				if i != rf.me {
					reqVoteArgs := RequestVoteArgs{ID: rf.me, Term : rf.term, LogLen : len(rf.log)}
					if len(rf.log) > 0 {
						 reqVoteArgs.LogTerm = rf.log[reqVoteArgs.LogLen-1].Term
					}
					go rf.sendRequestVote(i, &reqVoteArgs, &votes[i])
				}
			}
		} else {
			rf.mu.Unlock()
			continue
		}

		rf.mu.Unlock()
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// count votes
		rf.mu.Lock()
		if rf.state == 1 {
			cnt := 0
			for _, v := range votes {
				if v.Succ && v.Term == rf.term {
					cnt++
				} else if !v.Succ && (v.Term > rf.term) {
					// discover new term
					rf.state = 0
					break
				}

			}

			// if debug {
			//	fmt.Printf("%v has %v votes in term %v\n", rf.me, cnt+1, rf.term)				
			// }

			if rf.state == 1 && cnt >= (len(rf.peers)-1)/2 {
				rf.state = 2
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == 2 {
			for i, _ := range rf.peers {
				if i != rf.me {
					args := AppendEntryArgs{ID : rf.me, IsHeartbeat: true, Term : rf.term, CommitIdx : rf.commitIdx}
					reply := AppendEntryReply{}

					// fmt.Printf("%v sends heartbeat to %v\n", rf.me, i)

					go rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
				}
			}
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) sendCommitReq() {
	var flag bool
	nextIdx := rf.commitIdx
	for rf.killed() == false {
		time.Sleep(50 * time.Millisecond)

		rf.mu.Lock()
		if rf.state != 2 {
			// will restart counts

			rf.commitCnts[nextIdx] = 0
			nextIdx = rf.commitIdx
			rf.mu.Unlock()
			continue
		}
	
		if rf.commitIdx >= nextIdx {
			// old entry has finished
			nextIdx = rf.commitIdx+1
			flag = false
		}
		if nextIdx == len(rf.log) {
			// all cmd are committed
			rf.mu.Unlock()
			continue
		}
		if flag {
			// old entry is running
			rf.mu.Unlock()
			continue
		}

		rf.commitCnts[nextIdx] = 0
		nextCmd := rf.log[nextIdx].Command
		for i, _ := range rf.peers {
			if i != rf.me {
				go rf.sendEntries(i, nextCmd, nextIdx)
			}
		}
		go rf.checkCommit(nextIdx)
		flag = true
		
		rf.mu.Unlock()
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()	
		if rf.state == 0 {
			// state = follower
			if !rf.leaderIsActive {
				// become candidate
				rf.state = 1
				rf.leader = rf.me
				// fmt.Printf("%v becomes candidate\n", rf.me)
			} else {
				rf.leaderIsActive = false
			}
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.commitIdx = -1
	rf.followIdx = -1

	rf.commitCnts = make(map[int]int)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.elect()
	go rf.heartbeat()
	go rf.sendCommitReq()

	return rf
}
