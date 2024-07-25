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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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
	CompTerm int
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

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	Log []LogEntry
	State int
	Term int
	
	// prevLeader int
	// prevTerm int
	Leader int
	// FollowIdx int
	CommitIdx int
	matchIndices []int
	// prevIndices []int
	// reMatch []bool
	followIdx int

	// maxCommitReq int
	active bool
	applyCh chan ApplyMsg

	cmdCond	*sync.Cond
	commitCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	isleader = rf.State == 2
	term = rf.Term

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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.Log)
	e.Encode(rf.State)
	e.Encode(rf.Term)
	e.Encode(rf.Leader)
	e.Encode(rf.CommitIdx)
	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).

	if debug {
		fmt.Printf("%v recovers from crash\n", rf.me)
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var state int
	var term int
	var leader int
	// var followIdx int
	var commitIdx int

	if d.Decode(&log) != nil || d.Decode(&state) != nil || d.Decode(&term) != nil || d.Decode(&leader) != nil || d.Decode(&commitIdx) != nil {
		fmt.Printf("read persist failure\n")
	} else {
		rf.Log = log
		rf.State = state
		rf.Term = term
		rf.Leader = leader
		rf.CommitIdx = commitIdx
	}
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
	tail := len(rf.Log)-1
	if tail >= 0 {
		if (term > rf.Log[tail].CompTerm) || (term == rf.Log[tail].CompTerm && size > tail) {
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

	if debug {
		fmt.Printf("%v in term %v receives vote req from %v in term %v\n", rf.me, rf.Term, args.ID, args.Term)
	}

	if args.Term > rf.Term {
		if rf.compareLogs(args.LogTerm, args.LogLen) {
			reply.Succ = true
			reply.Term = args.Term

			rf.Leader = args.ID
			rf.active = true

			if debug {
				fmt.Printf("%v in term %v votes for %v in term %v\n", rf.me, rf.Term, args.ID, args.Term)
			}
		} else {
			reply.Succ = false
			reply.Term = rf.Term
		}

		prevState := rf.State

		if prevState != 0 || reply.Succ {
			rf.State = 0
			rf.Term = args.Term
			rf.followIdx = rf.CommitIdx
		}

		rf.persist()

		if prevState != 0 {
			go rf.ticker()
		}

	} else {
		reply.Succ = false
		reply.Term = rf.Term
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
	isLeader = rf.State == 2
	if isLeader {
		index = len(rf.Log)
		term = rf.Term

		if debug {
			fmt.Printf("%v adds new cmd at %v in term %v\n", rf.me, index, term)
		}

		entry := LogEntry{Term : term, CompTerm : term, Command : command}
		rf.Log = append(rf.Log, entry)

		// broadcast new cmd
		rf.cmdCond.Broadcast()

		rf.persist()
	}
	rf.mu.Unlock()

	return index+1, term, isLeader
}


func (rf *Raft) sendEntries(server int, votes []int, term int) {
	var prevIdx int
	isMatch := false
	init := true

	for rf.killed() == false {
		rf.mu.Lock()
		if rf.Term > term  {
			// abort
			if debug {
				fmt.Printf("%v abort send commit req to %v rf.commit %v\n", rf.me, server, rf.CommitIdx)
			}

			rf.mu.Unlock()
			return
		}

		nextIdx := len(rf.Log)-1
		
		// empty log or server has catched up
		if nextIdx == -1 || votes[server] == nextIdx {
			if debug {
				fmt.Printf("%v thinks %v is up-to-date %v, no need to send commit req\n", rf.me, server, votes[server])
			}

			// wait for new cmd
			rf.cmdCond.Wait()
			rf.mu.Unlock()

			continue
		}

		if init {
			prevIdx = nextIdx - 1
			init = false
		}
		
		if prevIdx == -1 {
			isMatch = true
		}

		var entries []LogEntry
		if isMatch {
			entries = make([]LogEntry, nextIdx - prevIdx)
			for i:= prevIdx+1; i<=nextIdx; i++ {
				entries[i-prevIdx-1] = rf.Log[i]
			}
		}

		args := AppendEntryArgs{ID : rf.me, Term : rf.Term, MatchIdx : rf.matchIndices[server], CommitIdx : rf.CommitIdx, PrevIdx : prevIdx, Entries : entries}
		if prevIdx >= 0 {
			args.PrevTerm = rf.Log[prevIdx].Term
		}
		reply := AppendEntryReply{}
		
		rf.mu.Unlock()

		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

		if debug {
			fmt.Printf("%v send commit req to %v prevIdx %v channel %v in term %v current term %v len %v\n", rf.me, server, prevIdx, ok, term, rf.Term, len(entries))
		}
		
		if ok {
			rf.mu.Lock()
			if rf.Term > term {
				// abort
				rf.mu.Unlock()
				return
			}

			if reply.Term > rf.Term {
				if debug {
					fmt.Printf("%v sees larger term %v > %v\n", rf.me, reply.Term, rf.Term)
				}

				rf.State = 0
				rf.Term = reply.Term
				rf.followIdx = rf.CommitIdx
				
				rf.persist()

				rf.mu.Unlock()
				go rf.ticker()
				return
			} else if isMatch {
				// send succ
				votes[server] = nextIdx
				prevIdx = nextIdx

				rf.commitCond.Signal()
				rf.mu.Unlock()

			} else if reply.IsMatch {
				// match for the first time, send entries next time
				isMatch = true
				// rf.reMatch[server] = true
				rf.mu.Unlock()
			} else {
				// match fail, decr prevIdx
				// rf.reMatch[server] = false
				prevIdx = reply.PrevIdx
				// rf.prevIndices[server] = prevIdx
				rf.mu.Unlock()
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) printTerms() {
	if len(rf.Log) > 0 {
		fmt.Printf("%v loglen %v logterms: ", rf.me, len(rf.Log))
		for _, e := range rf.Log {
			fmt.Printf("%d ", e.Term)
		}
		fmt.Printf("\n%v commitIdx %v\n", rf.me, rf.CommitIdx)
	}
}

func (rf *Raft) commit(index int) {
	if index <= rf.CommitIdx {
		return
	}
	for i:=rf.CommitIdx+1; i<=index; i++ {
		rf.applyCh <- ApplyMsg{CommandValid : true, Command : rf.Log[i].Command, CommandIndex : i+1}
	}
	if debug {
		rf.printTerms()
		fmt.Printf("%v commits from %v to %v in term %v state %v leader %v\n", rf.me, rf.CommitIdx+1, index, rf.Term, rf.State, rf.Leader)
	}
	rf.CommitIdx = index
}

func (rf *Raft) checkCommit(votes []int, term int) {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.Term > term {
			rf.mu.Unlock()
			return
		}

		matchIdx := -1
		for i:=0; i<len(votes); i++ {
			if i != rf.me {
				cnt := 1
				for j:=0; j<len(votes); j++ {
					if j != rf.me && votes[i] <= votes[j] {
						cnt++
					}
				}
				if cnt >= (len(rf.peers)+1)/2 && votes[i] >= matchIdx {
					matchIdx = votes[i]
				}
			}
		}

		rf.commit(matchIdx)
		for i:=0; i<len(votes); i++ {
			if i != rf.me && votes[i] >= matchIdx {
				rf.matchIndices[i] = matchIdx
				if debug {
					fmt.Printf("%v thinks %v has commit up to %v\n", rf.me, i, matchIdx)
				}
			}
		}
		

		rf.persist()

		// wait for new votes
		rf.commitCond.Wait()
		rf.mu.Unlock()

		// time.Sleep(60 * time.Millisecond)
	}
}

type AppendEntryArgs struct {
	ID int
	Term int
	CommitIdx int
	MatchIdx int

	PrevTerm int
	PrevIdx int
	Entries []LogEntry
}

type AppendEntryReply struct {
	Term int
	PrevIdx int
	IsMatch bool
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	if debug {
		fmt.Printf("%v receives append req from %v with len %v argTerm %v rfTerm %v argCommit %v\n", rf.me, args.ID, len(args.Entries), args.Term, rf.Term, args.CommitIdx)
		rf.printTerms()
	}

	if args.Term >= rf.Term {
		if rf.Leader != args.ID {
			// find new leader
			prevState := rf.State
			rf.Leader = args.ID
			rf.Term = args.Term
			rf.State = 0
			rf.followIdx = rf.CommitIdx

			if prevState != 0 {
				go rf.ticker()
			}
		}
		rf.active = true

		if args.MatchIdx > rf.CommitIdx {
			if debug {
				fmt.Printf("%v receives append req from %v and matchidx %v\n", rf.me, args.ID, args.MatchIdx)
				rf.printTerms()
			}
			rf.commit(args.MatchIdx)
		}

		if args.PrevIdx >= 0 {
			if args.PrevIdx >= len(rf.Log) {
				reply.IsMatch = false
				reply.PrevIdx = len(rf.Log)-1
				return
			} else if rf.Log[args.PrevIdx].Term != args.PrevTerm {
				reply.IsMatch = false
				term := rf.Log[args.PrevIdx].Term
				if rf.Log[0].Term == term {
					reply.PrevIdx = rf.CommitIdx
				} else {
					idx := args.PrevIdx
					for rf.Log[idx].Term == term {
						idx--
					}
					reply.PrevIdx = idx-1
				}
				return
			}
		}

		reply.IsMatch = true

		// append but ignore previous append requests
		if args.Entries != nil && rf.followIdx < args.PrevIdx + len(args.Entries) {
			hd := args.PrevIdx+1
			tl := args.PrevIdx + len(args.Entries)

			for i:=hd; i<=tl; i++ {
				if i>=len(rf.Log) {
					rf.Log = append(rf.Log, args.Entries[i-hd])
				} else {
					rf.Log[i] = args.Entries[i-hd]
				}
			}

			rf.Log = rf.Log[:tl+1]
			rf.followIdx = tl

			if debug {
				fmt.Printf("%v follow %v up to %v\n", rf.me, args.ID, tl)
			}
		}

		if args.CommitIdx >= rf.followIdx {
			rf.commit(rf.followIdx)
		}

	} else {
		if debug {
			fmt.Printf("%v rej append req from %v, terms %v > %v\n", rf.me, args.ID, rf.Term, args.Term)
		}

		reply.Term = rf.Term
	}
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

func (rf *Raft) elect() {
	for rf.killed() == false {
		// send vote requests
		rf.mu.Lock()

		if rf.State != 1 {
			rf.mu.Unlock()
			return
		}

		// start a new term

		rf.Term++
		term := rf.Term
		votes := make([]RequestVoteReply, len(rf.peers))

		if debug {
			fmt.Printf("%v starts election in term %v state %v\n", rf.me, rf.Term, rf.State)
		}

		for i, _ := range rf.peers {
			if i != rf.me {
				reqVoteArgs := RequestVoteArgs{ID: rf.me, Term : rf.Term, LogLen : len(rf.Log)}
				if len(rf.Log) > 0 {
					 reqVoteArgs.LogTerm = rf.Log[reqVoteArgs.LogLen-1].CompTerm
				}
				go rf.sendRequestVote(i, &reqVoteArgs, &votes[i])
			}
		}
		rf.mu.Unlock()

		ms := 400 + (rand.Int63() % 800)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// count votes
		rf.mu.Lock()
		if rf.State != 1 || rf.Term > term {
			rf.mu.Unlock()
			return
		}

		cnt := 1
		for _, v := range votes {
			if v.Succ {
				cnt++
			} 
		}

		// if debug {
		//	fmt.Printf("%v gains %v votes among %v in term %v, current term %v\n", rf.me, cnt, len(rf.peers), term, rf.Term)
		// }

		if cnt >= (len(rf.peers)+1)/2 {
			rf.State = 2
			rf.Term = term
			rf.persist()
			go rf.heartbeat(term)
			go rf.sendCommitReq(term)

			rf.mu.Unlock()
			return
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) heartbeat(term int) {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.Term > term {
			rf.mu.Unlock()
			return
		}
		
		for i, _ := range rf.peers {
			if i != rf.me {
				args := AppendEntryArgs{ID : rf.me, Term : rf.Term, MatchIdx : rf.matchIndices[i], CommitIdx : rf.CommitIdx}
				reply := AppendEntryReply{}

				if debug {
				 	fmt.Printf("%v heartbeat to %v in term %v\n", rf.me, i, term)
				 	rf.printTerms()
				}

				go rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
			}
		}
		
		rf.mu.Unlock()

		time.Sleep(time.Duration(100) * time.Millisecond)
	}
} 


func (rf *Raft) sendCommitReq(term int) {
	rf.mu.Lock()

	if debug {
		fmt.Printf("%v starts leadership in term %v\n", rf.me, rf.Term)
	}

	if len(rf.Log) > 0 {
		rf.Log[len(rf.Log)-1].CompTerm = rf.Term
	}

	votes := make([]int, len(rf.peers))
	for i:=0; i<len(rf.peers); i++ {
		if i != rf.me {
			votes[i] = rf.matchIndices[i]
			go rf.sendEntries(i, votes, term)
		}
	}
	go rf.checkCommit(votes, term)

	rf.mu.Unlock()
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		
		// state = follower
		if !rf.active {
			// become candidate
			rf.State = 1
			rf.Leader = rf.me
			rf.persist()

			go rf.elect()
			rf.mu.Unlock()
			return
			// rf.prevLeader = rf.leader
			// rf.prevTerm = rf.Term

			// fmt.Printf("%v becomes candidate\n", rf.me)
		}

		rf.active = false
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 400 + (rand.Int63() % 800)
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
	rf.CommitIdx = -1

	rf.cmdCond = sync.NewCond(&rf.mu)
	rf.commitCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.followIdx = rf.CommitIdx
	rf.matchIndices = make([]int, len(peers))
	// rf.prevIndices = make([]int, len(peers))
	// rf.reMatch = make([]bool, len(peers))
	for i:=0; i<len(peers); i++ {
		rf.matchIndices[i] = -1
		// rf.reMatch[i] = true
	}

	// start ticker goroutine to start elections
	if rf.State == 0 {
		go rf.ticker()
	} else if rf.State == 1 {
		go rf.elect()
	} else {
		go rf.heartbeat(rf.Term)
		go rf.sendCommitReq(rf.Term)
	}

	return rf
}
