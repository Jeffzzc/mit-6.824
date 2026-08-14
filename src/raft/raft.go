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

import "sync"
import "sync/atomic"
import "6.824/src/labrpc"
// import "math/rand"
import "time"

import "bytes"
import "6.824/src/labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state       State
	electionDeadline time.Time
	heartbeatDeadline time.Time

	applyCh chan ApplyMsg
	applyCond *sync.Cond
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

const (
	heartbeatInterval = 120 * time.Millisecond
	electionTimeout = 150 * time.Millisecond
	tickerInterval = 10 * time.Millisecond
)


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil {
		return
	}

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}   

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.electionDeadline = time.Now().Add(electionTimeout)
	}

	reply.Term = rf.currentTerm

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && 
	   (args.LastLogTerm > rf.log[len(rf.log)-1].Term || 
	   (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	if reply.VoteGranted {
		rf.electionDeadline = time.Now().Add(electionTimeout)
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// Leader 的当前任期。
	Term int

	// Leader 的节点编号。
	LeaderId int

	// 新 Entries 前一条日志的位置和任期, 用于 consistency check
	PrevLogIndex int
	PrevLogTerm  int

	// Leader 新的日志条目
	//
	// heartbeat 时为空
	Entries []LogEntry

	// Leader 已知的最大已提交日志索引
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictIndex int
	ConflictTerm int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.electionDeadline = time.Now().Add(electionTimeout)
	}

	rf.state = Follower
	rf.electionDeadline = time.Now().Add(electionTimeout)

	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.log[args.PrevLogIndex].Term
		conflictIndex := args.PrevLogIndex
		for conflictIndex > 0 && rf.log[conflictIndex-1].Term == conflictTerm {
			conflictIndex--
		}
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = conflictIndex
		reply.Success = false
		return
	}

	insertIndex := args.PrevLogIndex + 1
	entryOffset := 0
	logChanged := false
	for entryOffset < len(args.Entries) && insertIndex < len(rf.log) {
		if rf.log[insertIndex].Term != args.Entries[entryOffset].Term {
			rf.log = rf.log[:insertIndex]
			logChanged = true
			break
		}
		insertIndex++
		entryOffset++
	}

	if entryOffset < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[entryOffset:]...)
		logChanged = true
	}

	if logChanged {
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		newCommit := min(args.LeaderCommit, len(rf.log) - 1)
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Broadcast()
		}
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.state != Leader {
		isLeader = false
		return index, rf.currentTerm, isLeader
	}

	newEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newEntry)
	index = len(rf.log) - 1
	term = rf.currentTerm
	rf.persist()

	// 更新 Leader 自己的 matchIndex / nextIndex
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.nextIndex[rf.me] = len(rf.log)

	rf.broadcastAppendEntries(term)

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.applyCond != nil {
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	ticker := time.NewTicker(tickerInterval)
	defer ticker.Stop()

	for !rf.killed() {
		<- ticker.C

		now := time.Now()
		rf.mu.Lock()

		if rf.state == Leader {
			if now.After(rf.heartbeatDeadline) {
				term := rf.currentTerm
				rf.heartbeatDeadline = now.Add(heartbeatInterval)
				rf.mu.Unlock()
				rf.broadcastAppendEntries(term)
				continue
			}
		} else {
			if now.After(rf.electionDeadline) {
				rf.mu.Unlock()
				rf.startElection()
				continue
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.state == Leader || !time.Now().After(rf.electionDeadline) {
		return
	}

	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	rf.resetElectionDeadlineLocked()

	lastLogIndex, lastLogTerm := rf.lastLogInfoLocked()

	votes := 1
	majority := len(rf.peers) / 2 + 1

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}

		go func(server int, args *RequestVoteArgs) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, args, &reply)
			if !ok {
				return
			}

			wonElection := false
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}
			
			if rf.state != Candidate || rf.currentTerm != args.Term {
				return
			}

			if reply.VoteGranted {
				votes++
				if votes >= majority {
					wonElection = true
					rf.becomeLeaderLocked()
				}
			}
			
			if wonElection {
				rf.broadcastAppendEntries(rf.currentTerm)
			}

		}(server, args)
	}
}

func (rf *Raft) resetElectionDeadlineLocked() {
	rf.electionDeadline = time.Now().Add(electionTimeout)
}

func (rf *Raft) lastLogInfoLocked() (int, int) {
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) becomeFollowerLocked(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
	rf.resetElectionDeadlineLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	next := len(rf.log)
	for i := range rf.peers {
		rf.nextIndex[i] = next
		rf.matchIndex[i] = 0
	}
	rf.nextIndex[rf.me] = next
	rf.matchIndex[rf.me] = next - 1
	rf.heartbeatDeadline = time.Now().Add(heartbeatInterval)
}

func (rf *Raft) broadcastAppendEntries(term int) {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go rf.replicateToPeer(server, term)
	}
}

func (rf *Raft) replicateToPeer(server int, term int) {
	if !rf.killed() {
		rf.mu.Lock()

		// 这批复制任务已经过期
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		next := rf.nextIndex[server]
		if next < 1 {
			next = 1
			rf.nextIndex[server] = next
		}
		if next > len(rf.log) {
			next = len(rf.log)
			rf.nextIndex[server] = next
		}
		prevLogIndex := next - 1
		prevLogTerm := rf.log[prevLogIndex].Term
		entries := make([]LogEntry, len(rf.log[next:]))
		copy(entries, rf.log[next:])

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		
		// 记录这次 RPC 基于哪个 nextIndex 构造
		// 后面用来识别“迟到的失败回复”
		sentNext := next
		rf.mu.Unlock()

		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(server, args, &reply)
		if !ok {
			return
		}
		needBroadcastCommit := false
		rf.mu.Lock()
		// RPC 返回期间，当前节点可能已失去 leadership 或进入新 term
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		// 任何 RPC reply 携带更高 term，都必须立刻 step down
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			matched := args.PrevLogIndex + len(args.Entries)
			// if matched > rf.matchIndex[server] {
			// 	rf.matchIndex[server] = matched
			// }
			// if matched > rf.nextIndex[server] {
			// 	rf.nextIndex[server] = matched + 1
			// }
			rf.matchIndex[server] = matched
    		rf.nextIndex[server] = matched + 1
			if rf.advancedCommitLocked() {
				needBroadcastCommit = true
			}
			rf.mu.Unlock()
			if needBroadcastCommit {
				rf.broadcastAppendEntries(rf.currentTerm)
			}
			return
		}

		if rf.nextIndex[server] != sentNext {
			rf.mu.Unlock()
			return
		}

		if reply.ConflictTerm == -1 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			lastIndexOfConflictTerm := -1
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					lastIndexOfConflictTerm = i
					break
				}
			}

			if lastIndexOfConflictTerm != -1 {
				rf.nextIndex[server] = lastIndexOfConflictTerm + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(
	server int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) advancedCommitLocked() bool {
	if rf.state != Leader {
		return false
	}

	oldCommit := rf.commitIndex
	majority := len(rf.peers) / 2 + 1
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		// Section 5.4.2 的关键限制：
		// Leader 只能“通过数多数派副本”的方式直接提交当前 term 的日志。
		// 一旦当前 term 的日志提交，其之前的旧 term 日志会被间接一起提交。
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		// 接下来统计多少节点有 log[n]
		count := 0
		for peer := range rf.peers {
			if rf.matchIndex[peer] >= n {
				count ++
			}
		}

		if count >= majority {
			rf.commitIndex = n
			break
		}
	}

	if rf.commitIndex > oldCommit {
		rf.applyCond.Broadcast()
		return true
	}

	return rf.commitIndex > oldCommit
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		start := rf.lastApplied + 1
		end := rf.commitIndex
		
		entries := make([]LogEntry, end - start + 1)
		copy(entries, rf.log[start:end+1])
		rf.lastApplied = end
		rf.mu.Unlock()

		for i, entry := range entries {
			msg := ApplyMsg {
				CommandValid: true,
				Command: entry.Command,
				CommandIndex: start + i,
			}

			rf.applyCh <- msg
		}
	}

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.log = []LogEntry{
		{Term: 0},
	}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Lock()
	rf.electionDeadline = time.Now().Add(electionTimeout)
	rf.heartbeatDeadline = time.Now().Add(heartbeatInterval)
	rf.mu.Unlock()

	go rf.ticker()

	go rf.applier()
	return rf
}