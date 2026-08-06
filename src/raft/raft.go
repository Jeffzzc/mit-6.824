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
import "math/rand"
import "time"

// import "bytes"
// import "../labgob"



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

// ServerState 表示 Raft 节点的三种角色。
type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

const (
	// Leader 每 120ms 发送一次心跳。
	//
	// 实验要求每秒心跳次数不能超过 10 次，
	// 120ms 一次大约是每秒 8.3 次。
	heartbeatInterval = 120 * time.Millisecond

	// 每个节点的选举超时是随机的。
	//
	// 如果所有节点的超时时间相同，它们可能同时成为 Candidate，
	// 相互瓜分选票，导致一直无法选出 Leader。
	electionTimeoutMin = 400 * time.Millisecond
	electionTimeoutMax = 700 * time.Millisecond

	// ticker 每隔一小段时间检查是否超时。
	tickerInterval = 10 * time.Millisecond
)

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

	// 所有服务器都需要维护的持久化状态。
	//
	// Part 2A 暂时不测试节点崩溃后的持久化恢复，
	// 但是选举逻辑仍然需要维护这两个变量。
	currentTerm int

	// votedFor 表示当前任期把票投给了谁。
	// -1 表示本任期还没有投票。
	votedFor    int

	// 当前节点身份：Follower、Candidate 或 Leader。
	state ServerState

	// 选举超时的绝对截止时间。
	//
	// 当当前时间超过 electionDeadline 时，
	// Follower 或 Candidate 应该开启新一轮选举。
	electionDeadline time.Time

	// Leader 下一次发送心跳的时间。
	heartbeatDeadline time.Time

	// 每个节点有一个独立随机数生成器，
	// 用于生成随机 election timeout。
	//
	// rand.Rand 不是并发安全的，因此只能在持有 rf.mu 时使用。
	rng *rand.Rand
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// Candidate 当前所在任期。
	Term int

	// Candidate 的节点编号。
	CandidateId int

	// 这两个字段在 2B 中用于比较日志的新旧程度。
	//
	// Part 2A 还没有日志，所以所有节点都发送 0。
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	// 接收者当前任期。
	//
	// Candidate 收到更高任期后必须退回 Follower。
	Term int

	// 是否同意投票。
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 默认拒绝投票
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Candidate 的任期比当前节点旧，直接拒绝。
	if args.Term < rf.currentTerm {
		return
	}

	// 如果 Candidate 的任期更高，
	// 当前节点必须承认新的任期，并退回 Follower。
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// 一个节点在一个任期内只能投一票。
	//
	// votedFor == -1：
	// 本任期还没有投票。
	//
	// votedFor == args.CandidateId：
	// 允许重复回复同一个 Candidate，因为之前的 RPC 回复可能丢失。
	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId

	// Part 2A 还没有真正的日志，
	// 因此所有 Candidate 的日志都视为一样新。
	//
	// 到 Part 2B 时需要在这里加入日志新旧判断。
	if canVote {
		rf.votedFor = args.CandidateId
		rf.state = Follower

		// votedFor 属于持久化状态。
		// Part 2A 的 persist() 暂时为空。
		rf.persist()

		// 成功投票之后，重置 election timeout。
		//
		// 否则当前节点可能刚投完票，
		// 马上又因为旧计时器超时而发起自己的选举。
		rf.resetElectionDeadlineLocked()

		reply.VoteGranted = true
	}

	reply.Term = rf.currentTerm
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

	// 不要在这里无限重试。
	//
	// Call() 已经会等待 RPC 成功或者超时。
	// 如果一次 RPC 丢失，Candidate 等待下一次 election timeout，
	// 再发起新一轮选举即可。
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// AppendEntries 在 Part 2A 中只用作空心跳。
//
// 因为目前不复制日志，所以暂时只需要 Term 和 LeaderId。
//
type AppendEntriesArgs struct {
	// Leader 的当前任期。
	Term int

	// Leader 的节点编号。
	LeaderId int
}

type AppendEntriesReply struct {
	// 接收者的当前任期。
	Term int

	// 是否接受此次心跳。
	Success bool
}

// AppendEntries RPC handler.
//
// Part 2A 中收到合法 AppendEntries 的主要作用是：
//
//  1. 证明 Leader 仍然存活；
//  2. 重置 Follower 的 election timeout；
//  3. Candidate 收到同任期 Leader 心跳后退回 Follower；
//  4. 旧 Leader 通过回复发现自己任期已经过时。
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// 来自旧任期 Leader 的心跳必须拒绝。
	if args.Term < rf.currentTerm {
		return
	}

	// 如果收到更高任期的 Leader 心跳，
	// 更新当前任期并清除之前的投票。
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1

		rf.persist()
	}

	// 即使当前节点是 Candidate，
	// 收到相同任期的合法 Leader 心跳后也必须退回 Follower。
	rf.state = Follower

	// 收到合法心跳后重置选举超时，
	// 从而避免在 Leader 正常工作时发起选举。
	rf.resetElectionDeadlineLocked()

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(
	server int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return -1, rf.currentTerm, rf.state == Leader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker 是 Raft 的主要后台循环。
//
// 它负责两件事：
//
//  1. Leader 定期发送心跳；
//  2. Follower/Candidate 选举超时后发起新选举。

func (rf *Raft) ticker() {
	ticker := time.NewTicker(tickerInterval)
	defer ticker.Stop()

	for !rf.killed() {
		<- ticker.C

		now := time.Now()
		rf.mu.Lock()

		if rf.state == Leader {
			// Leader 到达下一次心跳时间。
			// now >= rf.heartbeatDeadline
			if !now.Before(rf.heartbeatDeadline) {
				term := rf.currentTerm
				// 先更新下一次心跳时间，再释放锁。
				//
				// 否则 ticker 下一次循环可能再次看到旧截止时间，
				// 导致重复发送多轮心跳。
				rf.heartbeatDeadline = now.Add(heartbeatInterval)
				rf.mu.Unlock()

				// 网络 RPC 不能在持有 rf.mu 时发送。
				rf.broadcastHeartbeats(term)
				continue
			}
		} else {
			// Follower 或 Candidate 到达 election timeout。
			// now >= rf.electionDeadline
			if !now.Before(rf.electionDeadline) {
				rf.mu.Unlock()

				rf.startElection()
				continue
			}
		}

		rf.mu.Unlock()
	}
}

// startElection 开启新一轮选举。
func (rf *Raft) startElection() {
	rf.mu.Lock()

	// ticker 判断超时之后会先释放锁。
	//
	// 在 ticker 释放锁到 startElection 获得锁之间，
	// 可能恰好收到 Leader 心跳并重置 electionDeadline。
	//
	// 因此这里必须再次检查是否真的仍然超时。
	if rf.killed() || rf.state == Leader || !time.Now().After(rf.electionDeadline) {
		rf.mu.Unlock()
		return
	}

	// 当前节点成为 Candidate。
	rf.state = Candidate

	// 发起新选举必须增加任期。
	rf.currentTerm++

	// 保存本轮选举的任期快照。
	//
	// 后续 RPC 回复可能很晚才到达，
	// 必须用 electionTerm 判断回复是否属于当前这轮选举。
	electionTerm := rf.currentTerm

	// Candidate 首先给自己投票。
	rf.votedFor = rf.me
	rf.persist()

	// 如果本轮选举没有成功，
	// Candidate 在新的随机超时后发起下一轮选举。
	rf.resetElectionDeadlineLocked()

	// 已经获得自己的一票。
	votes := 1

	// 多数票数量。
	//
	// 3 个节点需要 2 票；
	// 5 个节点需要 3 票。
	majority := len(rf.peers)/2 + 1

	// 处理只有一个节点的情况。
	if votes >= majority {
		rf.becomeLeaderLocked()

		rf.mu.Unlock()

		// 成为 Leader 后立即发送心跳。
		rf.broadcastHeartbeats(electionTerm)
		return
	}

	rf.mu.Unlock()

	// 并发向所有其他节点请求投票。
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		args := RequestVoteArgs{
			Term:         electionTerm,
			CandidateId:  rf.me,
			LastLogIndex: 0,
			LastLogTerm:  0,
		}

		// server 和 args 都以参数形式传入 goroutine。
		//
		// 不能直接不带参数地引用循环变量，
		// 否则不同 goroutine 可能读到错误的 server 值。
		go func(peer int, request RequestVoteArgs) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(peer, &request, &reply)

			if !ok {
				// RPC 丢失或目标节点不可达。
				//
				// 不需要立即重试，
				// 等待本节点下一次 election timeout 即可。
				return
			}

			wonElection := false

			rf.mu.Lock()

			// 如果回复中的任期比当前节点更高，
			// 当前 Candidate 或 Leader 必须立即退回 Follower。
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)

				rf.mu.Unlock()
				return
			}

			// 过滤旧选举中的迟到回复。
			//
			// 例如：
			// 1. 节点在 term 3 发出 RequestVote；
			// 2. RPC 很慢；
			// 3. 节点已经进入 term 4；
			// 4. term 3 的投票才返回。
			//
			// 这个旧投票不能计入 term 4。
			if rf.state != Candidate || rf.currentTerm != electionTerm {
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				// votes 会被多个 RPC goroutine 访问，
				// 但所有访问都在 rf.mu 保护下，
				// 因此不会产生 data race。
				votes++

				if votes >= majority {
					rf.becomeLeaderLocked()
					wonElection = true
				}
			}

			rf.mu.Unlock()

			// 网络 RPC 不能在持有 rf.mu 的情况下发送。
			if wonElection {
				rf.broadcastHeartbeats(electionTerm)
			}
		}(server, args)
	}
}

// broadcastHeartbeats 向所有其他节点并发发送空 AppendEntries。
func (rf *Raft) broadcastHeartbeats(term int) {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(peer int) {
			rf.mu.Lock()

			// 心跳 goroutine 可能因为调度或网络延迟而很晚执行。
			//
			// 发送前必须确认当前节点：
			// 1. 仍然是 Leader；
			// 2. 仍然处于发起该心跳时的任期。
			if rf.killed() || rf.state != Leader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}

			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}

			rf.mu.Unlock()

			var reply AppendEntriesReply
			
			ok := rf.sendAppendEntries(
				peer,
				&args,
				&reply,
			)

			if !ok {
				// 单次心跳失败无需立即重试。
				// 下一轮定时心跳仍会继续发送。
				return
			}
			
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// RPC 返回时，当前节点可能已经不是 Leader，
			// 或者已经进入了新的任期。
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}
		}(server)
	}
}

// becomeFollowerLocked 将节点转为 Follower。
//
// 调用该函数之前必须持有 rf.mu。
func (rf *Raft) becomeFollowerLocked(term int) {

	// 只有进入更高任期时才能清除 votedFor。
	//
	// 如果只是同一任期从 Candidate 退回 Follower，
	// 本任期已经投过的票仍然有效，不能清除。
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}

	rf.state = Follower

	rf.resetElectionDeadlineLocked()
}

// becomeLeaderLocked 将 Candidate 转为 Leader。
//
// 调用该函数之前必须持有 rf.mu。
func (rf *Raft) becomeLeaderLocked() {
	rf.state = Leader

	// startElection 在成为 Leader 后会立即发送第一轮心跳。
	//
	// 因此下一轮定时心跳放在 heartbeatInterval 之后。
	rf.heartbeatDeadline = time.Now().Add(heartbeatInterval)
}

// resetElectionDeadlineLocked 重新设置随机选举超时。
//
// 调用该函数之前必须持有 rf.mu。
func (rf *Raft) resetElectionDeadlineLocked() {
	timeoutRange := electionTimeoutMax - electionTimeoutMin
	timeout := electionTimeoutMin

	if timeoutRange > 0 {
		timeout += time.Duration(rf.rng.Int63n(int64(timeoutRange)))
	}

	rf.electionDeadline = time.Now().Add(timeout)
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
	// 初始任期为 0。
	rf.currentTerm = 0

	// -1 表示当前任期没有投票。
	rf.votedFor = -1

	// 节点启动时首先是 Follower。
	rf.state = Follower

	// 为不同节点生成不同的随机数序列。
	//
	// me 参与 seed，降低多个节点得到相同选举超时的概率。
	seed := time.Now().UnixNano() +	int64(me + 1) * 1000003

	rf.rng = rand.New(
		rand.NewSource(seed),
	)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Lock()

	// 节点启动后设置第一次随机选举超时。
	rf.resetElectionDeadlineLocked()

	// 当前不是 Leader，暂时不会使用 heartbeatDeadline。
	rf.heartbeatDeadline = time.Now()

	rf.mu.Unlock()

	// Make() 必须快速返回，
	// 因此选举和心跳逻辑放到后台 goroutine 中运行。
	go rf.ticker()

	return rf
}