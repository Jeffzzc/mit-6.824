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

// LogEntry 是 Raft 日志中的一条记录。
// Command 是客户端提交的命令，Term 是该条日志被 Leader 接收时的任期。
type LogEntry struct {
	Command interface{}
	Term    int
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

	currentTerm int

	// votedFor 表示当前任期把票投给了谁。
	// -1 表示本任期还没有投票。
	votedFor    int

	log         []LogEntry

	// ---------------- Volatile state on all servers ----------------
	commitIndex int // 已知的最大已提交日志索引
	lastApplied int // 已经应用到状态机的最大日志索引

	// ---------------- Volatile state on leaders ----------------
	// nextIndex[i]：下一次准备发送给 peer i 的日志下标。
	// matchIndex[i]：已知 peer i 已复制成功的最大日志下标。
	nextIndex	[]int
	matchIndex	[]int
	
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

	// applyCh 用来把已经 commit 的日志交给 tester/service。
	applyCh chan ApplyMsg

	// 当 commitIndex 前进时唤醒 applier；避免一直轮询占 CPU。
	applyCond *sync.Cond
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
// RequestVote 实现 Figure 2 + Section 5.4.1 的 election restriction

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

	// ---------------- 2B 新增：Election Restriction ----------------
	// Candidate 的日志必须“至少和我一样新”才能得到我的票
	// 比较规则：
	// 1. 最后一条日志 term 更大 => Candidate 更新
	// 2. term 相同，则最后 index 更大/相同 => Candidate 更新
	myLastIndex, myLastTerm := rf.lastLogInfoLocked()
	candidateUpToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId

	if canVote && candidateUpToDate {
		rf.votedFor = args.CandidateId
		rf.state = Follower
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

// AppendEntriesArgs 既用于真正复制日志,也用于 heartbeat
// heartbeat 时 Entries 为空
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
	// 接收者的当前任期。
	Term int

	// 是否接受此次心跳。
	Success bool

	// 这两个字段不是 Figure 2 必需字段，而是论文 5.3 提到的快速回退优化。
	// TestBackup2B 一类测试中很有用，可以避免 nextIndex 每次只减 1。
	ConflictTerm  int
	ConflictIndex int
}

// AppendEntries 负责：
// 1. 心跳
// 2. PrevLog consistency check
// 3. 删除冲突 suffix
// 4. append Leader 的新日志
// 5. 根据 LeaderCommit 推进 commitIndex

/*
收到 AppendEntries
       │
       ▼
Leader term 太旧？
       │
    yes│
       └──────> reject
       │no
       ▼
更新 term / 转 Follower
       │
       ▼
reset election timer
       │
       ▼
PrevLogIndex 存在吗？
       │
    no │
       └──────> reject + conflictIndex=len(log)
       │yes
       ▼
PrevLogTerm 一样吗？
       │
    no │
       └──────> reject + conflictTerm/conflictIndex
       │yes
       ▼
    前缀匹配
       │
       ▼
逐条比较 Entries
       │
       ├── 相同 → 跳过
       │
       └── 冲突 → 删除 suffix
                     │
                     ▼
              append 剩余 entries
                     │
                     ▼
               persist log
                     │
                     ▼
             更新 commitIndex
                     │
                     ▼
              唤醒 applier
                     │
                     ▼
               Success=true
*/

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1

	// Rule 1: Leader 的 term 太旧
	if args.Term < rf.currentTerm {
		return
	}

	// args.Term >= currentTerm，承认当前任期的 Leader
	// 对 Candidate 来说，即使 term 相同，也必须退回 Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// 即使当前节点是 Candidate，
	// 收到相同任期的合法 Leader 心跳后也必须退回 Follower。
	rf.state = Follower

	// 收到当前 Leader 的 AppendEntries包括 consistency check 失败的 RPC
	// 就说明 Leader 活着 因此重置 election timeout
	rf.resetElectionDeadlineLocked()

	// Rule 2a: Follower 日志没有 PrevLogIndex 这一项
	if args.PrevLogIndex >= len(rf.log) {
		// 告诉 Leader：我的日志总长度只有 len(log),
		// 你下一次可以直接从这里开始试
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		reply.Term = rf.currentTerm
		return
	}

	// Rule 2b: PrevLogIndex 存在, 但 term 不匹配
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.log[args.PrevLogIndex].Term
		conflictIndex := args.PrevLogIndex

		// 找到 follower 中 conflictTerm 第一次出现的位置
		// Leader 可以一次跳过整个 conflictTerm
		for conflictIndex > 0 && rf.log[conflictIndex - 1].Term == conflictTerm {
			conflictIndex--
		}

		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = conflictIndex
		reply.Term = rf.currentTerm
		return
	}

	// 到这里说明 PrevLogIndex/PrevLogTerm 已匹配
	// 从 PrevLogIndex+1 开始逐条比较新 Entries
	insertIndex := args.PrevLogIndex + 1
	// Leader传来的 Entries 中当前比较的位置
	entryOffset := 0
	logChanged := false

	// Figure 2 Rule 3：如果同一 index 的 term 冲突,
	// 删除该 index 以及其后的全部本地日志
	for entryOffset < len(args.Entries) && insertIndex < len(rf.log) {
		if rf.log[insertIndex].Term != args.Entries[entryOffset].Term {
			// 删除冲突 suffix
			// 实际保留到 insertIndex-1, 因为 slice 的上界是开区间
			rf.log = rf.log[:insertIndex]
			logChanged = true
			break
		}
		insertIndex++
		// 这里也会把Follower已经存在的并且和Leader相同的日志保留下来
		entryOffset++
	}

	// Figure 2 Rule 4: append 尚不存在的 Entries
	if entryOffset < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[entryOffset:]...)
		logChanged = true
	}

	if logChanged {
		rf.persist()
	}

	// Figure 2 Rule 5: Follower 的 commitIndex 不得超过 LeaderCommit,
	// 也不得超过自己实际拥有的最后一条日志
	if args.LeaderCommit > rf.commitIndex {
		newCommit := minInt(args.LeaderCommit, len(rf.log) - 1)
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Broadcast()
		}
	}

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

// Start 接受客户端命令
// 2B 的关键: 只有 Leader 才把 command append 到自己的 log, 随后立即触发复制
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	
	term := rf.currentTerm
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return -1, term, false
	}

	// 因为 log[0] 是 sentinel, 所以 len(log) 正好就是新日志的 index
	index := len(rf.log)

	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	term = rf.currentTerm
	rf.persist()

	// 单节点集群时, 自己就是多数派, 可以直接 commit
	rf.advanceCommitLocked()
	rf.mu.Unlock()

	// 不等下一次 heartbeat, 立即复制客户端刚提交的日志
	rf.broadcastAppendEntries(term)

	return index, term, true
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

	// TODO: 没懂什么意思
	// 唤醒可能睡在 Cond.Wait() 中的 applier, 让它有机会退出
	rf.mu.Lock()
	if rf.applyCond != nil {
		rf.applyCond.Broadcast()
	}
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker 负责 election timeout 和定时 heartbeat

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
				rf.broadcastAppendEntries(term)
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

// startElection 在 2A 基础上增加 Candidate 自己最后一条日志的 index/term
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

	// ---------------- 2B 新增 ----------------
	// RequestVote 必须携带 Candidate 最后日志的信息。
	lastLogIndex, lastLogTerm := rf.lastLogInfoLocked()

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
		rf.broadcastAppendEntries(electionTerm)
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
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
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
				rf.broadcastAppendEntries(electionTerm)
			}
		}(server, args)
	}
}

// broadcastAppendEntries 为每个 follower 启动一次复制任务。
//
// 每个 follower 可能处于不同的 nextIndex，因此发送内容也不同：
// - 已经追上 Leader：Entries 为空，相当于 heartbeat；
// - 落后 Leader：Entries = log[nextIndex:]。
func (rf *Raft) broadcastAppendEntries(term int) {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go rf.replicateToPeer(server, term)
	}
}

// replicateToPeer 尝试让某一个 Follower 的日志追上 Leader。
//
// consistency failure 时会根据 ConflictTerm/ConflictIndex 快速回退并立即重试。
// 网络丢包(Call 返回 false)时不无限重试，等待下一次 heartbeat/Start 即可。
func (rf *Raft) replicateToPeer(peer int, term int) {
	for !rf.killed() {
		rf.mu.Lock()

		// 这批复制任务已经过期
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		// 计算要发送给 follower 的日志条目
		next := rf.nextIndex[peer]

		// 防御性限制。正常情况下 nextIndex 始终位于 [1, len(log)]
		if next < 1 {
			next = 1
			rf.nextIndex[peer] = next
		}
		if next > len(rf.log) {
			next = len(rf.log)
			rf.nextIndex[peer] = next
		}

		prevLogIndex := next - 1
		prevLogTerm := rf.log[prevLogIndex].Term

		// 必须 copy 一份 suffix 后再解锁。
		// 这样 RPC 编码 args 时不会和其他 goroutine append rf.log 产生共享切片问题。
		entries := make([]LogEntry, len(rf.log[next:]))
		copy(entries, rf.log[next:])

		args := AppendEntriesArgs{
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
		ok := rf.sendAppendEntries(peer, &args, &reply)
		
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
			// 此次 AppendEntries 成功意味着：
			// follower 至少已经拥有到 PrevLogIndex + len(Entries) 的日志
			// 因此可以更新 nextIndex/matchIndex
			matched := args.PrevLogIndex + len(args.Entries)

			// 可能存在多个并发 RPC，所以只能单调增加 matchIndex/nextIndex
			if matched > rf.matchIndex[peer] {
				rf.matchIndex[peer] = matched
			}
			if matched + 1 > rf.nextIndex[peer] {
				rf.nextIndex[peer] = matched + 1
			}

			// Leader 根据多数派 matchIndex 判断哪些日志可以 commit
			if rf.advanceCommitLocked() {
				needBroadcastCommit = true
			}

			rf.mu.Unlock()

			// commitIndex 前进后尽快告诉 Followers，
			// 不必等下一次 120ms heartbeat。
			if needBroadcastCommit {
				rf.broadcastAppendEntries(term)
			}

			return
		}

		// ---------------- consistency failure ----------------
		// 可能有另一个更新的 AppendEntries 已经成功并推进 nextIndex
		// 此时这个旧失败回复绝不能把 nextIndex 又往回拉
		if rf.nextIndex[peer] != sentNext {
			rf.mu.Unlock()
			return
		}
		
		// 快速回退优化：
		// 1. follower 太短：直接跳到 follower 的日志末尾；
		// 2. term 冲突：如果 Leader 也有 conflictTerm，跳到 Leader 中该 term
		//    的最后一项之后；否则跳到 follower 中该 term 第一次出现的位置。
		if reply.ConflictTerm == -1 {
			// follower 日志太短，直接跳到 follower 的日志末尾
			rf.nextIndex[peer] = reply.ConflictIndex
		} else {
			lastIndexOfConflictTerm := -1
			for i := len(rf.log) - 1; i >= 1; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					lastIndexOfConflictTerm = i
					break
				}
			}

			if lastIndexOfConflictTerm != -1 {
				rf.nextIndex[peer] = lastIndexOfConflictTerm + 1
			} else {
				rf.nextIndex[peer] = reply.ConflictIndex
			}
		}

		if rf.nextIndex[peer] < 1 {
			rf.nextIndex[peer] = 1
		}
		if rf.nextIndex[peer] > len(rf.log) {
			rf.nextIndex[peer] = len(rf.log)
		}

		rf.mu.Unlock()

	}
}

// advanceCommitLocked 根据 Figure 2 推进 Leader 的 commitIndex。
// 调用者必须持有 rf.mu。
//
// 返回 true 表示 commitIndex 发生了变化。

// 本质是: Leader 根据 matchIndex[] 检查哪些日志已经复制到多数节点，
// 如果某条“当前 term 的日志”已经被多数节点保存，就把 commitIndex 推进到那里。

/*

	Follower AppendEntries 成功
			↓
	Leader 更新 matchIndex[peer]
			↓
	advanceCommitLocked()
			↓
		统计多数派
			↓
	  commitIndex 前进
			↓
		Broadcast()
			↓
	  applier apply

*/

func (rf *Raft) advanceCommitLocked() bool {
	if rf.state != Leader {
		return false
	}

	oldCommit := rf.commitIndex
	majority := len(rf.peers) / 2 + 1

	// 从最大的 n 向下找，可以一次直接推进到当前可提交的最大位置。
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

	return false
}

// applier 按顺序把 [lastApplied+1, commitIndex] 的日志发送到 applyCh
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		for !rf.killed() && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		start := rf.lastApplied + 1
		end := rf.commitIndex

		// 在锁内复制需要 apply 的日志，然后解锁再向 channel 发送。
		// channel 可能阻塞，因此绝不能持有 rf.mu 发送。
		entries := make([]LogEntry, end - start + 1)
		copy(entries, rf.log[start:end+1])

		// 这里只有一个 applier goroutine，因此可以先记录 lastApplied。
		rf.lastApplied = end
		rf.mu.Unlock()

		for i, entry := range entries {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: start + i,
			}

			rf.applyCh <- msg
		}
	}
}


// becomeFollowerLocked 的调用者必须持有 rf.mu
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

// Candidate 成为 Leader 时初始化 nextIndex[] / matchIndex[]
func (rf *Raft) becomeLeaderLocked() {
	rf.state = Leader

	// Figure 2：nextIndex 初始化为 Leader lastLogIndex + 1
	next := len(rf.log)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = next
		rf.matchIndex[i] = 0
	}

	// Leader 自己已经拥有自己的全部日志。
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.nextIndex[rf.me] = len(rf.log)

	// startElection 在成为 Leader 后会立即发送第一轮心跳。
	//
	// 因此下一轮定时心跳放在 heartbeatInterval 之后。
	rf.heartbeatDeadline = time.Now().Add(heartbeatInterval)
}

// 返回本节点最后一条日志的 index 和 term。
// sentinel log[0] 保证这个函数始终安全。
func (rf *Raft) lastLogInfoLocked() (int, int) {
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term

	return lastIndex, lastTerm
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

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
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

	// 使用 log[0] 作为 sentinel。
	// 因此第一条真实日志的 index 是 1，与 Raft Figure 2 一致。
	rf.log = []LogEntry{
		{Term: 0},
	}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh

	// 为不同节点生成不同的随机数序列。
	//
	// me 参与 seed，降低多个节点得到相同选举超时的概率。
	seed := time.Now().UnixNano() +	int64(me + 1) * 1000003

	rf.rng = rand.New(
		rand.NewSource(seed),
	)

	// sync.Cond 使用同一把 rf.mu。
	rf.applyCond = sync.NewCond(&rf.mu)

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

	// commit -> applyCh 后台线程。
	go rf.applier()

	return rf
}