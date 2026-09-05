package kvraft

import (
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"log"
	"6.824/src/raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Op 就是真正写进 Raft log 的命令
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	
	Type string // "Get" or "Put" or "Append"
	Key string
	Value string

	// 唯一标识一个客户端请求
	ClientId int64
	RequestId int64
}

// 某个 Op 被真正 apply 后得到的结果
//
// Get 需要 Value
// Put/Append 不需要 Value，但统一用这个结构比较方便
type OpResult struct {
	Type string

	ClientId  int64
	RequestId int64

	Err   Err
	Value string
}

// 每个客户端最近一次已经执行过的请求以及对应结果
//
// 不能只记录 RequestId
// 因为 Get 的 response 可能丢失
// 如果 Get 重试时数据库已经发生变化，我们必须返回第一次 Get 的原结果
type ClientRecord struct {
	RequestId int64
	Err       Err
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// ============================================================
	// Lab 3A state
	// ============================================================

	// 真正的 key/value state machine。
	db map[string]string

	// ClientId -> 最近一次已经执行过的请求
	// 用于避免：
	// Append("x", "A") 因 RPC response 丢失而被重复执行两次
	clientRecords map[int64]ClientRecord

	// Raft log index -> 等待这个 entry apply 的 RPC handler
	//
	// RPC handler:
	//
	//     index := rf.Start(op)
	//     ↓
	//     waitCh[index] 等待
	//
	// applier:
	//
	//     applyCh 收到 index
	//     ↓
	//     waitCh[index] <- result
	waitCh map[int]chan OpResult
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key: args.Key,
		Type: "Get",
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}

	result, ok := kv.startAndWait(op)

	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key: args.Key,
		Value: args.Value,
		Type: args.Op,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}

	result, ok := kv.startAndWait(op)

	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = result.Err
}

// 把一个 Op 放入 Raft, 然后等待它真正被 commit + apply
//
// 返回：
//     result, true
//
// 表示自己的 Op 确实被 apply
//
// 返回：
//     _, false
//
// 表示：
//     1. 当前节点不是 Leader
//     2. 等待超时
//     3. 同一个 log index 最终 commit 的不是自己的 Op
//
func (kv *KVServer) startAndWait(op Op) (OpResult, bool) {
	kv.mu.Lock()

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		kv.mu.Unlock()
		return OpResult{}, false
	}

	/*
		不要简单写：

		    if _, ok := kv.waitCh[index]; !ok {
		        kv.waitCh[index] = make(...)
		    }

		因为发生 Leader change 后，
		同一个 Raft index 可能已经代表另一个 command。

		这里给当前请求创建新的 channel
	*/
	ch := make(chan OpResult, 1)
	kv.waitCh[index] = ch
	DPrintf("kvserver[%d]: 创建reply通道:index=[%d]\n", kv.me, index)
	kv.mu.Unlock()
	/*
		现在等待 applier
		不能无限等
		例如：
		    S1 是 Leader
		    ↓
		    Start(op)
		    ↓
		    S1 被网络隔离
		    ↓
		    这个 op 永远无法获得多数派
		如果没有 timeout，这个 RPC handler 会永远挂住
	*/
	select {
	case result := <-ch:
		/*
			非常重要：

			仅仅知道：

			    index 被 apply

			还不能说明：

			    自己的 Op 被 apply

			例如：
			Term 5:
			    index 10 = Put(x, 1)

			随后旧 Leader 失败
			Term 6:
			    index 10 = Put(y, 2)

			最终真正 commit 的可能是 Put(y,2)
			所以必须比较 ClientId + RequestId
		*/
		if result.ClientId != op.ClientId ||
			result.RequestId != op.RequestId {
			kv.removeWaitCh(index, ch)
			return OpResult{}, false
		}

		kv.removeWaitCh(index, ch)
		return result, true
	
	case <-time.After(500 * time.Millisecond):
		DPrintf("kvserver[%d]: 处理请求超时: %v\n", kv.me, op)
		/*
			超时后让 Clerk 换一台 server 重试
		*/
		kv.removeWaitCh(index, ch)
		return OpResult{}, false
	}
}

// 删除当前 RPC handler 对应的 channel
// 为什么还要判断:
//     current == ch
// 因为同一个 index 可能在 Leader change 后被另一个请求重新使用
// 旧 RPC handler 超时时，不能误删新请求的 channel
func (kv *KVServer) removeWaitCh(index int, ch chan OpResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	current, exists := kv.waitCh[index]

	if exists && current == ch {
		delete(kv.waitCh, index)
	}
}

func(kv *KVServer) applier() {
	for !kv.killed() {
		msg := <- kv.applyCh
		DPrintf("kvserver[%d]: 收到applyCh消息: %v\n", kv.me, msg)
		/*
			Lab 3A 暂时只处理普通 command

			Lab 3B snapshot 时还会处理 SnapshotValid
			或对应版本中的 snapshot ApplyMsg
		*/
		if !msg.CommandValid {
			continue
		}

		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}
		kv.mu.Lock()
		/*
			只有这里才真正改变 db

			RPC handler 绝对不能直接修改数据库
		*/
		result := kv.applyOperation(op)
		// 通知正在等待这个 Raft log index 的 RPC handler
		if ch, exists := kv.waitCh[msg.CommandIndex]; exists {

			/*
				channel 是 buffered channel(size=1)

				使用非阻塞 send，防止 RPC handler 已经 timeout
				导致 applier 卡死
			*/
			select {
			case ch <- result:
			default:
			}
		}

		kv.mu.Unlock()
	}
}

// 真正执行一个已经 committed 的 Op
//
// 调用时 kv.mu 必须已经被持有
//
func(kv *KVServer) applyOperation(op Op) OpResult {
	result := OpResult{
		Type:      op.Type,
		ClientId:  op.ClientId,
		RequestId: op.RequestId,
		Err:       OK,
		Value:     "",
	}

	record, exists := kv.clientRecords[op.ClientId]

	if exists {
		if op.RequestId == record.RequestId {

			result.Err = record.Err
			result.Value = record.Value

			return result
		}
		/*
			比最近请求还旧

			按照 Lab 的假设：
			一个 Clerk 同时最多只有一个 outstanding request

			所以正常 Client 不应该还在等待这个旧 request

			我们不再次执行它即可。
		*/
		if op.RequestId < record.RequestId {

			result.Err = ErrWrongLeader

			return result
		}
	}

	switch op.Type {
	case "Put":
		kv.db[op.Key] = op.Value
		result.Err = OK
	case "Append":
		kv.db[op.Key] += op.Value
		result.Err = OK
	case "Get":
		value, exists := kv.db[op.Key]
		if !exists {
			result.Err = ErrNoKey
			result.Value = ""
		} else {
			result.Err = OK
			result.Value = value
		}
	default:
		result.Err = ErrWrongLeader
	}

	kv.clientRecords[op.ClientId] = ClientRecord{
		RequestId: op.RequestId,
		Err:       result.Err,
		Value:     result.Value,
	}

	return result
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	// ============================================================
	// 初始化 Lab 3A state
	// ============================================================
	kv.db = make(map[string]string)
	kv.clientRecords = make(map[int64]ClientRecord)
	kv.waitCh = make(map[int]chan OpResult)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go kv.applier()

	return kv
}
