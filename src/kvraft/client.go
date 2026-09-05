package kvraft

import "6.824/src/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// 最近一次成功访问的 leader
	// 下次优先找它，可以避免每次都从 server 0 开始试
	leader int

	// 每个 Clerk 有唯一 ClientId
	clientId int64

	// 每次新的逻辑请求递增
	requestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.leader = 0
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

// 返回一个新的 RequestId
// 注意：一次逻辑请求只能调用一次
// RPC retry 时必须继续使用同一个 requestId
func(ck *Clerk) nextRequestId() int64 {
	ck.requestId++
	return ck.requestId
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	requestId := ck.nextRequestId()

	args := GetArgs{
		Key: key,
		ClientId: ck.clientId,
		RequestId: requestId,
	}

	for {
		server := ck.leader
		var reply GetReply

		ok := ck.servers[server].Call(
			"KVServer.Get",
			&args,
			&reply,
		)

		if ok && !reply.WrongLeader {
			if reply.Err == OK {
				ck.leader = server
				return reply.Value
			}

			if reply.Err == ErrNoKey {
				ck.leader = server
				return ""
			}
		}

		// 当前 server 不是 leader，或者 RPC 失败。
		// 换下一台。
		ck.leader = (server + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	requestId := ck.nextRequestId()

	args := PutAppendArgs{
		Key: key, 
		Value: value,
		Op: op,
		ClientId: ck.clientId,
		RequestId: requestId,
	}

	for {
		server := ck.leader
		var reply PutAppendReply

		ok := ck.servers[server].Call(
			"KVServer.PutAppend",
			&args,
			&reply,
		)

		if ok && !reply.WrongLeader && reply.Err == OK {
			ck.leader = server
			return
		}

		// 当前 server 不是 leader，或者 RPC 失败。
		// 换下一台。
		ck.leader = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
