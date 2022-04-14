package kvraft

import (
	"6.824/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId      int   // client认为的leader的id
	clientId      int64 // 使用nrand函数产生的随机clerk id
	lastRequestID int	// 确保每次的k/v操作是幂等的
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
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.lastRequestID = 0
	return ck
}
// 修改leaderId
func (ck *Clerk) changeLeader() {
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
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
	ck.lastRequestID += 1
	for {
		args := &GetArgs{
			key,
			ck.clientId,
			ck.lastRequestID,
		}
		reply := &GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", args, reply)
		if !ok || reply.Err == ErrWrongLeader {
			// 该节点联系不上或者不是leader
			ck.changeLeader()
			continue
		} else if reply.Err == ErrNoKey {
			return ""
		} else if reply.Err == OK {
			return reply.Value
		}
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
	ck.lastRequestID += 1
	for {
		args := &PutAppendArgs{
			key,
			value,
			op,
			ck.clientId,
			ck.lastRequestID,
		}
		reply := &PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err == ErrWrongLeader {
			// 该节点联系不上或者不是leader
			ck.changeLeader()
			continue
		}
		if reply.Err == OK {
			// 更新成功
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
