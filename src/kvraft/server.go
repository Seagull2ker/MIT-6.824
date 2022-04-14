package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true
const (
	// 重试的时间间隔
	raftRetryTimeout = 500
	// 检测snapshot时间间隔
	snapshotTimeout = 10
)

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	ClientId  int64
	Seq       int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// 每个Server对应一个Raft节点
	kvData        map[string]string // kv数据库
	clientDone    map[int64]int     // 记录每个client的seq
	waitChs       map[int]chan Op   // 对每一个index处的日志对应一个chan Op用于监听该日志在raft中是否保存成功
	snapshotIndex int               // snapshot部分最后的index

	judgeOp       Op
	cacheOp		  map[int]Op
}

func (kv *KVServer) getAgree(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	opCh, ok := kv.waitChs[index]
	if !ok {
		opCh = make(chan Op, 1)
		kv.waitChs[index] = opCh
	}
	return opCh
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		"Get",
		args.Key,
		"",
		args.ClientId,
		args.Seq,
	}
	kv.mu.Lock()
	kv.judgeOp = op
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		// 这个节点不是leader，返回错误
		reply.Err = ErrWrongLeader
		return
	}
	// 等待raft备份结果
	ch := kv.getAgree(index)
	var getOp Op
	select {
	case getOp = <-ch:
		// 收到raft消息
		close(ch)
	case <-time.After(time.Millisecond * time.Duration(raftRetryTimeout)):
		// 超时，换服务器重试
		reply.Err = ErrWrongLeader
		return
	}
	if !opEqual(getOp, op) {
		// 前后op不一样
		reply.Err = ErrWrongLeader
		return
	}
	// Leader执行get操作
	kv.mu.Lock()
	val, ok := kv.kvData[args.Key]
	delete(kv.waitChs, index)
	kv.mu.Unlock()
	if !ok {
		reply.Value = ""
		reply.Err = ErrNoKey
	} else {
		reply.Value = val
		reply.Err = OK
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		args.Op,
		args.Key,
		args.Value,
		args.ClientId,
		args.Seq,
	}
	kv.mu.Lock()
	kv.judgeOp = op
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		// 这个节点不是leader，返回错误
		reply.Err = ErrWrongLeader
		return
	}
	// 等待raft备份结果
	ch := kv.getAgree(index)
	var getOp Op
	select {
	case getOp = <-ch:
		// 收到raft消息
		close(ch)
	case <-time.After(time.Millisecond * time.Duration(raftRetryTimeout)):
		// 超时，换服务器重试
		reply.Err = ErrWrongLeader
		return
	}
	if !opEqual(getOp, op) {
		// 前后op不一样
		reply.Err = ErrWrongLeader
		return
	}
	// put/append操作在applyLoop中完成
	reply.Err = OK
	kv.mu.Lock()
	delete(kv.waitChs, index)
	kv.mu.Unlock()
}

func (kv *KVServer) operate(option string, key string, value string) {
	switch option {
	case "Put":
		// 执行put操作
		kv.kvData[key] = value
	case "Append":
		// 执行append操作
		kv.kvData[key] += value
	}
}
func opEqual(a, b Op) bool {
	return a.Key == b.Key && a.Operation == b.Operation && a.Value == b.Value && a.ClientId == b.ClientId && a.Seq == b.Seq
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

// 循环监听raft的applyCh信道
func (kv *KVServer) applyLoop() {
	for kv.killed() == false {
		select {
		case applyMsg := <-kv.applyCh:
			// 收到raft通过applyCh信道上传的信息
			if applyMsg.CommandValid {
				// raft成功commit
				commitCommand := applyMsg.Command.(Op)
				kv.mu.Lock()
				kv.snapshotIndex = applyMsg.CommandIndex
				if lastSeq, ok := kv.clientDone[commitCommand.ClientId]; !ok || lastSeq < commitCommand.Seq {
					// 这个是新的k/v操作，执行这个Op
					kv.clientDone[commitCommand.ClientId] = commitCommand.Seq
					kv.operate(commitCommand.Operation, commitCommand.Key, commitCommand.Value)
				}
				waitCh, ok := kv.waitChs[applyMsg.CommandIndex]
				kv.mu.Unlock()
				if ok {
					waitCh <- commitCommand
				}
			} else if applyMsg.SnapshotValid {
				// 执行snapshot
				DPrintf("kv begin install snapshot")
				kv.mu.Lock()
				//kv.snapshotIndex = applyMsg.SnapshotIndex
				if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
					// 返回true说明可以安装快照
					DPrintf("安装快照")
					kv.installSnapshot(applyMsg.Snapshot)
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) snapshotLoop() {
	for kv.killed() == false {
		if kv.maxraftstate == -1 {
			return
		}
		if kv.rf.RaftSizeCheck(kv.maxraftstate) {
			// 需要进行snapshot
			DPrintf("KVRaft[%v]'s size is big enough, snapshot!", kv.me)
			kv.mu.Lock()
			snapData := kv.kvByte()
			lastIndex := kv.snapshotIndex
			kv.mu.Unlock()
			// 调用rf的Snapshot()函数进行快照
			kv.rf.Snapshot(lastIndex, snapData)
		}
		time.Sleep(time.Millisecond * time.Duration(snapshotTimeout))
	}
}

func (kv *KVServer) kvByte() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(kv.kvData)
	_ = e.Encode(kv.clientDone)
	_ = e.Encode(kv.snapshotIndex)
	return w.Bytes()
}

func (kv *KVServer) installSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	kvData := make(map[string]string)
	clientDone := make(map[int64]int)
	var index int
	if d.Decode(&kvData) != nil ||
		d.Decode(&clientDone) != nil ||
		d.Decode(&index) != nil {
		DPrintf("Decode failed")
	} else {
		// decode成功
		kv.kvData = kvData
		kv.clientDone = clientDone
		kv.snapshotIndex = index
		DPrintf("KVRaft[%v] install snapshot successfully, kv is %v", kv.me, kv.kvData)
	}
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.kvData = make(map[string]string)
	kv.clientDone = make(map[int64]int)
	kv.snapshotIndex = 0
	// 获取snapshot中的数据
	kv.installSnapshot(persister.ReadSnapshot())
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.waitChs = make(map[int]chan Op)
	kv.cacheOp = make(map[int]Op)
	go kv.applyLoop()
	go kv.snapshotLoop()
	// You may need initialization code here.

	return kv
}
