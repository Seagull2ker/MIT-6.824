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
	"reflect"
	"sort"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"math/rand"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 常量的设置，包括状态对应的int，以及timeout时间
const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	// 设置选举时间的随机区间
	ElectionTimeoutMax = 400
	ElectionTimeoutMin = 250
	// 设置心跳的timeout
	HeartbeatTimeout = 100
	// 设置leader自检commit的timeout
	CheckCommitTimeout = 10
)

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
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
	currentTerm  int           // 当前term ID----------------通用持久数据，响应RPC前写入持久存储
	votedFor     int           // 给哪个candidate投票，默认为-1----------------通用持久数据，响应RPC前写入持久存储
	log          []LogEntry    // 日志记录----------------通用持久数据，响应RPC前写入持久存储
	getVoteNum   int           // 获得的票数
	updateNum    int           // 日志成功同步数
	state        int           // 当前的角色
	timer        *time.Timer   // 计时器
	//electionTime int           // election timeout
	heartbeat    chan bool     // 心跳
	commitIndex  int           // 最新的committed的log entry的index----------------通用易失数据
	lastApplied  int           // 最新的applied给上层状态机的log entry的index----------------通用易失数据
	nextIndex    []int         // leader记录的即将发给每个server的下一个log entry的index----------------leader的易失数据，选举成功后初始化
	matchIndex   []int         // leader记录的每个server复制的最高index----------------leader的易失数据，选举成功后初始化
	applyCond    *sync.Cond    // 条件变量，用于通知commitIndex更新
	applyCh      chan ApplyMsg // 结构体使用chan，由leader返回给tester
	//killCh		 chan bool	   // 用于结束进程
	//log的第一条日志保存lastSnapshotIndex和lastSnapshotTerm!!!!!!!!!!!!!!
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()
	return term, isleader
}

// 获取raft持久化状态数据的字节流
func (rf *Raft) raftStateByte() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.raftStateByte())
	//rf.persister.mu.Lock()
	//rf.persister.raftstate = rf.raftStateByte()
	//rf.persister.mu.Unlock()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var cTerm int
	var vote int
	var logs []LogEntry
	if d.Decode(&cTerm) != nil ||
		d.Decode(&vote) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("Decode failed")
	} else {
		// decode成功
		rf.mu.Lock()
		rf.currentTerm, rf.votedFor, rf.log = cTerm, vote, logs
		rf.commitIndex = logs[0].Index
		rf.lastApplied = logs[0].Index
		rf.mu.Unlock()
	}
}
// lab3中server判断是否需要快照
func (rf *Raft) RaftSizeCheck(longestSize int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.persister.RaftStateSize() >= longestSize {
		//DPrintf("RaftStateSize is %v, bigger than %v", rf.persister.RaftStateSize(), longestSize)
		return true
	}
	return false
}

// 获取log.Index为index的日志实际的索引值，没有则返回-1
func (rf *Raft) getIndex(index int) int {
	if index < rf.log[0].Index || index > rf.log[len(rf.log)-1].Index {
		return -1
	}
	return index - rf.log[0].Index
}
func (rf *Raft) getLogAtIndex(index int) LogEntry {
	i := rf.getIndex(index)
	if i != -1 {
		return rf.log[i]
	}
	return LogEntry{}
}

// 判断一个日志条目是否为空
func (log LogEntry) isEmptyLog() bool {
	if reflect.DeepEqual(log, LogEntry{}) {
		return true
	}
	return false
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// 如果上传的快照是旧的放弃并返回false，如果是最近的需要raft修剪日志并返回true
	// 查看网上教程才知道别处通过applyCh发送log entry的时候！！！！不要加锁！！！！[https://www.cnblogs.com/sun-lingyu/p/14591757.html]
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.commitIndex {
		// snapshot比follower的日志还要旧，不安装快照，返回false
		DPrintf("snapshot比follower[%v]的日志还要旧，不安装快照", rf.me)
		return false
	}
	if lastIncludedIndex >= rf.log[len(rf.log)-1].Index {
		// follower持有的日志已经在snapshot中了，甚至还有follower没有的日志，直接覆盖并重置log
		rf.log = rf.log[:1]
	} else {
		// follower日志中包含leader的snapshot中没有的日志，需要保留
		newLog := rf.log[rf.getIndex(lastIncludedIndex)+1:]
		rf.log = append(rf.log[:1], newLog...)
	}
	rf.log[0].Index = lastIncludedIndex
	rf.log[0].Term = lastIncludedTerm
	rf.commitIndex = lastIncludedIndex
	rf.applyCond.Broadcast()
	rf.lastApplied = lastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.raftStateByte(), snapshot)
	//DPrintf("After install snapshot, server[%v]'s commitIndex is %v, lastApplied is %v, log is %v", rf.me, rf.commitIndex, rf.lastApplied, rf.log)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// Snapshot函数是上层service每隔一段时间调用，让raft节点创建一个快照，参数snapshot是index处的command对应的字节流
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("index: %v, rf.log[0].Index: %v, rf.commitIndex: %v", index, rf.log[0].Index, rf.commitIndex)
	if index > rf.log[0].Index && index <= rf.commitIndex {
		snapTerm := rf.log[rf.getIndex(index)].Term
		// 更新log，注意！！保留index=0处的空日志
		newLog := rf.log[rf.getIndex(index)+1:]
		rf.log = append(rf.log[:1], newLog...)
		rf.log[0].Index = index
		rf.log[0].Term = snapTerm
		rf.persister.SaveStateAndSnapshot(rf.raftStateByte(), snapshot)
		DPrintf("Now, server[%v] isLeader: %v, Snapshot success, log is %v", rf.me, rf.state==LEADER, rf.log)
	}
}

// Snapshot RPC
type InstallSnapshotArgs struct {
	Term              int    // 领导人的任期号
	LeaderId          int    // 领导人Id
	LastIncludedIndex int    // 快照中包含的最后日志条目的索引值
	LastIncludedTerm  int    // 快照中包含的最后日志条目的任期
	Data              []byte // 从偏移量开始的快照分块的原始字节
}

type InstallSnapshotReply struct {
	Term int // 当前任期号（currentTerm），便于领导人更新自己
}

// 接受到leader的args
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.changeState(FOLLOWER)
		rf.persist()
	}
	// follower可以重置election timeout
	rf.timer.Reset(RandomizedElectionTimeout())
	rf.mu.Unlock()
	// 使用applyMsg通知上层状态机
	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	// 是否安装快照由上层service调用CondInstallSnapshot来决定
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate的term ID
	CandidateId  int // candidate的ID
	LastLogIndex int // candidate所持有的最后一条日志的index
	LastLogTerm  int // candidate所持有的最后一条日志的term ID
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 接收方的term ID
	VoteGranted bool // 接收方是否同意选票
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.changeState(FOLLOWER)
		rf.currentTerm = args.Term
		rf.persist()
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			// 同意投票重置election timeout
			rf.timer.Reset(RandomizedElectionTimeout())
		}
	}
}

type AppendEntriesArgs struct {
	Term         int        // leader的term ID
	LeaderId     int        // leader的ID
	PrevLogIndex int        // 正在备份的日志记录之前的log的index
	PrevLogTerm  int        // 正在备份的日志记录之前的log的term ID
	Entries      []LogEntry // 正在备份的log
	LeaderCommit int        // leader已经提交的最后一条日志的index
}

type AppendEntriesReply struct {
	Term          int  // 接收方的term ID
	Success       bool // Follower能够在自己的log中找到index值和Term ID与prevLogIndex和prevLogTerm相同的记录时为true
	ConflictTerm  int  // follower冲突条目的任期号
	ConflictIndex int  // 冲突条目的Index索引值
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = -2
		return
	} else if args.Term > rf.currentTerm {
		rf.changeState(FOLLOWER)
		rf.currentTerm = args.Term
		rf.persist()
	}
	reply.Term = rf.currentTerm
	go func() {
		rf.heartbeat <- true
	}()

	// 日志replication
	// false的情况
	if args.PrevLogIndex > 0 && args.PrevLogIndex >= rf.log[0].Index {
		if args.PrevLogIndex > rf.log[len(rf.log)-1].Index || (args.PrevLogIndex <= rf.log[len(rf.log)-1].Index && args.PrevLogTerm != rf.getLogAtIndex(args.PrevLogIndex).Term) {
			// 不匹配，false
			reply.Success = false
			pre := rf.getLogAtIndex(args.PrevLogIndex)
			if pre.isEmptyLog() {
				// PrevLogIndex超出了log范围
				reply.ConflictIndex = rf.log[len(rf.log)-1].Index + 1
				reply.ConflictTerm = -1
			} else {
				// PrevLogIndex处的日志term不匹配
				reply.ConflictTerm = pre.Term
				reply.ConflictIndex = rf.log[0].Index
				// 找到与ConflictTerm相等的第一个index
				for _, v := range rf.log {
					if v.Term == reply.ConflictTerm {
						reply.ConflictIndex = v.Index
						break
					}
				}
			}
			return
		}
	}
	// 有日志需要同步
	if len(args.Entries) != 0 {
		for i, logEntry := range args.Entries {
			if logEntry.Index <= rf.log[0].Index {
				continue
			}
			index := rf.getIndex(logEntry.Index) // follower原有日志的index
			if index == -1 {
				// 此时logEntry及之后的在follower的log中没有了
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
			if rf.log[index].Term != logEntry.Term {
				rf.log = append(rf.log[:index], args.Entries[i:]...)
				break
			} // term一样啥也不用做，继续向后比对Log
		}
		rf.persist()
	}
	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		// commitIndex更新的值为：LeaderCommit和最新日志条目索引值的|最小值|
		oldCommitIndex := rf.commitIndex
		if args.LeaderCommit > rf.log[len(rf.log)-1].Index {
			rf.commitIndex = rf.log[len(rf.log)-1].Index
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		// 通知applier线程更新lastApplied，并提交信息，如果commitIndex没有变就不用广播了
		for i := rf.commitIndex; i > oldCommitIndex; i-- {
			if rf.getLogAtIndex(i).Term == rf.currentTerm { // 确保term一致
				rf.commitIndex = i
				rf.applyCond.Broadcast()
				break
			}
		}
	}
	reply.Success = true
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return -1, -1, false
	}
	term = rf.currentTerm
	isLeader = true
	// 先添加到自己的log中，这里不用persist
	index = rf.log[len(rf.log)-1].Index + 1
	rf.log = append(rf.log, LogEntry{command, term, index})
	rf.mu.Unlock()
	rf.backupLog()

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
	// 网上race: limit on 8128 simultaneously alive goroutines is exceeded, dying
	rf.timer.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 修改状态函数, 注意状态改变时候是否需要修改其他属性!!!!!!!!!!!!，比如follower转为follower但是要更新votedFor
func (rf *Raft) changeState(state int) {
	switch state {
	case FOLLOWER:
		rf.state = FOLLOWER
		rf.votedFor = -1
	case CANDIDATE:
		rf.state = CANDIDATE
	case LEADER:
		rf.state = LEADER
		// 选举成功，先初始化nextIndex[]和matchIndex[]
		for i := range rf.peers {
			rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
			rf.matchIndex[i] = 0
		}
	default:
		panic("state error")
	}
}

// 选举函数
func (rf *Raft) startElection() {
	rf.mu.Lock()
	// timer已经超时，channel为空，ok
	rf.timer.Reset(RandomizedElectionTimeout())
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.getVoteNum = 1
	rf.persist()
	rf.mu.Unlock()
	// 给每个server发送requestVote
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				rf.mu.Lock()
				cTerm := rf.currentTerm
				lastIndex := rf.log[len(rf.log)-1].Index
				lastTerm := rf.log[len(rf.log)-1].Term
				rf.mu.Unlock()
				args := &RequestVoteArgs{
					cTerm,
					rf.me,
					lastIndex,
					lastTerm,
				}
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(i, args, reply) {
					rf.mu.Lock()
					if rf.state != CANDIDATE || rf.currentTerm != cTerm {
						rf.mu.Unlock()
						return
					}
					if reply.Term > rf.currentTerm {
						//DPrintf("candidate server[%v]'s term is less than server[%v], back to follower", rf.me, i)
						rf.currentTerm = reply.Term
						rf.changeState(FOLLOWER)
						rf.persist()
					} else if reply.VoteGranted {
						rf.getVoteNum += 1
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

// 心跳函数
func (rf *Raft) heartBeat() {
	//DPrintf("leader server[%v] start broadcast to send heartbeat, time is %vms", rf.me, time.Now().UnixNano()/1e6)
	for i := range rf.peers {
		// 循环发送heart beat包
		if i != rf.me {
			go rf.sendHeartbeat(i)
		}
	}
}

// i是发送的目标follower的id
func (rf *Raft) sendHeartbeat(i int) {
	rf.mu.Lock()
	lastSnapIndex := rf.log[0].Index
	cTerm := rf.currentTerm
	prevIndex := rf.nextIndex[i] - 1
	comIndex := rf.commitIndex
	rf.mu.Unlock()
	if prevIndex >= lastSnapIndex {
		// prev处的日志还在log中，发送普通的AppendEntries RPC
		rf.mu.Lock()
		prevTerm := rf.getLogAtIndex(prevIndex).Term
		rf.mu.Unlock()
		args := &AppendEntriesArgs{
			cTerm,
			rf.me,
			prevIndex,
			prevTerm,
			[]LogEntry{},
			comIndex,
		}
		reply := &AppendEntriesReply{}
		if rf.sendAppendEntries(i, args, reply) {
			//DPrintf("leader server[%v] send heartbeat to server[%v], args is %v", rf.me, i, args)
			rf.mu.Lock()
			if cTerm != rf.currentTerm || rf.state != LEADER { // 处理心跳时term已经变化，不再处理心跳
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.currentTerm {
				//DPrintf("follower server[%v]'s term is %v ,bigger than leader server[%v]'s term %v, back to follower", i, reply.Term, rf.me, rf.currentTerm)
				rf.currentTerm = reply.Term
				rf.changeState(FOLLOWER)
				rf.persist()
				rf.mu.Unlock()
				return
			}
			if reply.Success == true {
				// 心跳发送成功
				//DPrintf("follower server[%v] success get heartbeat, matchIndex is %v, nextIndex is %v", i, rf.matchIndex[i], rf.nextIndex[i])
			} else {
				if reply.ConflictTerm == -1 {
					// 在follower中没有找到index相同的log，nextIndex退回到follower日志的最后
					rf.nextIndex[i] = reply.ConflictIndex
				} else if reply.ConflictTerm != -2 {
					// 在follower中找到了index相同的log，但是term不匹配
					rf.nextIndex[i] = reply.ConflictIndex
					for j := len(rf.log) - 1; j >= 0; j-- {
						if rf.log[j].Term == reply.ConflictTerm {
							// rf.nextIndex[i]设为最后一个term日志的index+1
							rf.nextIndex[i] = rf.log[j].Index + 1
							break
						} else if rf.log[j].Term < reply.ConflictTerm {
							// leader的log中没有该term的日志
							rf.nextIndex[i] = reply.ConflictIndex
							break
						}
					}
				}
			}
			rf.mu.Unlock()
		}
	}
}

// 备份log函数
func (rf *Raft) backupLog() {
	rf.mu.Lock()
	rf.updateNum = 1
	rf.mu.Unlock()
	//DPrintf("leader server[%v] start broadcast to send new log, time is %vms", rf.me, time.Now().UnixNano()/1e6)
	for i := range rf.peers {
		// 循环发送带信息的AppendEntries包
		if i != rf.me {
			go rf.sendUpdate(i)
		}
	}
}

// i是发送的目标follower的id
func (rf *Raft) sendUpdate(i int) {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		// 不断发送备份消息
		lastIndex := rf.log[len(rf.log)-1].Index
		nIndex := rf.nextIndex[i]
		lastSnapIndex := rf.log[0].Index
		cTerm := rf.currentTerm
		prevIndex := nIndex - 1
		leaderCommit := rf.commitIndex
		rf.mu.Unlock()
		if lastIndex >= nIndex && prevIndex >= lastSnapIndex {
			// 如果leader最后的日志比nextIndex更新，并且leader日志中有nextIndex及之后的日志，发送普通的AppendEntries RPC就行
			rf.mu.Lock()
			prevTerm := rf.getLogAtIndex(prevIndex).Term
			newLogEntries := rf.log[rf.getIndex(nIndex):]
			rf.mu.Unlock()
			// leader的最后一条日志比i的nextIndex更新，需要发送更新Append
			args := &AppendEntriesArgs{
				cTerm,
				rf.me,
				prevIndex,     // 正在备份的日志记录之前的log的index
				prevTerm,      // 正在备份的日志记录之前的log的term
				newLogEntries, // 正在备份的log
				leaderCommit,  // leader已经提交的最后一条日志的index
			}
			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(i, args, reply) {
				//DPrintf("server[%v] send update to server[%v], arg is %v", rf.me, i, args)
				rf.mu.Lock()
				if cTerm != rf.currentTerm || rf.state != LEADER { // 处理心跳时term已经变化，不再处理心跳
					//DPrintf("server[%v]'s term had change, don't deal with rpc", i)
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.currentTerm {
					//DPrintf("follower server[%v]'s term is %v ,bigger than leader server[%v]'s term %v, back to follower", i, reply.Term, rf.me, rf.currentTerm)
					rf.currentTerm = reply.Term
					rf.changeState(FOLLOWER)
					rf.persist()
					rf.mu.Unlock()
					return
				}
				// 判断备份结果
				if reply.Success {
					// leader的日志在备份到大部分节点后进行持久化，保证leader还没更新commit时crash丢掉该日志!!!!!!!!!!!!!!
					rf.updateNum += 1
					if rf.updateNum > len(rf.peers)/2 {
						rf.persist()
					}
					// 备份成功，更新该follower的nextIndex和matchIndex
					rf.matchIndex[i] = prevIndex + len(args.Entries) // 更新matchIndex
					rf.nextIndex[i] = rf.matchIndex[i] + 1           // 更新nextIndex
					//DPrintf("follower server[%v] success update, matchIndex is %v, nextIndex is %v", i, rf.matchIndex[i], rf.nextIndex[i])
					rf.mu.Unlock()
					return // 备份成功，结束循环
				} else {
					//DPrintf("follower server[%v] update fail, change nextIndex and retry", i)
					// 优化nextIndex，每次-1消耗太大，可以在AppendEntriesReply中添加冲突条目的任期号和Index索引，借助这些直接更新nextIndex[i]
					if reply.ConflictTerm == -1 {
						// 在follower中没有找到index相同的log，nextIndex退回到follower日志的最后
						rf.nextIndex[i] = reply.ConflictIndex
					} else if reply.ConflictTerm != -2 {
						// 在follower中找到了index相同的log，但是term不匹配
						rf.nextIndex[i] = reply.ConflictIndex
						for j := len(rf.log) - 1; j >= 0; j-- {
							if rf.log[j].Term == reply.ConflictTerm {
								// rf.nextIndex[i]设为最后一个term日志的index+1
								rf.nextIndex[i] = rf.log[j].Index + 1
								break
							} else if rf.log[j].Term < reply.ConflictTerm {
								// leader的log中没有该term的日志
								rf.nextIndex[i] = reply.ConflictIndex
								break
							}
						}
					}
				}
				rf.mu.Unlock()
			}
		} else if lastIndex >= nIndex && prevIndex < lastSnapIndex {
			// 如果nextIndex后部分日志已经被快照删除了，发送InstallSnapShot RPC
			rf.mu.Lock()
			lastIncludedIndex := rf.log[0].Index
			lastIncludedTerm := rf.log[0].Term
			snapShotData := rf.persister.ReadSnapshot()
			rf.mu.Unlock()
			args := &InstallSnapshotArgs{
				cTerm,
				rf.me,
				lastIncludedIndex,
				lastIncludedTerm,
				snapShotData,
			}
			reply := &InstallSnapshotReply{}
			//DPrintf("server[%v] send InstallSnapshot to server[%v], arg is %v", rf.me, i, args)
			if rf.sendInstallSnapshot(i, args, reply) {
				// 发送快照InstallSnapshot
				rf.mu.Lock()
				if cTerm != rf.currentTerm || rf.state != LEADER {
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.currentTerm {
					//DPrintf("follower server[%v]'s term is %v ,bigger than leader server[%v]'s term %v, back to follower", i, reply.Term, rf.me, rf.currentTerm)
					rf.currentTerm = reply.Term
					rf.changeState(FOLLOWER)
					rf.persist()
					rf.mu.Unlock()
					return
				}
				// 备份成功，更新该follower的nextIndex和matchIndex
				//DPrintf("follower server[%v] success snapshot", i)
				rf.matchIndex[i] = args.LastIncludedIndex // 更新matchIndex
				rf.nextIndex[i] = rf.matchIndex[i] + 1    // 更新nextIndex
				rf.mu.Unlock()
			}
		} else { // 说明leader的log没有比server[i]更新，return退出循环
			return
		}
		//  else if lastIndex <= prevIndex {
		//	// leader日志不比follower的日志新，发送普通的AppendEntries RPC心跳包
		//	rf.mu.Lock()
		//	prevTerm := rf.getLogAtIndex(prevIndex).Term
		//	rf.mu.Unlock()
		//	args := &AppendEntriesArgs{
		//		cTerm,
		//		rf.me,
		//		prevIndex,
		//		prevTerm,
		//		[]LogEntry{},
		//		leaderCommit,
		//	}
		//	reply := &AppendEntriesReply{}
		//	if rf.sendAppendEntries(i, args, reply) {
		//		DPrintf("leader server[%v] send heartbeat to server[%v], args is %v", rf.me, i, args)
		//		rf.mu.Lock()
		//		if cTerm != rf.currentTerm || rf.state != LEADER { // 处理心跳时term已经变化，不再处理心跳
		//			rf.mu.Unlock()
		//			return
		//		}
		//		if reply.Term > rf.currentTerm {
		//			DPrintf("follower server[%v]'s term is %v ,bigger than leader server[%v]'s term %v, back to follower", i, reply.Term, rf.me, rf.currentTerm)
		//			rf.currentTerm = reply.Term
		//			rf.changeState(FOLLOWER)
		//			rf.persist()
		//			rf.mu.Unlock()
		//			return
		//		}
		//		if reply.Success == true {
		//			// 心跳发送成功
		//			DPrintf("follower server[%v] success get heartbeat, matchIndex is %v, nextIndex is %v", i, rf.matchIndex[i], rf.nextIndex[i])
		//			rf.mu.Unlock()
		//			return
		//		} else {
		//			if reply.ConflictTerm == -1 {
		//				// 在follower中没有找到index相同的log，nextIndex退回到follower日志的最后
		//				rf.nextIndex[i] = reply.ConflictIndex
		//			} else {
		//				// 在follower中找到了index相同的log，但是term不匹配
		//				rf.nextIndex[i] = rf.log[0].Index
		//				for j := len(rf.log) - 1; j >= 1; j-- {
		//					if rf.log[j].Term == reply.ConflictTerm {
		//						// rf.nextIndex[i]设为最后一个term日志的index+1
		//						rf.nextIndex[i] = rf.log[j].Index + 1
		//						break
		//					} else if rf.log[j].Term < reply.ConflictTerm {
		//						// leader的log中没有该term的日志
		//						rf.nextIndex[i] = reply.ConflictIndex
		//						break
		//					}
		//				}
		//			}
		//		}
		//		rf.mu.Unlock()
		//	}
		//} else { // 说明leader的log没有比server[i]更新，return退出循环
		//	return
		//}
	}
}

// 生成election timeout的随机函数
func RandomizedElectionTimeout() time.Duration {
	//rand.Seed(int64(me) * time.Now().Unix())
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)+ElectionTimeoutMin) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case FOLLOWER:
			select {
			case <-rf.heartbeat:
				// 有心跳
				rf.mu.Lock()
				// timer未超时，channel为空，ok
				rf.timer.Reset(RandomizedElectionTimeout())
				//DPrintf("server[%v] get heartbeat, commitIndex is %v", rf.me, rf.commitIndex)
				rf.mu.Unlock()
			case <-rf.timer.C:
				// 超时，成为candidate
				rf.mu.Lock()
				//DPrintf("server[%v] election timeout, become candidate, term is %v, lastLogIndex is %v, lastLogTerm is %v", rf.me, rf.currentTerm+1, rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term)
				rf.changeState(CANDIDATE)
				rf.mu.Unlock()
				rf.startElection() // 开始选举
			}
		case CANDIDATE:
			select {
			case <-rf.heartbeat:
				// 收到心跳，说明有leader，回到follower
				//DPrintf("server[%v] get heartbeat, become follower", rf.me)
				rf.mu.Lock()
				// timer未超时，channel为空，ok
				rf.timer.Reset(RandomizedElectionTimeout())
				rf.changeState(FOLLOWER)
				rf.persist()
				rf.mu.Unlock()
			case <-rf.timer.C:
				// election timeout超时，重新选举
				rf.mu.Lock()
				//DPrintf("server[%v] election timeout, restart election, term is %v, lastLogIndex is %v, lastLogTerm is %v", rf.me, rf.currentTerm+1, rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term)
				rf.mu.Unlock()
				rf.startElection()
			default:
				rf.mu.Lock()
				if rf.getVoteNum > len(rf.peers)/2 {
					//DPrintf("candidate server[%v] get more than half of votes, become leader", rf.me)
					rf.changeState(LEADER)
				}
				rf.mu.Unlock()
			}
		case LEADER:
			rf.heartBeat()
			rf.backupLog()
			time.Sleep(time.Duration(HeartbeatTimeout) * time.Millisecond)
		default:
			panic("Invalid peer state!")
		}
	}
}

// 判断节点现在能否apply
func (rf *Raft) checkApply() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > rf.lastApplied {
		return true
	}
	return false
}

// 每个节点都有的循环保持[lastApplied,commitIndex]窗口的协程
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.applyCond.L.Lock()
		for !rf.checkApply() {
			rf.applyCond.Wait()
			if rf.killed() {
				return
			}
		}
		// 当commitIndex>lastApplied时上述for退出，执行下面代码
		// 把lastApplied+1，并把log[lastApplied]应用到上层状态机上
		// 优化：批量apply，减少rpc数量
		rf.mu.Lock()
		firstApplyIndex, lastApplyIndex := rf.lastApplied, rf.commitIndex
		applyEntries := make([]LogEntry, lastApplyIndex-firstApplyIndex)
		copy(applyEntries, rf.log[rf.getIndex(firstApplyIndex)+1:rf.getIndex(lastApplyIndex)+1])
		rf.mu.Unlock()
		// 开始快速apply日志，不能加锁！！！！
		for _, entry := range applyEntries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		if lastApplyIndex > rf.lastApplied {
			rf.lastApplied = lastApplyIndex
		}
		rf.mu.Unlock()
		//DPrintf("server[%v] has <- applyCh the log before %v, commitIndex is %v", rf.me, rf.lastApplied, rf.commitIndex)
		rf.applyCond.L.Unlock()
	}
}

// leader检查更新自己的commitIndex
func (rf *Raft) checkCommit() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			time.Sleep(time.Duration(HeartbeatTimeout) * time.Millisecond)
			continue
		}
		tmp := make([]int, len(rf.matchIndex))
		copy(tmp, rf.matchIndex)
		tmp[rf.me] = rf.log[len(rf.log)-1].Index
		sort.Ints(tmp)
		nMax := tmp[len(tmp)-len(tmp)/2-1]
		N := rf.commitIndex
		for n := rf.log[len(rf.log)-1].Index; n > rf.commitIndex; n-- {
			if n <= nMax && rf.getLogAtIndex(n).Term == rf.currentTerm {
				N = n
				break
			}
		}
		if N > rf.commitIndex {
			rf.commitIndex = N
			rf.applyCond.Broadcast()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(CheckCommitTimeout) * time.Millisecond)
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
	rf.mu.Lock()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{nil, 0, 0}) // 占位Log
	rf.getVoteNum = 0
	rf.state = FOLLOWER
	rf.timer = time.NewTimer(RandomizedElectionTimeout())
	rf.heartbeat = make(chan bool, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&sync.Mutex{})
	//rf.killCh = make(chan bool, 1)
	rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to apply to state machine
	go rf.applier()
	go rf.checkCommit()

	return rf
}
