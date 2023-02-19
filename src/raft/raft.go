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
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)

import "labrpc"

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

func (rf *Raft) applyMsg() {
	//====================================如果commitIndex > lastApplied:=================================================
	for rf.commitIndex > rf.lastApplied {
		//====================================1. 增加lastApplied=========================================================
		rf.lastApplied++
		//================================2. 将log[lastApplied]提交到状态机================================================
		rf.applyCh <- ApplyMsg{
			Index:   rf.lastApplied,
			Command: rf.logs[rf.lastApplied].Command,
		}
	}
}

type State int

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

const MinTimeout = 350
const MaxTimeout = 500
const HeartbeatTimeout = 80

type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//-------------------------------Persistent state on all servers:---------------------------------------------------
	//---------------------(Updated on stable storage before responding to RPCs)----------------------------------------
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	logs        []LogEntry // log entries;
	// each entry contains command for state machine, and term when entry was received by leader

	//--------------------------------Volatile state on all servers:----------------------------------------------------
	commitIndex int // index of the highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of the highest log entry applied to state machine (initialized to 0, increases monotonically)

	//-----------------------------------Volatile state on leaders:-----------------------------------------------------
	//---------------------------------(Reinitialized after election)---------------------------------------------------
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server(initialized to 0, increases monotonically)

	state        State
	timer        *time.Timer
	voteCount    int       // for leader
	resetTimerCh chan bool // for follower
	applyCh      chan ApplyMsg
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// Arguments:
//		term			candidate’s term
//		candidateId		candidate requesting vote
//		lastLogIndex	index of candidate’s last log entry
//		lastLogTerm		term of candidate’s last log entry
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// Results:
//		term			currentTerm, for candidate to update itself
//		voteGranted		true means candidate received vote
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

//=================如果RPC请求或者响应包含的term T > currentTerm：设置currentTerm = T，状态转换为follower=======================
func (rf *Raft) updateTerm(T int) {
	rf.currentTerm = T
	rf.votedFor = -1
	rf.persist()
	rf.asFollower()
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// ================如果RPC请求或者响应包含的term T > currentTerm：设置currentTerm = T，状态转换为follower===================
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	//============================================返回false如果term < currentTerm=========================================
	if args.Term >= rf.currentTerm {
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].Term
		//=======================================如果votedFor字段是null或者candidateId=====================================
		requirement1 := rf.votedFor == -1 || rf.votedFor == args.CandidateId
		//===============================================并且============================================================
		//======================================candidate的log至少与接收者的一样新==========================================
		requirement2 := (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
		//======================================那么授予投票，设置voteGranted为true=========================================
		if requirement1 && requirement2 {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			if rf.state == FOLLOWER { //只对投票的follower重置定时器
				rf.resetTimerCh <- true
			}
		}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.state == CANDIDATE {
		//=====================如果RPC请求或者响应包含的term T > currentTerm：设置currentTerm = T，状态转换为follower===========
		if reply.Term > rf.currentTerm {
			rf.updateTerm(reply.Term)
		} else {
			if reply.VoteGranted && reply.Term == rf.currentTerm {
				rf.voteCount++
				//==============================如果接收到大多数服务器的投票，转换为leader=====================================
				if rf.voteCount > len(rf.peers)/2 {
					rf.asLeader()
				}
			}
		}
	}
	return ok
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	T := args.Term
	switch rf.state {
	case FOLLOWER:
		if T > rf.currentTerm {
			rf.currentTerm = T
			rf.votedFor = -1
			rf.persist()
		}
	case CANDIDATE:
		if T >= rf.currentTerm {
			rf.currentTerm = T
			rf.asFollower()
		}
	case LEADER:
		if T > rf.currentTerm {
			rf.currentTerm = T
			rf.asFollower()
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = false

	// -----------------------------------------1. 返回false如果term < currentTerm----------------------------------------
	if T < rf.currentTerm {
		reply.NextIndex = args.PrevLogIndex + 1
		return
	}

	// ------------------2. 返回false如果log不包含一个索引是prevLogIndex并且term是prevLogTerm的entry--------------------------
	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		if rf.state == FOLLOWER { // 只对follower重置定时器
			rf.resetTimerCh <- true
		}
		// lab3: optimize
		if args.PrevLogIndex >= len(rf.logs) {
			reply.NextIndex = len(rf.logs)
		} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			conflictTerm := rf.logs[args.PrevLogIndex].Term
			firstIndex := args.PrevLogIndex
			for firstIndex > 0 && rf.logs[firstIndex].Term == conflictTerm {
				firstIndex--
			}
			reply.NextIndex = firstIndex + 1
		}
		return
	}

	// ---------3. 如果一个已经存在entry与一个新的entry冲突（相同index但是不同term），那么删除已经存在的entry以及所有之后的entry--------
	if args.PrevLogIndex+1 < len(rf.logs) {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
	}

	// -----------------------------------------4. 添加所有不在log中的entries----------------------------------------------
	rf.logs = append(rf.logs, args.Entries...)
	rf.persist()

	// ---------5. 如果leaderCommit > commitIndex那么设置commitIndex = min(leaderCommit, index of last new entry)---------
	if args.LeaderCommit > rf.commitIndex {
		min := func(x int, y int) int {
			if x < y {
				return x
			} else {
				return y
			}
		}
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
		rf.applyMsg()
	}

	reply.Success = true
	reply.NextIndex = len(rf.logs)

	if rf.state == FOLLOWER { // 只对follower重置定时器
		rf.resetTimerCh <- true
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		//*********************************************lock*************************************************************
		rf.mu.Lock()
		if rf.state == LEADER {
			if reply.Success && reply.Term == rf.currentTerm {
				//=============================如果成功，更新那个follower的nextIndex和matchIndex============================
				rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				//====================================如果存在一个N使得====================================================
				//==================================1. N > commitIndex==================================================
				for N := len(rf.logs) - 1; N > rf.commitIndex; N-- {
					//==============================2. log[N].term == currentTerm=======================================
					if rf.logs[N].Term == rf.currentTerm {
						count := 1
						for i, v := range rf.matchIndex {
							if i != rf.me && v >= N {
								count++
							}
						}
						//==========================3. 大多数matchIndex[i] >= N==========================================
						if count > len(rf.peers)/2 {
							//============================那么设置commitIndex = N========================================
							rf.commitIndex = N
							break
						}
					}
				}
				rf.applyMsg()
			} else {
				//============如果RPC请求或者响应包含的term T > currentTerm：设置currentTerm = T，状态转换为follower============
				if rf.currentTerm < reply.Term {
					rf.updateTerm(reply.Term)
				} else {
					//===================================如果由于log不一致失败，减少nextIndex然后重试=========================
					rf.nextIndex[server] = reply.NextIndex
				}
			}
		}
		rf.mu.Unlock()
		//**********************************************unlock**********************************************************
	}
	return ok
}

func getRandomTimeout() time.Duration {
	return time.Duration(rand.Intn(MaxTimeout-MinTimeout)+MinTimeout) * time.Millisecond
}

func (rf *Raft) followerLoop() {
	for rf.state == FOLLOWER {
		select {
		case reset := <-rf.resetTimerCh:
			if reset {
				rf.timer = time.NewTimer(getRandomTimeout())
			}
		//======如果直到election timeout都没有收到来自当前leader的AppendEntries RPC或者没有向candidate投票，转换为candidate=======
		case <-rf.timer.C:
			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.resetTimerCh = make(chan bool, 5)
			rf.mu.Unlock()
			go rf.candidateLoop()
		}
	}
}

func (rf *Raft) candidateLoop() {
	for rf.state == CANDIDATE {
		rf.mu.Lock()
		if rf.state != CANDIDATE {
			rf.mu.Unlock()
			return
		}
		//=======================================只要转换为candidate就应该开始election======================================
		//===========================================1. 增加currentTerm字段===============================================
		rf.currentTerm++
		//=============================================2. 为自己投票======================================================
		rf.votedFor = rf.me
		rf.voteCount = 1
		//=========================================3. 重置election定时器==================================================
		rf.timer = time.NewTimer(getRandomTimeout())
		rf.persist()
		//====================================4. 发送RequestVote RPC给所有其他服务器========================================
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.logs) - 1,
					LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
				}
				reply := &RequestVoteReply{}
				go rf.sendRequestVote(i, args, reply)
			}
		}
		rf.mu.Unlock()
		//==================================如果election timeout超时，开始一次新的election==================================
		if rf.state == CANDIDATE {
			<-rf.timer.C // 阻塞等待election timeout超时
		}
	}
}

func (rf *Raft) leaderLoop() {
	//************************************************lock**************************************************************
	rf.mu.Lock()
	if rf.state == LEADER {
		//====================开始election后，发送初始的空AppendEntries RPC(heartbeat)给每个服务器============================
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
					Entries:      make([]LogEntry, 0),
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				go rf.sendAppendEntries(i, args, reply)
			}
		}
		rf.timer = time.NewTimer(time.Duration(HeartbeatTimeout) * time.Millisecond)
	}
	rf.mu.Unlock()
	//********************************************unlock****************************************************************

	if rf.state == LEADER {
		<-rf.timer.C
	}

	for rf.state == LEADER {
		//************************************************Lock**********************************************************
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		//============================如果last log index >= 某个follower的nextIndex=======================================
		//==============那么发送AppendEntries RPC(携带从nextIndex开始的log entries)给那个follower============================
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
					Entries:      rf.logs[rf.nextIndex[i]:],
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				go rf.sendAppendEntries(i, args, reply)
			}
		}
		//=========================================全部发送完成之后重置定时器================================================
		if rf.state == LEADER {
			rf.timer = time.NewTimer(time.Duration(HeartbeatTimeout) * time.Millisecond)
		}
		rf.mu.Unlock()
		//*********************************************unlock***********************************************************

		if rf.state == LEADER {
			<-rf.timer.C
		}
	}
}

func (rf *Raft) asFollower() {
	if rf.state != FOLLOWER {
		rf.state = FOLLOWER
		rf.timer = time.NewTimer(getRandomTimeout())
		rf.voteCount = 0
		rf.votedFor = -1
		rf.resetTimerCh = make(chan bool, 5)
		rf.persist()
		go rf.followerLoop()
	}
}

func (rf *Raft) asLeader() {
	if rf.state != LEADER {
		rf.state = LEADER
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.logs)
			rf.matchIndex[i] = 0
		}
		go rf.leaderLoop()
	}
}

// Make
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	var rf = &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		currentTerm:  0,
		votedFor:     -1,
		state:        FOLLOWER,
		timer:        time.NewTimer(getRandomTimeout()),
		resetTimerCh: make(chan bool, 5),
		commitIndex:  0,
		voteCount:    0,
		lastApplied:  0,
		applyCh:      applyCh,
	}

	rf.logs = append(rf.logs, LogEntry{-1, nil})
	rf.readPersist(persister.ReadRaftState())

	go rf.followerLoop()
	return rf
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise, start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.state == LEADER
	term = rf.currentTerm

	if isLeader {
		rf.logs = append(rf.logs, LogEntry{
			Term:    term,
			Command: command,
		})
		rf.persist()
		index = len(rf.logs) - 1
	}

	return index, term, isLeader
}

// Kill
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}
