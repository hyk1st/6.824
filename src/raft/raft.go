package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

//var Rand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

//
//this is an outline of the API that raft must expose to
//the service (or tester). see comments below for
//each of these functions for more details.
//
//rf = Make(...)
//  create a new Raft server.
//rf.Start(command interface{}) (index, term, isleader)
//  start agreement on a new log entry
//rf.GetState() (term, isLeader)
//  ask a Raft for its current term, and whether it thinks it is leader
//ApplyMsg
//  each time a new entry is committed to the log, each Raft peer
//  should send an ApplyMsg to the service (or tester)
//  in the same server.
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

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER
)

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	CurrentTerm int //当前任期
	VotedFor    int //当前任期投票给的候选人，若没投出去，则为-1
	state       int //2A rf此时的状态，FOLLOWER,CANDIDATE or LEADER
	//changeChan      chan int   // 状态发生变化时，用以通知tiker函数
	commitIndex     int        //2 已知的最大的已经被提交的日志条目的索引值
	CurrentLogIndex int        // 已知的日志条目索引
	Logs            []LogEntry //日志条目
	LastLogIndex    int        // 快照包含的最后的日志索引
	LastLogTerm     int        // 快照包含的最后的日志任期
	lastApplied     int        //2 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）
	nextIndex       []int      //2 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	matchIndex      []int      //2 对于每一个服务器，已经复制给他的日志的最高索引值
	applyCh         chan ApplyMsg
	heartTime       *time.Timer
	selectTime      *time.Timer
	Rand            *rand.Rand
	applyCond       *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.state == LEADER
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.CurrentLogIndex)
	e.Encode(rf.LastLogIndex)
	e.Encode(rf.LastLogTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentLogIndex, CurrentTerm, VotedFor, LastLogIndex, LastLogTerm int
	d.Decode(&CurrentTerm)
	d.Decode(&VotedFor)
	rf.mu.Lock()
	d.Decode(&rf.Logs)
	d.Decode(&CurrentLogIndex)
	d.Decode(&LastLogIndex)
	d.Decode(&LastLogTerm)
	rf.VotedFor = VotedFor
	rf.CurrentTerm = CurrentTerm
	rf.CurrentLogIndex = CurrentLogIndex
	rf.LastLogIndex = LastLogIndex
	rf.LastLogTerm = LastLogTerm
	rf.commitIndex = LastLogIndex
	rf.lastApplied = LastLogIndex
	rf.mu.Unlock()
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}
	temp := rf.LastLogIndex
	if rf.CurrentLogIndex > lastIncludedIndex {
		rf.Logs = rf.Logs[lastIncludedIndex-temp : rf.CurrentLogIndex-temp+1]
	} else {
		rf.Logs = []LogEntry{{Term: lastIncludedTerm, Index: lastIncludedIndex}}
	}
	rf.CurrentLogIndex = max(rf.CurrentLogIndex, lastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, lastIncludedIndex)
	rf.LastLogIndex = lastIncludedIndex
	rf.LastLogTerm = lastIncludedTerm
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.CurrentLogIndex)
	e.Encode(rf.LastLogIndex)
	e.Encode(rf.LastLogTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	rf.applyCond.Signal()
	return true
}

func (rf *Raft) GetLogSize() int {
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//fmt.Println("id: ", rf.me, " commit index: ", rf.commitIndex, "current index: ", rf.CurrentLogIndex, " lastlog: ", rf.LastLogIndex, " index: ", index, " len(log): ", len(rf.Logs))
	rf.mu.Lock()
	if index <= rf.LastLogIndex {
		return
	}
	temp := rf.LastLogIndex

	rf.CurrentLogIndex = max(rf.CurrentLogIndex, index)
	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)
	rf.LastLogIndex = index
	rf.LastLogTerm = rf.Logs[index-temp].Term
	rf.Logs = rf.Logs[index-temp : rf.CurrentLogIndex-temp+1]
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.CurrentLogIndex)
	e.Encode(rf.LastLogIndex)
	e.Encode(rf.LastLogTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	rf.mu.Unlock()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //2A 候选人的任期号
	CandidateId  int //2A 请求投票的候选人的ID
	LastLogIndex int //2A 候选人的最后日志条目的索引值
	LastLogTerm  int //2A 候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //是否投票给该候选人
}

type AppendEntriesArgs struct {
	Term         int        //2A LEADER的任期号
	LeaderId     int        //2A LEADER的id
	PrevLogIndex int        //2A 新的日志条目前的索引值
	PrevLogTerm  int        //2A 新的日志条目前的任期号
	Entries      []LogEntry //2A 准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
	LeaderCommit int        //2A 领导人已经提交的日志的索引值
}

type AppendEntriesReply struct {
	Term    int  //2A 当前的任期号，用于领导人去更新自己
	Success bool //2A 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
	Entries []LogEntry
}

type SnapshotReq struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type SnapshotResp struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if rf.CurrentTerm < args.Term {
		rf.selectTime.Reset(time.Duration((rf.Rand.Int()%150)+300) * time.Millisecond)
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.state = FOLLOWER
		//rf.persist()
	}
	if rf.CurrentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	} else if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && ((rf.Logs[rf.CurrentLogIndex-rf.LastLogIndex].Term < args.LastLogTerm) || ((rf.Logs[rf.CurrentLogIndex-rf.LastLogIndex].Term == args.LastLogTerm) && (rf.CurrentLogIndex <= args.LastLogIndex))) {
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
		rf.VotedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	}
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) InstallSnapshot(args *SnapshotReq, reply *SnapshotResp) {
	rf.mu.Lock()
	//fmt.Println("install snapshot")
	reply.Term = rf.CurrentTerm
	if reply.Term > args.Term {
		rf.mu.Unlock()
		return
	}
	if args.LastIncludeIndex <= rf.LastLogIndex {
		rf.mu.Unlock()
		return
	}
	//temp := rf.LastLogIndex
	//if rf.CurrentLogIndex > args.LastIncludeIndex {
	//	rf.Logs = rf.Logs[args.LastIncludeIndex-temp : rf.CurrentLogIndex-temp+1]
	//} else {
	//	rf.Logs = []LogEntry{{Term: args.LastIncludeTerm, Index: args.LastIncludeIndex}}
	//}
	//rf.CurrentLogIndex = max(rf.CurrentLogIndex, args.LastIncludeIndex)
	//rf.commitIndex = max(rf.commitIndex, args.LastIncludeIndex)
	//rf.lastApplied = max(rf.lastApplied, args.LastIncludeIndex)
	//rf.LastLogIndex = args.LastIncludeIndex
	//rf.LastLogTerm = args.LastIncludeTerm
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.state = FOLLOWER
		rf.persist()
	}
	rf.selectTime.Reset(time.Duration((rf.Rand.Int()%150)+300) * time.Millisecond)
	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//e.Encode(rf.CurrentTerm)
	//e.Encode(rf.VotedFor)
	//e.Encode(rf.Logs)
	//e.Encode(rf.CurrentLogIndex)
	//e.Encode(rf.LastLogIndex)
	//e.Encode(rf.LastLogTerm)
	//data := w.Bytes()
	//rf.persister.SaveStateAndSnapshot(data, args.Data)
	rf.mu.Unlock()
	//rf.applyCond.Signal()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludeIndex,
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

func (rf *Raft) sendInstallSnapshot(server int, args *SnapshotReq, reply *SnapshotResp) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	rf.mu.Lock()
	isLeader = rf.state == LEADER
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	term = rf.CurrentTerm
	rf.CurrentLogIndex++
	index = rf.CurrentLogIndex - rf.LastLogIndex
	if len(rf.Logs) > index {
		rf.Logs[index] = LogEntry{Term: term, Index: rf.CurrentLogIndex, Command: command}
	} else {
		rf.Logs = append(rf.Logs, LogEntry{Term: term, Index: rf.CurrentLogIndex, Command: command})
	}
	index = rf.CurrentLogIndex
	rf.persist()
	rf.mu.Unlock()
	go rf.sendHeart()
	return index, term, isLeader
}

func max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
func (rf *Raft) selectLeader() {
	var agreeNum, flag atomic.Int32
	agreeNum.Store(1)
	flag.Store(1)
	rf.mu.Lock()
	req := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.CurrentLogIndex,
		LastLogTerm:  rf.Logs[rf.CurrentLogIndex-rf.LastLogIndex].Term,
	}
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(id int, req RequestVoteArgs) {
			if rf.state != CANDIDATE || rf.CurrentTerm != req.Term {
				return
			}
			resp := &RequestVoteReply{}
			ok := rf.sendRequestVote(id, &req, resp)
			if !ok {
				ok = rf.sendRequestVote(id, &req, resp)
				if !ok {
					return
				}
			}
			rf.mu.Lock()
			if resp.VoteGranted {
				agreeNum.Add(1)
				if agreeNum.Load() > int32(len(rf.peers)>>1) && flag.Load() == 1 {
					flag.Store(0)
					if rf.state == CANDIDATE && rf.CurrentTerm == req.Term {
						rf.selectTime.Reset(1<<63 - 1)
						rf.heartTime.Reset(50 * time.Millisecond)
						rf.state = LEADER
						rf.persist()
						for ind, _ := range rf.peers {
							rf.nextIndex[ind] = rf.CurrentLogIndex + 1
							rf.matchIndex[ind] = 0
						}
						rf.mu.Unlock()
						go rf.sendHeart()
						return
					}
				}
			} else {
				if resp.Term > rf.CurrentTerm {
					rf.selectTime.Reset(time.Duration((rf.Rand.Int()%150)+300) * time.Millisecond)
					rf.CurrentTerm = resp.Term
					rf.VotedFor = -1
					rf.state = FOLLOWER
					rf.persist()
				}
			}
			rf.mu.Unlock()

		}(i, *req)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Entries = nil
	rf.mu.Lock()
	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.Success = false
	} else {
		if rf.CurrentLogIndex < args.PrevLogIndex || (rf.LastLogIndex < args.PrevLogIndex && rf.Logs[args.PrevLogIndex-rf.LastLogIndex].Term != args.PrevLogTerm) {
			reply.Term = rf.CurrentTerm
			reply.Success = false
			end := min(args.PrevLogIndex, rf.CurrentLogIndex)
			sqrtLen := int(math.Sqrt(float64(end - rf.commitIndex + 1)))
			reply.Entries = make([]LogEntry, 0)
			for i := rf.commitIndex; i <= end; i += sqrtLen {
				reply.Entries = append(reply.Entries, rf.Logs[i-rf.LastLogIndex])
			}

		} else {
			for i, v := range args.Entries {
				index := i + 1 + args.PrevLogIndex
				if index <= rf.commitIndex || index <= rf.LastLogIndex {
					continue
				}
				if index <= rf.CurrentLogIndex && v.Term == rf.Logs[index-rf.LastLogIndex].Term {
					continue
				}
				if len(rf.Logs) > index-rf.LastLogIndex {
					rf.Logs[index-rf.LastLogIndex] = v
				} else {
					rf.Logs = append(rf.Logs, v)
				}
				rf.CurrentLogIndex = index
				//rf.persist()
			}
			rf.commitIndex = max(rf.commitIndex, min(rf.CurrentLogIndex, args.LeaderCommit))
			rf.applyCond.Signal()
			reply.Term = rf.CurrentTerm
			reply.Success = true
		}
		rf.selectTime.Reset(time.Duration((rf.Rand.Int()%150)+300) * time.Millisecond)
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.VotedFor = -1
		}
		rf.state = FOLLOWER
		rf.persist()
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) sendHeart() {
	rf.mu.Lock()
	req := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	tag := rf.CurrentLogIndex
	rf.mu.Unlock()
	var cnt atomic.Int32
	cnt.Store(1)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(id int, req AppendEntriesArgs) {
			for {
				rf.mu.Lock()
				// || rf.CurrentTerm != req.Term
				if rf.state != LEADER || rf.CurrentTerm != req.Term {
					rf.mu.Unlock()
					break
				}
				end := tag
				nxt := rf.nextIndex[id]
				if rf.nextIndex[id] <= rf.LastLogIndex {
					//fmt.Println("send snapshot")
					reply := &SnapshotResp{}
					reqs := &SnapshotReq{
						Term:             rf.CurrentTerm,
						LeaderId:         rf.me,
						LastIncludeIndex: rf.LastLogIndex,
						LastIncludeTerm:  rf.LastLogTerm,
						Data:             rf.persister.ReadSnapshot(),
					}
					rf.mu.Unlock()
					flag := rf.sendInstallSnapshot(id, reqs, reply)
					rf.mu.Lock()
					if flag {
						if reply.Term > rf.CurrentTerm {
							rf.selectTime.Reset(time.Duration((rf.Rand.Int()%150)+300) * time.Millisecond)
							rf.CurrentTerm = reply.Term
							rf.VotedFor = -1
							rf.state = FOLLOWER
							rf.persist()
							rf.mu.Unlock()
							break
						} else if reply.Term > req.Term || nxt != rf.nextIndex[id] || rf.CurrentTerm != req.PrevLogTerm {
							rf.mu.Unlock()
							break
						} else {
							rf.nextIndex[id] = reqs.LastIncludeIndex + 1
							nxt = rf.nextIndex[id]
							req.PrevLogTerm = reqs.LastIncludeTerm
							req.PrevLogIndex = reqs.LastIncludeIndex
						}
					} else {
						rf.mu.Unlock()
						break
					}
				} else {
					req.PrevLogIndex = nxt - 1
					req.PrevLogTerm = rf.Logs[req.PrevLogIndex-rf.LastLogIndex].Term
				}
				// && nxt > rf.LastLogIndex
				if end >= nxt && nxt > rf.LastLogIndex {
					req.Entries = make([]LogEntry, end-nxt+1)
					copy(req.Entries, rf.Logs[nxt-rf.LastLogIndex:end-rf.LastLogIndex+1])
				}
				rf.mu.Unlock()
				resp := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(id, &req, resp)
				if ok {
					if resp.Success {
						cnt.Add(1)
						if cnt.Load() > int32(len(rf.peers)/2) {
							cnt.Store(-998244353)
							rf.mu.Lock()
							//rf.CurrentTerm == req.Term &&
							if rf.state == LEADER && rf.CurrentTerm == req.Term && rf.commitIndex < end && end >= rf.LastLogIndex && (rf.Logs[end-rf.LastLogIndex].Term == rf.CurrentTerm) {
								rf.commitIndex = end
								rf.applyCond.Signal()
								//fmt.Println("id: ", rf.me, " commit ", end)
							}
							rf.mu.Unlock()
						}
					}
					rf.mu.Lock()

					if resp.Term > rf.CurrentTerm {
						rf.selectTime.Reset(time.Duration((rf.Rand.Int()%150)+300) * time.Millisecond)
						rf.CurrentTerm = resp.Term
						rf.VotedFor = -1
						rf.state = FOLLOWER
						rf.persist()
						rf.mu.Unlock()
						break
					}
					// || req.Term != rf.CurrentTerm
					if resp.Term > req.Term || req.Term != rf.CurrentTerm {
						rf.mu.Unlock()
						break
					}
					if resp.Success == false {
						if rf.nextIndex[id] == nxt {
							var temp int = 0
							for _, v := range resp.Entries {
								//v.Index <= rf.LastLogIndex ||
								if v.Index <= rf.LastLogIndex || rf.Logs[v.Index-rf.LastLogIndex].Term == v.Term {
									temp = max(temp, v.Index+1)
								}
							}
							if temp <= rf.LastLogIndex {
								//fmt.Println("send snapshot")
								//fmt.Println("testing...")
								reply := &SnapshotResp{}
								reqs := &SnapshotReq{
									Term:             rf.CurrentTerm,
									LeaderId:         rf.me,
									LastIncludeIndex: rf.LastLogIndex,
									LastIncludeTerm:  rf.LastLogTerm,
									Data:             rf.persister.ReadSnapshot(),
								}
								rf.mu.Unlock()
								flag := rf.sendInstallSnapshot(id, reqs, reply)
								rf.mu.Lock()
								if flag && rf.nextIndex[id] == nxt {
									if reply.Term > rf.CurrentTerm {
										rf.selectTime.Reset(time.Duration((rf.Rand.Int()%150)+300) * time.Millisecond)
										rf.CurrentTerm = resp.Term
										rf.VotedFor = -1
										rf.state = FOLLOWER
										rf.persist()
										rf.mu.Unlock()
										break
									} else {
										rf.nextIndex[id] = reqs.LastIncludeIndex + 1
										rf.mu.Unlock()
										continue
									}
								} else {
									rf.mu.Unlock()
									break
								}
							}
							if temp == 0 {
								fmt.Println("impossible ")
								rf.nextIndex[id] = req.PrevLogIndex
							} else {
								rf.nextIndex[id] = temp
							}
							rf.mu.Unlock()
							continue
						}
					} else {
						if rf.matchIndex[id] < tag {
							rf.matchIndex[id] = tag
						}
						rf.nextIndex[id] = max(rf.nextIndex[id], rf.matchIndex[id]+1)
					}
					rf.mu.Unlock()
				}
				break
			}
		}(i, *req)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		select {
		case <-rf.selectTime.C:
			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.CurrentTerm++
			rf.VotedFor = rf.me
			go rf.selectLeader()
			rf.selectTime.Reset(time.Duration((rf.Rand.Int()%150)+300) * time.Millisecond)
			rf.mu.Unlock()
		case <-rf.heartTime.C:
			rf.mu.Lock()
			if rf.state == LEADER {
				go rf.sendHeart()
				rf.heartTime.Reset(50 * time.Millisecond)
			}
			rf.mu.Unlock()
		}

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		for true {
			rf.mu.Lock()
			for rf.lastApplied >= rf.commitIndex {
				rf.applyCond.Wait()
			}
			firstIndex, commitIndex, lastApplied := rf.LastLogIndex, rf.commitIndex, rf.lastApplied
			entries := make([]LogEntry, commitIndex-lastApplied)
			copy(entries, rf.Logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
			rf.mu.Unlock()
			for _, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandTerm:  entry.Term,
					CommandIndex: entry.Index,
				}
			}
			rf.mu.Lock()
			// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
			// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
			rf.lastApplied = max(rf.lastApplied, commitIndex)
			rf.mu.Unlock()
			//if rf.lastApplied >= rf.LastLogIndex && rf.lastApplied < rf.commitIndex {
			//	rf.lastApplied++
			//	temp := rf.Logs[rf.lastApplied-rf.LastLogIndex]
			//	if temp.Index != rf.lastApplied {
			//		fmt.Println("raft error at 771")
			//	}
			//	rf.mu.Unlock()
			//	rf.applyCh <- ApplyMsg{
			//		CommandValid: true,
			//		Command:      temp.Command,
			//		CommandIndex: temp.Index,
			//		CommandTerm:  temp.Term,
			//	}
			//} else {
			//	rf.mu.Unlock()
			//	break
			//}
		}
	}
}

func (rf *Raft) check() {
	for true {
		if rf.commitIndex < rf.LastLogIndex {
			fmt.Println("check id: ", rf.me, " state: ", rf.state, " error: rf.commitIndex < rf.LastLogIndex")
		}
		if rf.commitIndex > rf.CurrentLogIndex {
			fmt.Println("check id: ", rf.me, " state: ", rf.state, " error: rf.commitIndex > rf.CurrentLogIndex")
		}
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
	rf := &Raft{
		peers:           peers,
		persister:       persister,
		me:              me,
		dead:            0,
		CurrentTerm:     0,
		VotedFor:        -1,
		state:           FOLLOWER,
		commitIndex:     0,
		Logs:            make([]LogEntry, 1),
		lastApplied:     0,
		CurrentLogIndex: 0,
		nextIndex:       make([]int, len(peers)+1),
		matchIndex:      make([]int, len(peers)+1),
		//changeChan:      make(chan int, 1024),
		applyCh:      applyCh,
		LastLogIndex: 0,
		LastLogTerm:  0,
		Rand:         rand.New(rand.NewSource(int64(me * 10086))),
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.selectTime = time.NewTimer(time.Duration((rf.Rand.Int()%150)+300) * time.Millisecond)
	rf.heartTime = time.NewTimer(50 * time.Millisecond)
	rf.applyCond = sync.NewCond(&rf.mu)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLog()
	//go rf.check()
	go func() {
		for !rf.killed() {
			time.Sleep(5 * time.Second)
			rf.applyCond.Signal()
		}
	}()
	return rf
}
