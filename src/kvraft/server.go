package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"log"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key       string
	Value     string
	Op        int
	ClientId  int64
	RequestId int64
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	Data         map[string]string
	SuccessReq   map[int64]Op
	indexCh      map[int]chan Op
	lastApply    int
	// Your definitions here.
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *KVServer) isLeader() bool {
	_, ok := kv.rf.GetState()
	return ok
}
func (kv *KVServer) compareIndex(client, reqId int64) bool {
	if v, ok := kv.SuccessReq[client]; !ok || v.RequestId < reqId {
		return true
	}
	return false
}

func (kv *KVServer) getChan(index int) chan Op {
	kv.indexCh[index] = make(chan Op, 1)
	return kv.indexCh[index]
}
func (kv *KVServer) Command(args *Args, reply *Reply) {
	if (!kv.isLeader()) || kv.killed() {
		//fmt.Println(kv.isLeader(), "  ", kv.killed())
		reply.Err = ErrWrongLeader
		return
	}
	req := Op{
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	kv.mu.Lock()
	index, _, leader := kv.rf.Start(req)
	if !leader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := kv.getChan(index)
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.indexCh, index)
		kv.mu.Unlock()
	}()
	select {
	case msg := <-ch:
		if msg.ClientId == args.ClientId && msg.RequestId == args.RequestId && kv.isLeader() {
			reply.Value = msg.Value
			reply.Err = OK
			return
		}
	case <-time.After(500 * time.Millisecond):
	}
	reply.Err = ErrWrongLeader
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (kv *KVServer) applyMsg() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				if kv.lastApply >= msg.CommandIndex {
					continue
				}
				kv.lastApply = max(kv.lastApply, msg.CommandIndex)
				kv.mu.Lock()
				if kv.compareIndex(op.ClientId, op.RequestId) {
					switch op.Op {
					case GET:
						op.Value = kv.Data[op.Key]
					case PUT:
						kv.Data[op.Key] = op.Value
					case APPEND:
						kv.Data[op.Key] += op.Value
					}
					kv.SuccessReq[op.ClientId] = op
				} else {
					op = kv.SuccessReq[op.ClientId]
				}
				if ch, ok := kv.indexCh[msg.CommandIndex]; ok {
					term, leader := kv.rf.GetState()
					if ch != nil && leader && term == msg.CommandTerm {
						ch <- op
					} else if ch != nil {
						ch <- Op{
							Key:       "",
							Value:     "",
							Op:        0,
							ClientId:  0,
							RequestId: 0,
						}
					}
					delete(kv.indexCh, msg.CommandIndex)
				}
				if kv.maxraftstate != -1 && kv.maxraftstate <= kv.rf.GetLogSize() {
					snapshot, index := kv.PersistSnapShot()
					kv.rf.Snapshot(index, snapshot)
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid && kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.mu.Lock()
				kv.DecodeSnapShot(msg.Snapshot)
				kv.lastApply = msg.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var Data map[string]string
	var SuccessReq map[int64]Op

	if d.Decode(&Data) == nil && d.Decode(&SuccessReq) == nil {
		kv.Data = Data
		kv.SuccessReq = SuccessReq
	} else {
		fmt.Printf("[Server(%v)] Failed to decode snapshot！！！", kv.me)

	}
}

// PersistSnapShot 持久化快照对应的map
func (kv *KVServer) PersistSnapShot() ([]byte, int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Data)
	e.Encode(kv.SuccessReq)
	data := w.Bytes()
	return data, kv.lastApply
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.SuccessReq = make(map[int64]Op)
	kv.indexCh = make(map[int]chan Op)
	kv.lastApply = 0
	kv.Data = make(map[string]string)
	// You may need initialization code here.
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	go kv.applyMsg()
	return kv
}
