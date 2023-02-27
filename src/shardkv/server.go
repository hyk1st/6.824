package shardkv

import (
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "sync"
import "6.824/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardKV struct {
	mu            sync.RWMutex
	me            int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	make_end      func(string) *labrpc.ClientEnd
	gid           int
	ctrlers       []*labrpc.ClientEnd
	maxraftstate  int // snapshot if log grows this big
	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config
	sc            *shardctrler.Clerk
	dead          int32
	// Your definitions here.
	Data      [shardctrler.NShards]ShardData
	indexCh   map[int]chan LastRespData
	lastApply int
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	DPrintf("{Node %v}{Group %v} has been killed", kv.me, kv.gid)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *ShardKV) isLeader() bool {
	_, ok := kv.rf.GetState()
	return ok
}
func (kv *ShardKV) compareIndex(shard int, client, reqId int64) bool {
	return kv.Data[shard].CompareIndex(client, reqId)
}

func (kv *ShardKV) getChan(index int) chan LastRespData {
	kv.indexCh[index] = make(chan LastRespData, 1)
	return kv.indexCh[index]
}

func (kv *ShardKV) isMyKey(key string) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.currentConfig.Shards[key2shard(key)] == kv.gid
}

func (kv *ShardKV) Command(args *CommandArgs, reply *RWReply) {
	DPrintf("group %v id %v try Command \n%+v\n", kv.gid, kv.me, args)
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		DPrintf("group %v id %v ErrWrongLeader \n%+v\n", kv.gid, kv.me, args)
		return
	}
	req := args.Detail.(RWArgs)

	if !kv.isMyKey(req.Key) {
		DPrintf("group %v id %v ErrWrongGroup 88\n%+v\n", kv.gid, kv.me, args)
		reply.Err = ErrWrongGroup
		return
	}
	shardId := key2shard(req.Key)

	kv.mu.RLock()
	if kv.Data[shardId].State != Server && kv.Data[shardId].State != GC {
		reply.Err = ErrWrongGroup

		DPrintf("group %v id %v ErrWrongGroup 98\n%+v\n", kv.gid, kv.me, args)
		kv.mu.RUnlock()
		return
	}

	DPrintf("group %v id %v sucess \n%+v\n", kv.gid, kv.me, args)
	kv.mu.RUnlock()
	//fmt.Println(args)
	index, _, leader := kv.rf.Start(*args)
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getChan(index)
	kv.mu.Unlock()
	defer func() {
		go func() {
			kv.mu.Lock()
			delete(kv.indexCh, index)
			kv.mu.Unlock()
		}()
	}()
	select {
	case msg := <-ch:
		if msg.ClientId == req.ClientId && msg.RequestId == req.RequestId {
			reply.Value = msg.LastValue
			reply.Err = OK
			return
		}
	case <-time.After(500 * time.Millisecond):
	}
	reply.Err = ErrWrongLeader
}

func (kv *ShardKV) FlushConfig(args *CommandArgs, reply *RWReply) {
	DPrintf("group %v id %v try FlushConfig \n%v\n", kv.gid, kv.me, args)
	if !kv.isLeader() {
		//fmt.Println(kv.isLeader(), "  ", kv.killed())
		reply.Err = ErrWrongLeader
		return
	}
	index, _, leader := kv.rf.Start(*args)
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	DPrintf("new msg has been start at index %v, %v\n", index, args)
	ch := kv.getChan(index)
	kv.mu.Unlock()
	defer func() {
		go func() {
			kv.mu.Lock()
			delete(kv.indexCh, index)
			kv.mu.Unlock()
		}()
	}()
	select {
	case msg := <-ch:
		reply.Value = msg.LastValue
		reply.Err = OK
		return
	case <-time.After(500 * time.Millisecond):
	}
	reply.Err = ErrWrongLeader
}

func (kv *ShardKV) PullShard(args *ShardArgs, reply *ShardReply) {
	DPrintf("group %v id %v try PullShard \n%v\n", kv.gid, kv.me, args)
	kv.mu.RLock()
	t := kv.currentConfig.Num
	kv.mu.RUnlock()
	if args.ConfigNum > t || (!kv.isLeader()) {
		reply.Err = ErrWrongGroup
		return
	}

	DPrintf("try pull shard from group %v id %v %v\n", kv.gid, kv.me, args.Shard2ID)
	kv.mu.RLock()
	reply.Err = OK
	reply.ShardId = args.Shard2ID
	reply.ShardData = kv.Data[args.Shard2ID].DeepCopy()
	reply.ConfigNum = kv.currentConfig.Num
	kv.mu.RUnlock()
}
func (kv *ShardKV) IsPulledShard(args *ShardArgs, reply *ShardReply) {
	DPrintf("group %v id %v try IsPulledShard \n%v\n", kv.gid, kv.me, args)

	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("receive push shard %v, state %v from group %v id %v\n", args.Shard2ID, kv.Data[args.Shard2ID].State, kv.gid, kv.me)
	kv.mu.RLock()
	if kv.currentConfig.Num > args.ConfigNum {
		reply.Err = OK
		kv.mu.RUnlock()
		return
	}
	DPrintf("aleady push shard %v from group %v id %v\n", args.Shard2ID, kv.gid, kv.me)
	kv.mu.RUnlock()
	tr := RWReply{}
	kv.FlushConfig(&CommandArgs{
		Op:     Pulled2Server,
		Detail: *args,
	}, &tr)
	reply.Err = tr.Err
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (kv *ShardKV) applyReadWrite(args RWArgs) LastRespData {
	DPrintf("group %v id %v try apply ReadWrite \n%v\n", kv.gid, kv.me, args)
	shardId := key2shard(args.Key)
	if kv.currentConfig.Shards[shardId] != kv.gid || (kv.Data[shardId].State != Server && kv.Data[shardId].State != GC) {
		return LastRespData{}
	}
	resp := LastRespData{
		LastValue: "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	if kv.compareIndex(shardId, args.ClientId, args.RequestId) {
		switch args.Op {
		case GET:
			resp.LastValue = kv.Data[shardId].Get(args.Key)
		case PUT:
			kv.Data[shardId].Put(args.Key, args.Value)
		case APPEND:
			kv.Data[shardId].Append(args.Key, args.Value)
		}
		kv.Data[shardId].LastResp[args.ClientId] = resp
	} else {
		resp = kv.Data[shardId].LastResp[args.ClientId]
	}
	return resp
}

func (kv *ShardKV) applyFlushConfig(args shardctrler.Config) LastRespData {
	DPrintf("group %v id %v try apply flush config \n%v\n", kv.gid, kv.me, args)
	if kv.currentConfig.Num+1 == args.Num {
		DPrintf("group %v id %v apply flush config \n%v\n", kv.gid, kv.me, args)
		kv.changeShardState(args)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = args
	}
	return LastRespData{}
}

func (kv *ShardKV) applyMsg() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			DPrintf("1  {Node %v}{Group %v}  receive msg last apply: %v, msg.index: %v\n\n", kv.me, kv.gid, kv.lastApply, msg.SnapshotIndex)
			//DPrintf("2  %v   %v", msg.CommandValid, kv.lastApply)
			//DPrintf("3   %+v\n", msg)
			if msg.CommandValid {
				op := msg.Command.(CommandArgs)
				kv.mu.Lock()
				if kv.lastApply >= msg.CommandIndex {
					kv.mu.Unlock()
					continue
				}
				kv.lastApply = max(kv.lastApply, msg.CommandIndex)
				resp := LastRespData{}
				switch op.Op {
				case ReadWrite:
					resp = kv.applyReadWrite(op.Detail.(RWArgs))
				case FlushConfig:
					kv.applyFlushConfig(op.Detail.(shardctrler.Config))
				case AddShards:
					kv.applyAddShards(op.Detail.(ShardReply))
				case DeleteShards:
					kv.applyDeleteShards(op.Detail.(ShardArgs))
				case Pulled2Server:
					kv.applyPulled2Server(op.Detail.(ShardArgs))
				case EmptyLog:
				}
				if ch, ok := kv.indexCh[msg.CommandIndex]; ok {
					if ch != nil {
						ch <- resp
					}
					delete(kv.indexCh, msg.CommandIndex)
				}
				flag := kv.maxraftstate != -1 && kv.maxraftstate <= kv.rf.GetRaftStateSize()
				if flag {
					DPrintf("{Node %v}{Group %v} snapshot\n", kv.me, kv.gid)
					snapshot, index := kv.PersistSnapShot()
					kv.rf.Snapshot(index, snapshot)
					DPrintf("{Node %v}{Group %v} snapshot end\n", kv.me, kv.gid)
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.mu.Lock()
					DPrintf("{Node %v}{Group %v} last apply %v, msg.inddex %v", kv.me, kv.gid, kv.lastApply, msg.SnapshotIndex)
					kv.DecodeSnapShot(msg.Snapshot)
					kv.lastApply = msg.SnapshotIndex
					kv.mu.Unlock()
				}
			}
		}
	}
	DPrintf("break apply\n")
}

func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	Data := [10]ShardData{}
	var lastConfig, currentConfig shardctrler.Config
	var lastApply int
	if d.Decode(&Data) == nil && d.Decode(&lastConfig) == nil && d.Decode(&currentConfig) == nil && d.Decode(&lastApply) == nil {
		kv.Data = Data
		kv.lastConfig = lastConfig
		kv.currentConfig = currentConfig
		kv.lastApply = lastApply
	} else {
		fmt.Printf("[Server(%v)] Failed to decode snapshot！！！", kv.me)

	}
}

// PersistSnapShot 持久化快照对应的map
func (kv *ShardKV) PersistSnapShot() ([]byte, int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Data)
	e.Encode(kv.lastConfig)
	e.Encode(kv.currentConfig)
	e.Encode(kv.lastApply)
	data := w.Bytes()
	ls := kv.lastApply
	return data, ls
}

func (kv *ShardKV) pullConfig() {
	for !kv.killed() {
		//DPrintf("group %v id %v not leader\n", kv.gid, kv.me)
		if kv.isLeader() {
			kv.mu.RLock()
			flag := false
			for _, v := range kv.Data {
				if v.State != Server {
					//DPrintf("group %v id %v not all server\n", kv.gid, kv.me)
					flag = true
				}
			}
			currentNum := kv.currentConfig.Num
			kv.mu.RUnlock()
			if !flag {
				DPrintf("group %v id %v try get  new config \n", kv.gid, kv.me)
				nextConfig := kv.sc.Query(currentNum + 1)
				if nextConfig.Num == currentNum+1 {
					DPrintf("group %v id %v reveive new config %v\n", kv.gid, kv.me, nextConfig)
					DPrintf("%v\n", kv.currentConfig)
					kv.FlushConfig(&CommandArgs{
						Op:     FlushConfig,
						Detail: nextConfig,
					}, &RWReply{})
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	DPrintf("break pullConfig()\n")
}

func (kv *ShardKV) changeShardState(args shardctrler.Config) {
	current := kv.currentConfig
	for i := 0; i < shardctrler.NShards; i++ {
		if current.Shards[i] == kv.gid && args.Shards[i] != kv.gid {
			if args.Shards[i] != 0 {
				kv.Data[i].State = Pulled
			}
		}
		if current.Shards[i] != kv.gid && args.Shards[i] == kv.gid {
			if current.Shards[i] != 0 {
				//fmt.Println(current, "\n", args, "\n", kv.Data[i], "\n", i)
				kv.Data[i].State = Pull
			}
		}
	}
}

func (kv *ShardKV) pullShardData() {
	for !kv.killed() {
		//DPrintf("group %v id %v pull sharddata\n", kv.gid, kv.me)
		if !kv.isLeader() {
			time.Sleep(100 * time.Millisecond)
			//DPrintf("group %v id %v not leader\n", kv.gid, kv.me)
			continue
		}
		var wg sync.WaitGroup
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.Data[i].State == Pull {
				wg.Add(1)
				go func(src int) {
					defer wg.Done()
					kv.mu.RLock()
					servers := kv.lastConfig.Groups[kv.lastConfig.Shards[src]]
					cfNum := kv.currentConfig.Num
					kv.mu.RUnlock()
					for _, dst := range servers {
						srv := kv.make_end(dst)
						args := &ShardArgs{
							ConfigNum: cfNum,
							Shard2ID:  src,
						}
						reply := &ShardReply{}
						DPrintf("group %v %v try pull shard from %v\n", kv.gid, src, dst)
						if srv.Call("ShardKV.PullShard", args, reply) && reply.Err == OK {
							//fmt.Println(dst, "pull from", src)
							//fmt.Println(reply)
							DPrintf("server-%v-%v success pull shard from %v \n", kv.gid, kv.me, dst)
							kv.FlushConfig(&CommandArgs{
								Op:     AddShards,
								Detail: *reply,
							}, &RWReply{})
						}
					}
				}(i)
			}
		}
		wg.Wait()
		time.Sleep(80 * time.Millisecond)
	}
	DPrintf("break pullShardData()")
}

func (kv *ShardKV) applyAddShards(reply ShardReply) {
	if reply.ConfigNum != kv.currentConfig.Num || kv.Data[reply.ShardId].State != Pull {
		return
	}
	DPrintf("group %v id %v apply add shard apply %v\n", kv.gid, kv.me, reply.ShardId)
	t := reply.ShardData.DeepCopy()
	kv.Data[reply.ShardId] = t
	kv.Data[reply.ShardId].State = GC
}

func (kv *ShardKV) gcShards() {
	for !kv.killed() {
		//DPrintf("group %v id %v gc shard\n", kv.gid, kv.me)
		if !kv.isLeader() {
			//DPrintf("group %v id %v not leader\n", kv.gid, kv.me)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		var wg sync.WaitGroup
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.Data[i].State == GC {
				wg.Add(1)
				go func(src int) {
					defer wg.Done()
					kv.mu.RLock()
					servers := kv.lastConfig.Groups[kv.lastConfig.Shards[src]]
					cfNum := kv.currentConfig.Num
					kv.mu.RUnlock()
					for _, dst := range servers {
						srv := kv.make_end(dst)
						args := &ShardArgs{
							ConfigNum: cfNum,
							Shard2ID:  src,
						}
						reply := &ShardReply{}
						DPrintf("group %v id %v try gc shard ask to %v\n", kv.gid, kv.me, dst)
						if srv.Call("ShardKV.IsPulledShard", args, reply) && reply.Err == OK {
							DPrintf("group %v id %v gc shard aleady receive %v\n", kv.gid, kv.me, reply.ShardId)
							kv.FlushConfig(&CommandArgs{
								Op:     DeleteShards,
								Detail: *args,
							}, &RWReply{})
						}
					}
				}(i)
			}
		}
		wg.Wait()
		time.Sleep(80 * time.Millisecond)
	}
	DPrintf("break gcShards()\n")
}

func (kv *ShardKV) applyDeleteShards(args ShardArgs) {
	if args.ConfigNum != kv.currentConfig.Num {
		return
	}
	DPrintf("group %v id %v apply gc shard %v\n", kv.gid, kv.me, args.Shard2ID)
	kv.Data[args.Shard2ID].State = Server
}

func (kv *ShardKV) applyPulled2Server(args ShardArgs) {
	if args.ConfigNum != kv.currentConfig.Num {
		return
	}
	DPrintf("group %v id %v apply pulled to server at shard %v\n", kv.gid, kv.me, args.Shard2ID)
	kv.Data[args.Shard2ID] = *NewShardData()
}

func (kv *ShardKV) sendEmpty() {
	for !kv.killed() {
		if kv.isLeader() {
			kv.FlushConfig(&CommandArgs{
				Op:     EmptyLog,
				Detail: nil,
			}, &RWReply{})
		}
		time.Sleep(4 * time.Second)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler_my.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	labgob.Register(shardctrler.CommandReq{})
	labgob.Register(shardctrler.CommandResp{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(CommandArgs{})
	labgob.Register(RWReply{})
	labgob.Register(RWArgs{})
	labgob.Register(ShardArgs{})
	labgob.Register(ShardReply{})
	labgob.Register(LastRespData{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.dead = 0
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	//kv.SuccessReq = make(map[int64]Op)
	kv.indexCh = make(map[int]chan LastRespData)
	kv.lastApply = 0
	kv.Data = [10]ShardData{}
	kv.sc = shardctrler.MakeClerk(ctrlers)
	kv.lastConfig = shardctrler.DefaultConfig()
	kv.currentConfig = shardctrler.DefaultConfig()
	// You may need initialization code here.
	snapshot := persister.ReadSnapshot()
	for i := 0; i < shardctrler.NShards; i++ {
		kv.Data[i] = *NewShardData()
	}
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}
	DPrintf("{Node %v}{Group %v} has been started", kv.me, kv.gid)
	go kv.applyMsg()
	go kv.pullConfig()
	go kv.pullShardData()
	go kv.gcShards()
	go kv.sendEmpty()
	return kv
}
