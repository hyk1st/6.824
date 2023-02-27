package shardctrler

import (
	"6.824/raft"
	"math"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	SuccessReq map[int64]Op
	indexCh    map[int]chan Op
	configs    []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Servers   map[int][]string // for Join
	GIDs      []int            // for Leave
	Shard     int              // for Move
	GID       int              // for Move
	Num       int              // for Query
	OpType    int
	ClientId  int64
	CommandId int64
	Config    Config
}

func (sc *ShardCtrler) isLeader() bool {
	_, ok := sc.rf.GetState()
	return ok
}
func (sc *ShardCtrler) compareIndex(client, reqId int64) bool {
	if v, ok := sc.SuccessReq[client]; !ok || v.CommandId < reqId {
		return true
	}
	return false
}

func (sc *ShardCtrler) getChan(index int) chan Op {
	sc.indexCh[index] = make(chan Op, 1)
	return sc.indexCh[index]
}
func (sc *ShardCtrler) Command(req *CommandReq, resp *CommandResp) {
	if !sc.isLeader() {
		resp.Err = ErrWrongLeader
		return
	}
	t := Op{
		Servers:   req.Servers,
		GIDs:      req.GIDs,
		Shard:     req.Shard,
		GID:       req.GID,
		Num:       req.Num,
		OpType:    req.OpType,
		ClientId:  req.ClientId,
		CommandId: req.CommandId,
	}
	sc.mu.Lock()
	index, _, leader := sc.rf.Start(t)
	if !leader {
		resp.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	ch := sc.getChan(index)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.indexCh, index)
		sc.mu.Unlock()
	}()
	select {
	case msg := <-ch:
		if msg.ClientId == req.ClientId && msg.CommandId == req.CommandId && sc.isLeader() {
			resp.Config = msg.Config
			resp.Err = OK
			return
		}
	case <-time.After(500 * time.Millisecond):
	}
	resp.Err = ErrWrongLeader
}

func (sc *ShardCtrler) applyMsg() {
	for {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				commamd := msg.Command.(Op)
				sc.mu.Lock()
				if sc.compareIndex(commamd.ClientId, commamd.CommandId) {
					switch commamd.OpType {
					case JOIN:
						sc.Join(commamd)
					case LEAVE:
						sc.Leave(commamd)
					case MOVE:
						sc.Move(commamd)
					case QUERY:
						commamd.Config = sc.Query(commamd)
					}
					sc.SuccessReq[commamd.ClientId] = commamd
				} else {
					commamd = sc.SuccessReq[commamd.ClientId]
				}
				if ch, ok := sc.indexCh[msg.CommandIndex]; ok {
					term, leader := sc.rf.GetState()
					if ch != nil && leader && term == msg.CommandTerm {
						ch <- commamd
						//fmt.Println(commamd.)
					} else if ch != nil {
						ch <- Op{}
					}
					delete(sc.indexCh, msg.CommandIndex)
				}
				sc.mu.Unlock()

			}
		}
	}
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func getMin(t map[int][]int) int {
	index, num := -1, math.MaxInt
	for k, v := range t {
		if k == 0 {
			continue
		}
		siz := len(v)
		if siz < num || (siz == num && k < index) {
			index, num = k, siz
		}
	}
	return index
}

func getMax(t map[int][]int) int {
	if len(t[0]) > 0 {
		return 0
	}
	index, num := -1, -1
	for k, v := range t {
		siz := len(v)
		if siz > num || (siz == num && k < index) {
			index, num = k, siz
		}
	}
	return index
}

func getGid2Shard(config *Config) map[int][]int {
	t := make(map[int][]int)
	t[0] = make([]int, 0)
	for gid := range config.Groups {
		t[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		t[gid] = append(t[gid], shard)
	}
	return t
}

func GidToShard(t map[int][]int) [NShards]int {
	r := [NShards]int{}
	for k, v := range t {
		for _, shard := range v {
			r[shard] = k
		}
	}
	return r
}

func balance(config *Config) {
	t := getGid2Shard(config)
	for {
		minId, maxId := getMin(t), getMax(t)
		//fmt.Println(minId, "  ", len(t[minId]), "               ", maxId, "  ", len(t[maxId]))
		if maxId != 0 && len(t[maxId])-len(t[minId]) <= 1 {
			break
		}
		t[minId] = append(t[minId], t[maxId][0])
		t[maxId] = t[maxId][1:]
		//time.Sleep(time.Second)
	}
	config.Shards = GidToShard(t)
}

func (sc *ShardCtrler) Join(req Op) {
	// Your code here.
	//fmt.Println("begin  ", req.CommandId)
	//fmt.Println(sc.configs)
	//fmt.Println(req.Servers)
	group := req.Servers
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: lastConfig.Shards,
		Groups: deepCopy(lastConfig.Groups),
	}
	for gid, servers := range group {
		if _, ok := newConfig.Groups[gid]; !ok {
			temp := make([]string, len(servers))
			copy(temp, servers)
			newConfig.Groups[gid] = temp
		}
	}
	balance(&newConfig)
	sc.configs = append(sc.configs, newConfig)
	//fmt.Println("after  ", req.CommandId)
	//fmt.Println(sc.configs)
}

func (sc *ShardCtrler) Leave(req Op) {
	// Your code here.

	gids := req.GIDs
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: lastConfig.Shards,
		Groups: deepCopy(lastConfig.Groups),
	}
	t := getGid2Shard(&newConfig)
	temp := make([]int, 0)
	for _, v := range gids {
		if _, ok := newConfig.Groups[v]; ok {
			delete(newConfig.Groups, v)
		}
		if shards, ok := t[v]; ok {
			temp = append(temp, shards...)
			delete(t, v)
		}
	}
	if len(newConfig.Groups) > 0 {
		t[0] = append(t[0], temp...)
		r := GidToShard(t)
		newConfig.Shards = r
		balance(&newConfig)
	} else {
		t[0] = append(t[0], temp...)
		r := GidToShard(t)
		newConfig.Shards = r
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) Move(req Op) {
	// Your code here.
	shard := req.Shard
	gid := req.GID
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	newConfig.Shards[shard] = gid
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) Query(req Op) Config {
	// Your code here.
	num := req.Num
	if num < 0 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[num]
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.SuccessReq = make(map[int64]Op)
	sc.indexCh = make(map[int]chan Op)
	go sc.applyMsg()

	return sc
}
