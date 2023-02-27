package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	GET = iota
	PUT
	APPEND
)

const (
	Server = iota
	Pull
	Pulled
	GC
)

const (
	ReadWrite = iota
	FlushConfig
	AddShards
	DeleteShards
	Pulled2Server
	EmptyLog
)

type Err string

type CommandArgs struct {
	Op     int
	Detail interface{}
}

// Put or Append
type RWArgs struct {
	Key   string
	Value string
	Op    int // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
}

type RWReply struct {
	Value string
	Err   Err
}

type LastRespData struct {
	LastValue string
	ClientId  int64
	RequestId int64
}

type ShardArgs struct {
	ConfigNum int
	Shard2ID  int
}

type ShardReply struct {
	Err       Err
	ShardId   int
	ConfigNum int
	ShardData ShardData
}
