package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	GET = iota
	PUT
	APPEND
)

type Err string

// Put or Append
type Args struct {
	Key   string
	Value string
	Op    int // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
}

type Reply struct {
	Value string
	Err   Err
}
