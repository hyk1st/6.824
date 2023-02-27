package kvraft

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clientId  int64
	requestId int64
	leaderId  int
	// You will have to modify this struct.
}

func nrand() int64 {
	maxx := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, maxx)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leaderId = 0
	// You'll have to add code here.
	return ck
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

func (ck *Clerk) Command(key, value string, op int) string {
	ck.requestId++
	args := Args{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	for {
		reply := Reply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Command", &args, &reply)
		if ok && reply.Err == OK {
			return reply.Value
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
}
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	return ck.Command(key, "", GET)
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) Put(key string, value string) {
	ck.Command(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(key, value, APPEND)
}
