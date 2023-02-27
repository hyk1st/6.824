package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId  int
	requestId int64
	clientId  int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leaderId = 0
	ck.requestId = 0
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) Command(req CommandReq) CommandResp {
	ck.requestId++
	req.CommandId = ck.requestId
	req.ClientId = ck.clientId
	for {
		resp := CommandResp{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Command", &req, &resp)
		if ok && resp.Err == OK {
			return resp
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}

}

func (ck *Clerk) Query(num int) Config {
	return ck.Command(CommandReq{Num: num, OpType: QUERY}).Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.Command(CommandReq{Servers: servers, OpType: JOIN})
}

func (ck *Clerk) Leave(gids []int) {
	ck.Command(CommandReq{GIDs: gids, OpType: LEAVE})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.Command(CommandReq{Shard: shard, GID: gid, OpType: MOVE})
}
