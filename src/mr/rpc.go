package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskReq struct {
	WorkerId int
}

type Task struct {
	Type       int
	ID         int
	ReducerNum int
	FileName   string
}

type CheckReq struct {
	Task     int //任务ID
	Type     int //任务类型
	WorkerId int //worker id
}

type CheckResp struct {
	Success bool //无关紧要，可以为空结构体
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
