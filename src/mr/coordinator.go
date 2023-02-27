package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var (
	mutex        sync.Mutex       //获取任务时的锁
	taskMutex    sync.Mutex       //修改下面taskOK和reduceTaskOK时的锁
	taskOK       map[int]struct{} // 用以存放map阶段任务是否完成
	reduceTaskOK map[int]struct{} // 用以存放reduce阶段任务是否完成
	inChan       map[int]int      //存放任务被哪个Worker拿走，没被拿走即为-1
	mapFinish    int              //map任务完成数
	reduceFinish int              //reduce任务完成数
)

const (
	MapType int = iota
	ReduceType
)

const (
	MapStatus int = iota
	ReduceStatus
	Waiting
	MapWaiting
	ReduceWaiting
	Success
)

type Coordinator struct {
	// Your definitions here.
	Status     int        // 当前系统状态
	MapChan    chan *Task // map任务队列
	ReduceChan chan *Task //reduce任务队列
	TaskNum    int        //map任务数
	ReducerNum int        //reduce任务数
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) produceTask(files []string) {
	for i, file := range files {
		task := Task{
			Type:       MapStatus,
			ID:         i,
			ReducerNum: c.ReducerNum,
			FileName:   file,
		}
		inChan[i] = -1
		c.MapChan <- &task
	}
}
func (c *Coordinator) makeReduceWork() {
	for i := 0; i < c.ReducerNum; i++ {
		task := &Task{
			Type:       ReduceStatus,
			ID:         i,
			ReducerNum: c.ReducerNum,
			FileName:   "",
		}
		inChan[i] = -1
		c.ReduceChan <- task
	}
}
func (c *Coordinator) PullTask(req *TaskReq, resp *Task) error {
	mutex.Lock()
	defer mutex.Unlock()

	switch c.Status {
	case MapStatus:
		{
			if len(c.MapChan) > 0 {
				*resp = *<-c.MapChan
				inChan[resp.ID] = req.WorkerId
				go func(temp Task) {
					select {
					case <-time.After(10 * time.Second):
						{
							taskMutex.Lock()
							if _, ok := taskOK[temp.ID]; !ok {
								c.MapChan <- &temp
								c.Status = MapStatus
								inChan[resp.ID] = -1
								log.Printf("map time exced %v\n", temp.ID)
							}
							taskMutex.Unlock()
						}
					}
				}(*resp)
			} else {
				c.Status = MapWaiting
				resp.Type = Waiting
			}
		}
	case ReduceStatus:
		{
			if len(c.ReduceChan) > 0 {
				*resp = *(<-c.ReduceChan)
				resp.Type = ReduceStatus
				temp := *resp
				inChan[resp.ID] = req.WorkerId
				//resp.Type =
				go func(temp Task) {
					select {
					case <-time.After(10 * time.Second):
						{
							taskMutex.Lock()

							if _, ok := reduceTaskOK[temp.ID]; !ok {
								c.ReduceChan <- &temp
								c.Status = ReduceStatus
								inChan[temp.ID] = -1
								log.Printf("reduce time exced %v\n", temp.ID)
							}
							taskMutex.Unlock()
						}
					}
				}(temp)
			} else {
				c.Status = ReduceWaiting
				resp.Type = Waiting
			}
		}
	case MapWaiting:
		{
			if mapFinish == c.TaskNum {
				c.makeReduceWork()
				c.Status = ReduceStatus
				resp.Type = Waiting

			} else {
				resp.Type = Waiting
			}
		}
	case ReduceWaiting:
		{
			if reduceFinish == c.ReducerNum {
				c.Status = Success
				resp.Type = Success
			} else {
				resp.Type = Waiting
			}

		}
	default:
		{
			resp.Type = Success
		}
	}
	return nil
}

func (c *Coordinator) SuccessCheck(req *CheckReq, resp *CheckResp) error {
	taskMutex.Lock()
	if inChan[req.Task] != req.WorkerId {
		taskMutex.Unlock()
		return nil
	}
	if req.Type == MapStatus {
		taskOK[req.Task] = struct{}{}
		mapFinish++
		log.Printf("map %v success by %v\n", req.Task, req.WorkerId)
	} else if req.Type == ReduceStatus {
		reduceTaskOK[req.Task] = struct{}{}
		reduceFinish++
		log.Printf("reduce %v success by %v\n", req.Task, req.WorkerId)
	}
	resp.Success = true
	taskMutex.Unlock()
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.Status == Success {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Status:     MapStatus,
		TaskNum:    len(files),
		ReducerNum: nReduce,
		MapChan:    make(chan *Task, len(files)), // 必须有缓冲，否则会有阻塞
		ReduceChan: make(chan *Task, nReduce),
	}
	// Your code here.
	taskOK = make(map[int]struct{})
	reduceTaskOK = make(map[int]struct{})
	inChan = make(map[int]int)
	c.produceTask(files) //制造任务
	c.server()
	return &c
}
