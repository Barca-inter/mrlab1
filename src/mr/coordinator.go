package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

type Coordinator struct {
	// Your definitions here.
	files            []string
	nReduce          int
	MapFile2Task     map[string]int
	workers          []string
	unFinishedMapCnt int
}

// Your code here -- RPC handlers for the worker to call.
//向worker发放id(按先后顺序发放，第一个worker id为0), 并存入coord信息中
func (c *Coordinator) AskForId(args *AskForIdInput, reply *AskForIdReply) error {
	len := len(c.workers)
	reply.WorkerIndex = len
	c.workers = append(c.workers, strconv.Itoa(len))
	fmt.Printf("give worker index %v\n", len)
	return nil
}

//向worker发放任务
func (c *Coordinator) AskForTask(args *AskForTaskInput, reply *AskForTaskReply) error {
	//如果没分配完map任务则继续分配
	if c.unFinishedMapCnt > 0 {

		for i, file := range c.files {
			if _, ok := c.MapFile2Task[file]; !ok {
				fmt.Printf("map file %v to worker %v\n", file, args.WorkerIndex)
				//将file与worker id关联
				c.MapFile2Task[file] = args.WorkerIndex
				//向worker发放map任务，返回文件id以及文件名
				reply.FileIndex = i
				reply.FileName = file
				reply.TaskType = Map
			}

		}
	} else {
		//分配完map任务通知worker
		fmt.Printf("all map task finish\n")
		reply.TaskType = MapFinish
		os.Exit(0)
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:            files,
		nReduce:          nReduce,
		MapFile2Task:     map[string]int{},
		unFinishedMapCnt: len(files),
	}

	// Your code here.

	c.server()
	return &c
}
