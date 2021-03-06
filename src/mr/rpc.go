package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	Map       string = "map"
	Reduce    string = "reduce"
	MapFinish string = "mapFinish"
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type AskForTaskInput struct {
	WorkerIndex int
}

type AskForTaskReply struct {
	TaskType  string
	FileName  string
	FileIndex int
}

type AskForIdInput struct {
}

type AskForIdReply struct {
	WorkerIndex int
}

type InformFinishInput struct {
	TaskType  string
	FileIndex int
}

type InformFinishReply struct {
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
