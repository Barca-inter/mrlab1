package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//向coord申请worker id
	workerIndex := AskForId()
	//向coord申请发放任务
	for {
		Reply := AskForTask(workerIndex)
		if err := RunTask(Reply, mapf, reducef); err != nil {
			log.Fatal("RunTask err:", err)
		}
		fmt.Printf("Finish task: %v %v\n", Reply.TaskType, Reply.FileName)
		InformFinish(Reply.TaskType, Reply.FileIndex)
		time.Sleep(1 * time.Second)
	}
}

func RunTask(Reply AskForTaskReply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) error {

	switch Reply.TaskType {
	case Map:
		//map
		// intermediate := []KeyValue{}
		file, err := os.Open(Reply.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", Reply.FileName)
			return err
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", Reply.FileName)
			return err
		}
		file.Close()
		kva := mapf(Reply.FileName, string(content))
		// intermediate = append(intermediate, kva...)

		sort.Sort(ByKey(kva))

		//以json格式输出中间文件mr-0, mr-1, mr-2...
		intermediateName := fmt.Sprintf("mr-%d.txt", Reply.FileIndex)
		ofile, _ := os.Create(intermediateName)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			_ = enc.Encode(&kv)
		}
		ofile.Close()
	case Reduce:
	case MapFinish:
		fmt.Printf("MapFinish \n")
		os.Exit(0)
	}
	return nil
}

//向coord申请worker id
func AskForId() int {

	// declare an argument structure.
	args := AskForIdInput{}

	// declare a reply structure.
	reply := AskForIdReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AskForId", &args, &reply)
	if ok {
		fmt.Printf("Get worker index: %v \n", reply.WorkerIndex)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply.WorkerIndex
}

//向coord申请发放任务
func AskForTask(workerIndex int) AskForTaskReply {

	// declare an argument structure.
	args := AskForTaskInput{}

	// fill in the argument(s).
	args.WorkerIndex = workerIndex

	// declare a reply structure.
	reply := AskForTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AskForTask", &args, &reply)
	if ok {
		fmt.Printf("Get task: %v %v \n", reply.TaskType, reply.FileName)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

//向coord通知任务完成
func InformFinish(TaskType string, FileIndex int) {

	// declare an argument structure.
	args := InformFinishInput{
		TaskType:  TaskType,
		FileIndex: FileIndex,
	}

	// declare a reply structure.
	reply := InformFinishReply{}

	ok := call("Coordinator.InformFinish", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
