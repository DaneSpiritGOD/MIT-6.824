package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"reflect"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValues struct {
	Key    string
	Values []string
}

type workerInfo struct {
	Id          WorkerIdentity
	reduceCount int
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

var mapFunc func(string, string) []KeyValue
var reduceFunc func(string, []string) string

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	mapFunc = mapf
	reduceFunc = reducef

	currentInfo := &workerInfo{}
	err := currentInfo.assignId()
	if err != nil {
		log.Fatalln(err)
	}

	err = currentInfo.getReduceCount()
	if err != nil {
		log.Fatalln(err)
	}

	for {
		task, err := currentInfo.askForTask()
		if err != nil {
			log.Print(err)
			continue
		}

		if reflect.DeepEqual(task, DoneTask) {
			break
		}

		pTask := &task
		if currentInfo.execute(pTask) != nil {
			log.Print(err)
			continue
		}

		if currentInfo.commitTask(pTask) != nil {
			log.Print(err)
			continue
		}
	}
}

func (info *workerInfo) assignId() error {
	var id WorkerIdentity
	if !call("Coordinator.GetWorkerId", struct{}{}, &id) {
		return fmt.Errorf("an error occurred when retrieving worker id")
	}

	info.Id = id
	log.Printf("Worker [%d] is initiated", id)
	return nil
}

func (info *workerInfo) getReduceCount() error {
	var count int
	if !call("Coordinator.GetRedueCount", struct{}{}, &count) {
		return fmt.Errorf("an error occurred when retrieving reduce count")
	}

	info.reduceCount = count
	log.Printf("Worker [%d] got reduce count:%d", info.Id, count)
	return nil
}

func (info *workerInfo) askForTask() (Task, error) {
	var task Task
	if !call("Coordinator.AssignTask", &info.Id, &task) {
		return NilTask, fmt.Errorf("an error occurred when retrieving worker id")
	}

	log.Printf("Worker [%d] got a new task [%d, input: %s]", info.Id, task.Id, task.Input)
	return task, nil
}

func (info *workerInfo) execute(task *Task) error {
	switch task.Type {
	case MapTaskType:
		kvs, err := parseMapFile(task.Input[0])
		if err != nil {
			return err
		}

		groups := getOrderedMapResultGroups(info.getHashId, kvs)

		outputs, err := encodeIntoReduceFiles(task.Id, groups)
		if err != nil {
			return err
		}

		task.Output = outputs
	case ReduceTaskType:
	}

	return nil
}

func (info *workerInfo) commitTask(task *Task) error {
	if !call("Coordinator.ReceiveTaskOutput", task, struct{}{}) {
		return fmt.Errorf("an error occurred when committing %v", task)
	}

	log.Printf("successfully committing %v", *task)
	return nil
}

func parseMapFile(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot open %v", filename)
	}

	defer func() {
		file.Close()
	}()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("cannot read %v", filename)
	}

	return mapFunc(filename, string(content)), nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
