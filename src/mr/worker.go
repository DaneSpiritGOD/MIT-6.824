package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"reflect"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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
		task, err := currentInfo.applyForTask()
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
	err := call("Coordinator.GetWorkerId", struct{}{}, &id, UnknownWorker)
	if err != nil {
		return err
	}

	info.Id = id
	log.Printf("Worker [%d] is initiated", id)
	return nil
}

func (info *workerInfo) getReduceCount() error {
	var count int
	err := call("Coordinator.GetReduceCount", struct{}{}, &count, info.Id)
	if err != nil {
		return err
	}

	info.reduceCount = count
	log.Printf("Worker [%d] got reduce count:%d", info.Id, count)
	return nil
}

func (info *workerInfo) applyForTask() (Task, error) {
	var task Task
	err := call("Coordinator.AssignTask", &info.Id, &task, info.Id)
	if err != nil {
		return NilTask, err
	}

	log.Printf("Worker [%d] got a new task [%v:%d, input: %s]", info.Id, task.Type, task.Id, task.Data)
	return task, nil
}

func (info *workerInfo) execute(task *Task) error {
	switch task.Type {
	case MapTaskType:
		kvs, err := decodeInputThenMap(task.Data[0])
		if err != nil {
			return err
		}

		groups := reorganizeMapOutputs(info.getHashId, kvs)

		outputs, err := encodeMapOutputs(task.Id, groups, createFileCacheForMap)
		if err != nil {
			return err
		}

		task.Data = outputs
	case ReduceTaskType:
		kvs, err := decodeInputThenReduce(task.Data, createFileInputReaderForReduce)
		if err != nil {
			return err
		}

		cache, err := createFileOutputWriterForReduce(task.Id)
		if err != nil {
			return err
		}

		output, err := encodeReduceOutputs(kvs, cache)
		if err != nil {
			return err
		}

		task.Data = []string{output}
	}

	return nil
}

func (info *workerInfo) commitTask(task *Task) error {
	err := call("Coordinator.ReceiveTaskOutput", task, nil, info.Id)
	if err != nil {
		return err
	}

	log.Printf("Worker [%d] committed task [%v:%v] successfully", info.Id, task.Type, task.Id)
	return nil
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
	call("Coordinator.Example", &args, &reply, UnknownWorker)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}, workerId WorkerIdentity) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatalf("Worker [%v] error [%v] occurred when dialing [%s]. It is probably caused by Master being terminated", workerId, err, rpcname)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		return fmt.Errorf("Worker [%v] error [%v] occurred when calling [%s]. It is probably caused by Master being terminated", workerId, err, rpcname)
	}

	return nil
}
