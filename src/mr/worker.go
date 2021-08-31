package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"reflect"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type workerInfo struct {
	Id WorkerIdentity
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
	err := currentInfo.initId()
	if err != nil {
		log.Println(err)
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

func (info *workerInfo) initId() error {
	var id WorkerIdentity
	if !call("Coordinator.GetWorkerId", struct{}{}, &id) {
		return fmt.Errorf("an error occurred when retrieving worker id")
	}

	info.Id = id
	log.Printf("worker: got id:%d", id)
	return nil
}

func (info *workerInfo) askForTask() (Task, error) {
	var task Task
	if !call("Coordinator.AssignTask", &info.Id, &task) {
		return NilTask, fmt.Errorf("an error occurred when retrieving worker id")
	}

	log.Printf("worker:%d got a new task:%d input:%s", info.Id, task.Id, task.Input)
	return task, nil
}

func (info *workerInfo) execute(task *Task) error {
	switch task.Type {
	case MapTaskType:
		filename := task.Input[0]
		file, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("cannot open %v when executing %v", filename, task)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return fmt.Errorf("cannot read %v", filename)
		}
		file.Close()

		kvs := mapFunc(filename, string(content))

		groupBy := func() []KeyValue {
			m := make(map[string]int)
			var keys []string
			for _, kv := range kvs {
				c, ok := m[kv.Key]
				if !ok {
					keys = append(keys, kv.Key)
				}
				m[kv.Key] = c + 1
			}

			sort.Strings(keys)

			kvs2 := make([]KeyValue, len(m))
			for _, key := range keys {
				kvs2 = append(kvs2, KeyValue{key, strconv.Itoa(m[key])})
			}
			return kvs2
		}

		kvs2 := groupBy()

	case ReduceTaskType:
		//filenames := task.Input
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
