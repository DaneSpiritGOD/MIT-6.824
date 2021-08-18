package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
)

type stage int

const (
	mapStage    stage = 0
	reduceStage stage = 1
)

type Coordinator struct {
	// Your definitions here.
	maxWorkerId uint32

	stage
	stageLock *sync.RWMutex

	mapTasks          chan *Task
	completedMapTasks chan *Task
	reduceTasks       chan *Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetWorkerId(_ struct{}, reply *WorkerIdentity) error {
	*reply = WorkerIdentity(atomic.AddUint32(&c.maxWorkerId, 1))
	return nil
}

func (C *Coordinator) AssignTask(workerId WorkerIdentity, reply *Task) error {
	select {
	case task := <-C.mapTasks:
		for {

		}
	case task := <-C.reduceTasks:
		for {

		}
	}

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

	if atomic.LoadUint32(&c.maxWorkerId) == 3 {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.completedMapTasks = make(chan *Task, len(files))

	c.stageLock = &sync.RWMutex{}
	go c.goToMapStage(files)

	c.server()
	return &c
}

func (c *Coordinator) goToMapStage(files []string) {
	c.stageLock.Lock()
	c.stage = mapStage
	c.stageLock.Unlock()

	c.mapTasks = make(chan *Task)

	id := 0
	for _, file := range files {
		id++
		c.mapTasks <- createTask(TaskIdentity(id), file)
	}
}

func (c *Coordinator) goToReduceStage() {
	c.stageLock.Lock()
	c.stage = reduceStage
	c.stageLock.Unlock()

	close(c.mapTasks)
	c.reduceTasks = make(chan *Task)
}
