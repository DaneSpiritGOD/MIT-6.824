package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	maxWorkerId uint32

	m int // count of map tasks
	r int // count of reduce task

	mapTasks             chan *Task
	completedMapTasks    chan *Task
	reduceTasks          chan *Task
	completedReduceTasks chan *Task

	done chan struct{}
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

func (c *Coordinator) AssignTask(workerId WorkerIdentity, reply *Task) error {
	select {
	case mapTask := <-c.mapTasks:
		mapTask.WorkerId = workerId
		*reply = *mapTask

		go func() {
			c.mapTasks <- mapTask
		}()

	case reduceTask := <-c.reduceTasks:
		reduceTask.WorkerId = workerId
		*reply = *reduceTask

		go func() {
			c.reduceTasks <- reduceTask
		}()

	case <-c.done:
		*reply = DoneTask
	}

	return nil
}

func (c *Coordinator) ReceiveTaskOutput(args *Task, _ *struct{}) error {
	switch args.Type {
	case Map:
		c.completedMapTasks <- args
	case Reduce:
		c.completedReduceTasks <- args
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
	// Your code here.
	select {
	case <-c.done:
		return true
	case <-time.After(time.Second * 1):
		return false
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.m = len(files)
	c.r = nReduce

	c.mapTasks = make(chan *Task)
	c.completedMapTasks = make(chan *Task)
	c.reduceTasks = make(chan *Task)
	c.completedReduceTasks = make(chan *Task)

	go c.createMapTasks(files)
	go c.createReduceTasks()
	go c.checkDone()

	c.server()
	return &c
}

func (c *Coordinator) createMapTasks(files []string) {
	id := 0
	for _, file := range files {
		id++
		c.mapTasks <- createTask(Map, TaskIdentity(id), []string{file})
	}
}

func (c *Coordinator) createReduceTasks() {
	reduceFiles := make(map[TaskIdentity][]string)

	for mapTaskCount := 0; mapTaskCount < c.m; mapTaskCount++ {
		mapTask := <-c.completedMapTasks
		for _, filename := range mapTask.Output {
			reduceId := extractReduceIdFromMapOutputFileName(filename)
			reduceFiles[reduceId] = append(reduceFiles[reduceId], filename)
		}
	}

	for id, files := range reduceFiles {
		c.reduceTasks <- createTask(Reduce, id, files)
	}
}

func (c *Coordinator) checkDone() {
	for reduceTaskCount := 0; reduceTaskCount < c.r; reduceTaskCount++ {
		<-c.completedReduceTasks
	}

	c.done <- struct{}{}
	close(c.done)
}

// filename format of map task: mr-x-y
func extractReduceIdFromMapOutputFileName(filename string) TaskIdentity {
	hyphenIdexBeforeY := strings.LastIndex(filename, "-")

	id, err := strconv.Atoi(filename[hyphenIdexBeforeY:])
	if err != nil {
		log.Panicf("error format of filename:%s", filename)
	}

	return TaskIdentity(id)
}
