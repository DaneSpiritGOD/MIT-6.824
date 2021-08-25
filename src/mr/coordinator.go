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

type taskHolder struct {
	idleTasks       chan *Task
	inProgressTasks chan *Task
	completedTasks  chan *Task
}

type Coordinator struct {
	// Your definitions here.
	maxWorkerId uint32

	m int // count of map tasks
	r int // count of reduce task

	mapHolder    taskHolder
	reduceHolder taskHolder

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
	task := func() Task {
		select {
		case mapTask := <-c.mapHolder.idleTasks:
			return *mapTask
		case reduceTask := <-c.reduceHolder.idleTasks:
			return *reduceTask
		case <-c.done:
			return DoneTask
		}
	}()
	task.WorkerId = workerId
	*reply = task
	return nil
}

func (c *Coordinator) ReceiveTaskOutput(args *Task, _ *struct{}) error {
	switch args.Type {
	case MapTaskType:
		c.mapHolder.completedTasks <- args
	case ReduceTaskType:
		c.reduceHolder.completedTasks <- args
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

	createTaskHolderFunc := func() taskHolder {
		return taskHolder{
			idleTasks:       make(chan *Task),
			inProgressTasks: make(chan *Task),
			completedTasks:  make(chan *Task),
		}
	}

	c.mapHolder = createTaskHolderFunc()
	c.reduceHolder = createTaskHolderFunc()

	go c.createMapTasks(files)
	go c.createReduceTasks()
	go c.checkDone()

	c.server()
	return &c
}

func (c *Coordinator) createMapTasks(files []string) {
	for id, file := range files {
		c.mapHolder.idleTasks <- createTask(MapTaskType, TaskIdentity(id+1), []string{file})
	}
}

func (c *Coordinator) createReduceTasks() {
	reduceFiles := make(map[TaskIdentity][]string)

	for mapTaskCount := 0; mapTaskCount < c.m; mapTaskCount++ {
		mapTask := <-c.mapHolder.completedTasks
		for _, filename := range mapTask.Output {
			reduceId := extractReduceIdFromMapOutputFileName(filename)
			reduceFiles[reduceId] = append(reduceFiles[reduceId], filename)
		}
	}

	for id, files := range reduceFiles {
		c.reduceHolder.idleTasks <- createTask(ReduceTaskType, id, files)
	}
}

func (c *Coordinator) checkDone() {
	for reduceTaskCount := 0; reduceTaskCount < c.r; reduceTaskCount++ {
		<-c.reduceHolder.completedTasks
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
