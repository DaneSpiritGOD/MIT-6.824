package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	maxWorkerId uint32

	m int // count of map tasks
	r int // count of reduce task

	mapHolder    *taskContainer
	reduceHolder *taskContainer

	done chan struct{}
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

func (c *Coordinator) GetWorkerId(_ struct{}, reply *WorkerIdentity) error {
	*reply = WorkerIdentity(atomic.AddUint32(&c.maxWorkerId, 1))
	return nil
}

func (c *Coordinator) GetReduceCount(_ struct{}, reply *int) error {
	*reply = c.r
	return nil
}

func (c *Coordinator) AssignTask(workerId *WorkerIdentity, reply *Task) error {
	task := func() Task {
		select {
		case mapTask := <-c.mapHolder.idleTasks:
			c.mapHolder.monitorInProgress(mapTask)
			return *mapTask
		case reduceTask := <-c.reduceHolder.idleTasks:
			c.reduceHolder.monitorInProgress(reduceTask)
			return *reduceTask
		case <-c.done:
			return DoneTask
		}
	}()
	task.WorkerId = *workerId
	*reply = task
	return nil
}

func (c *Coordinator) ReceiveTaskOutput(task *Task, _ *struct{}) error {
	var container *taskContainer
	switch task.Type {
	case MapTaskType:
		container = c.mapHolder
	case ReduceTaskType:
		container = c.reduceHolder
	default:
		return nil
	}

	if running, ok := container.inProgressWaitingFlags.LoadAndDelete(task.Id); ok {
		running.(inProgressUnit).markDone()

		log.Printf("Master: receive completed task [%v:%v output: %v] from Worker [%d].", task.Type, task.Id, task.Data, task.WorkerId)
		container.completedTasks <- task
	} else {
		// when there is no waiting flag for the specific in-progress task,
		// either the task is canceled due to timeout or same-id task has been done before already
		log.Printf("Master: receive REDUNDANT completed task [%v:%v output: %v] from Worker [%d].", task.Type, task.Id, task.Data, task.WorkerId)
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
func (c *Coordinator) Done(_ struct{}, done *bool) error {
	// Your code here.
	select {
	case <-c.done:
		*done = true
	case <-time.After(time.Second * 1):
		*done = false
	}
	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func Start(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.m = len(files)
	c.r = nReduce

	c.mapHolder = createTaskContainer()
	c.reduceHolder = createTaskContainer() // TODO: reduce task container can be create on all map tasks done

	go c.createMapTasks(files)
	go c.createReduceTasks()
	go c.checkDone()

	c.server()
	return &c
}

func (c *Coordinator) createMapTasks(files []string) {
	log.Printf("Master: totally [%d] map tasks.", len(files))
	for id, file := range files {
		c.mapHolder.idleTasks <- createTask(MapTaskType, TaskIdentity(id), []string{file})
	}
	log.Println("Master: all map tasks are totally assigned.")
}

func (c *Coordinator) createReduceTasks() {
	reduceFiles := make(map[TaskIdentity][]string)

	for mapTaskCount := 0; mapTaskCount < c.m; mapTaskCount++ {
		mapTask := <-c.mapHolder.completedTasks

		for _, filename := range mapTask.Data {
			reduceId := extractReduceIdFromMapOutputFileName(filename)
			reduceFiles[reduceId] = append(reduceFiles[reduceId], filename)
		}
	}

	log.Println("Master: all map tasks are completed.")
	log.Println("Master: clean up left map tasks.")
	c.mapHolder.cleanup()

	for id, files := range reduceFiles {
		c.reduceHolder.idleTasks <- createTask(ReduceTaskType, id, files)
	}
	log.Println("Master: all reduce tasks are totally assigned.")
}

func (c *Coordinator) checkDone() {
	for reduceTaskCount := 0; reduceTaskCount < c.r; reduceTaskCount++ {
		<-c.reduceHolder.completedTasks
	}

	log.Println("Master: all reduce tasks are completed.")
	log.Println("Master: clean up left reduce tasks.")
	c.reduceHolder.cleanup()

	c.done <- struct{}{}
	close(c.done)
	log.Println("Master: done.")
}
