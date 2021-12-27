package mr

import (
	"context"
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
			go func() {
				c.mapHolder.inProgressTasks <- mapTask
			}()
			return *mapTask
		case reduceTask := <-c.reduceHolder.idleTasks:
			go func() {
				c.reduceHolder.inProgressTasks <- reduceTask
			}()
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
	container := func() *taskContainer {
		switch task.Type {
		case MapTaskType:
			return c.mapHolder
		case ReduceTaskType:
			return c.reduceHolder
		default:
			return nil
		}
	}()

	if container == nil {
		return nil
	}

	// delete from waiting flags when task is completed
	if cancelFunc, ok := container.inProgressWaitingFlags.LoadAndDelete(task.Id); ok {
		cancelFunc.(context.CancelFunc)()

		log.Printf("Master: receive completed task [%v:%v output: %v] from Worker [%d].", task.Type, task.Id, task.Data, task.WorkerId)
		container.completedTasks <- task
	} else {
		// when there is no waiting flag for the specific in-progress task,
		// either the task is canceled due to timeout or another task with same id has been done before already
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
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.m = len(files)
	c.r = nReduce

	c.mapHolder = createTaskContainer()
	c.reduceHolder = createTaskContainer()

	go c.createMapTasks(files)
	go c.createReduceTasks()

	go c.mapHolder.checkInProgressTask()
	go c.reduceHolder.checkInProgressTask()

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
