package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskState int

const (
	Idle       TaskState = 0
	InProgress TaskState = 1
	Completed  TaskState = 2
)

type WorkerIdentity int

type WorkerInfo struct {
	Id WorkerIdentity
}

type MapTask struct {
	State       TaskState
	Worker      WorkerIdentity
	InputPath   string // path of input file
	OutcomePath string // path of file produced by map task
}

type ReduceTask struct {
	State     TaskState
	Worker    WorkerIdentity
	InputPath string
}

func createMapTask(input string) *MapTask {
	return &MapTask{State: Idle, InputPath: input}
}

func (mt *MapTask) Start(worker WorkerIdentity) error {
	if mt.State != Idle {
		return fmt.Errorf("worker:%d cannot start due to its state:%d", mt.Worker, mt.State)
	}

	mt.State = InProgress
	mt.Worker = worker
	return nil
}

func (mt *MapTask) Complete() error {
	if mt.State != InProgress {
		return fmt.Errorf("worker:%d cannot complete due to its state:%d", mt.Worker, mt.State)
	}

	mt.State = Completed
	mt.OutcomePath = ""
	return nil
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
