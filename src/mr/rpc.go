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
type WorkerIdentity uint32

type TaskState int

const (
	Idle       TaskState = 0
	InProgress TaskState = 1
	Completed  TaskState = 2
)

type TaskIdentity uint32

type Task struct {
	Id       TaskIdentity
	WorkerId WorkerIdentity

	State TaskState

	Input  string
	Output string
}

func createTask(id TaskIdentity, input string) *Task {
	return &Task{Id: id, State: Idle, Input: input}
}

func (mt *Task) Start(worker WorkerIdentity) error {
	if mt.State != Idle {
		return fmt.Errorf("worker:%d cannot start task:%d due to state:%d", mt.WorkerId, mt.Id, mt.State)
	}

	mt.State = InProgress
	mt.WorkerId = worker
	return nil
}

func (mt *Task) Complete() error {
	if mt.State != InProgress {
		return fmt.Errorf("worker:%d cannot complete task:%d due to state:%d", mt.WorkerId, mt.Id, mt.State)
	}

	mt.State = Completed
	mt.Output = ""
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
