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

const UnknownWorker WorkerIdentity = 0

type TaskIdentity int

type TaskType int

const (
	EmptyTaskType  TaskType = 0
	MapTaskType    TaskType = 1
	ReduceTaskType TaskType = 2
)

func (t WorkerIdentity) String() string {
	switch t {
	case UnknownWorker:
		return "UNKNOWN"
	default:
		return fmt.Sprint(uint32(t))
	}
}

func (t TaskType) String() string {
	switch t {
	case EmptyTaskType:
		return "EMPTY"
	case MapTaskType:
		return "MAP"
	case ReduceTaskType:
		return "REDUCE"
	default:
		return "UNKNOWN"
	}
}

type Task struct {
	Id       TaskIdentity
	WorkerId WorkerIdentity

	Type TaskType

	Data []string // input or output
}

var NilTask Task = Task{Type: EmptyTaskType}
var DoneTask Task = Task{Type: EmptyTaskType}

func createTask(taskType TaskType, id TaskIdentity, input []string) *Task {
	return &Task{Type: taskType, Id: id, Data: input}
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

func (task *Task) String() string {
	return fmt.Sprintf("Task (id: %v type: %v)", task.Id, task.Type)
}
