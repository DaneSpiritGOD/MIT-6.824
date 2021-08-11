package mr

import "fmt"

type taskState int

const (
	Idle       taskState = 0
	InProgress taskState = 1
	Completed  taskState = 2
)

type workerIndentity int

type mapTask struct {
	State       taskState
	Worker      workerIndentity
	InputPath   string // path of input file
	OutcomePath string // path of file produced by map task
}

type reduceTask struct {
	State     taskState
	Worker    workerIndentity
	InputPath string
}

func createMapTask(input string) *mapTask {
	return &mapTask{State: Idle, InputPath: input}
}

func (mt *mapTask) Start(worker workerIndentity) error {
	if mt.State != Idle {
		return fmt.Errorf("worker:%d cannot start due to its state:%d", mt.Worker, mt.State)
	}

	mt.State = InProgress
	mt.Worker = worker
	return nil
}

func (mt *mapTask) Complete() error {
	if mt.State != InProgress {
		return fmt.Errorf("worker:%d cannot complete due to its state:%d", mt.Worker, mt.State)
	}

	mt.State = Completed
	mt.OutcomePath = ""
	return nil
}
