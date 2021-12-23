package mr

import (
	"context"
	"log"
	"sync"
	"time"
)

type taskContainer struct {
	idleTasks chan *Task

	inProgressTasks                   chan *Task
	cancelWaitingForInProgressTimeout *sync.Map

	completedTasks chan *Task
}

func createTaskContainer() *taskContainer {
	return &taskContainer{
		idleTasks: make(chan *Task),

		inProgressTasks:                   make(chan *Task),
		cancelWaitingForInProgressTimeout: &sync.Map{},

		completedTasks: make(chan *Task),
	}
}

func (h *taskContainer) checkInProgressTask() {
	for inProgressTask := range h.inProgressTasks {
		ctx, cancelFunc := context.WithCancel(context.Background())
		h.cancelWaitingForInProgressTimeout.Store(inProgressTask.Id, cancelFunc)

		go func(ctxInner context.Context, task *Task) {
			select {
			case <-ctxInner.Done():
				log.Printf("Master: in-progress task (%v:%v) is completed.", task.Type, task.Id)
			case <-time.After(time.Second * 10):
				h.cancelWaitingForInProgressTimeout.Delete(task.Id)
				h.idleTasks <- task
				log.Printf("Master: in-progress task (%v:%v) is timeout. Add it to idle task channel.", task.Type, task.Id)
			}
		}(ctx, inProgressTask)
	}
}

func (h *taskContainer) cleanup() {
	h.cancelWaitingForInProgressTimeout.Range(func(key interface{}, value interface{}) bool {
		id := key.(TaskIdentity)
		value.(context.CancelFunc)()
		log.Printf("Master: left task (%v) is cancelled after previous stage is over", id)
		return true
	})
}
