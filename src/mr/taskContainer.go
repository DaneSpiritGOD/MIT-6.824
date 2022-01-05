package mr

import (
	"context"
	"log"
	"sync"
	"time"
)

type inProgressUnit struct {
	cancel context.CancelFunc
}

func (unit inProgressUnit) markDone() { unit.cancel() }

type taskContainer struct {
	idleTasks              chan *Task
	inProgressWaitingFlags *sync.Map
	completedTasks         chan *Task
}

func createTaskContainer() *taskContainer {
	return &taskContainer{
		idleTasks:              make(chan *Task),
		inProgressWaitingFlags: &sync.Map{},
		completedTasks:         make(chan *Task),
	}
}

func (tc *taskContainer) monitorInProgress(task *Task) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	tc.inProgressWaitingFlags.Store(task.Id, inProgressUnit{cancelFunc})

	go func(ctxInner context.Context, taskInner *Task) {
		select {
		case <-ctxInner.Done():
		case <-time.After(time.Second * 10):
			if running, ok := tc.inProgressWaitingFlags.LoadAndDelete(taskInner.Id); ok {
				running.(inProgressUnit).markDone()
				tc.idleTasks <- taskInner
				log.Printf("Master: in-progress task [%v:%v] is timeout. Add it to idle task pool.", taskInner.Type, taskInner.Id)
			}
		}
	}(ctx, task)
}

func (h *taskContainer) cleanup() {
	h.inProgressWaitingFlags.Range(func(key interface{}, value interface{}) bool {
		h.inProgressWaitingFlags.Delete(key)
		id := key.(TaskIdentity)
		value.(context.CancelFunc)()
		log.Printf("Master: left in-progress task (%v) is cancelled and removed after previous stage is over", id)
		return true
	})
}
