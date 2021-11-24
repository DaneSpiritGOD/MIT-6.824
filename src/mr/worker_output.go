package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

// filename format of map task: mr-x-y
func extractReduceIdFromMapOutputFileName(filename string) TaskIdentity {
	hyphenIdexBeforeY := strings.LastIndex(filename, "-")

	id, err := strconv.Atoi(filename[hyphenIdexBeforeY+1:])
	if err != nil {
		log.Panicf("error format of filename:%s", filename)
	}

	return TaskIdentity(id)
}

func createMapTaskOutputFileName(mapTaskId TaskIdentity, reduceTaskId TaskIdentity) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskId, reduceTaskId)
}
