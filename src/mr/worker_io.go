package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

func getOutputFileNameForMap(mapTaskId TaskIdentity, reduceTaskId TaskIdentity) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskId, reduceTaskId)
}

// filename format of map task: mr-x-y
func extractReduceIdFromMapOutputFileName(filename string) TaskIdentity {
	hyphenIdexBeforeY := strings.LastIndex(filename, "-")

	id, err := strconv.Atoi(filename[hyphenIdexBeforeY+1:])
	if err != nil {
		log.Panicf("error format of filename:%s", filename)
	}

	return TaskIdentity(id)
}

func getOutputFileNameForReduceTask(reduceTaskId TaskIdentity) string {
	return fmt.Sprintf("mr-out-%d", reduceTaskId)
}

func formatInLine(key string, value string) string {
	return fmt.Sprintf("%v %v\n", key, value)
}
