package mr

import (
	"encoding/json"
	"fmt"
	"os"
)

const NeverUsedReduceTaskId = TaskIdentity(-1)

type reduceGroup struct {
	mapTaskId    TaskIdentity
	reduceTaskId TaskIdentity
	tempFile     *os.File
	encoder      *json.Encoder
	finalTarget  string
}

func makeNewReduceGroup(mapTaskId TaskIdentity, reduceTaskId TaskIdentity) (reduceGroup, error) {
	if reduceTaskId == NeverUsedReduceTaskId {
		return reduceGroup{reduceTaskId: NeverUsedReduceTaskId}, nil
	}

	output := createMapTaskOutputFileName(mapTaskId, reduceTaskId)
	tempFile, err := os.CreateTemp("", "")
	if err != nil {
		return reduceGroup{}, fmt.Errorf("error:%v occurs when creating temp file of %s", err, output)
	}

	return reduceGroup{
		mapTaskId,
		reduceTaskId,
		tempFile,
		json.NewEncoder(tempFile),
		output,
	}, nil
}

func (rg reduceGroup) appendData(kvs KeyValues) error {
	err := rg.encoder.Encode(kvs)
	if err != nil {
		rg.tempFile.Close()
		return fmt.Errorf("error:%v occurs when encoding to json", err)
	}
	return nil
}

func (rg reduceGroup) complete() (string, error) {
	if rg.reduceTaskId == NeverUsedReduceTaskId {
		return "", nil
	}

	rg.tempFile.Close()
	err := os.Rename(rg.tempFile.Name(), rg.finalTarget)
	if err != nil {
		return "", fmt.Errorf("error:%v occurs when renaming file from %s to %s", err, rg.tempFile.Name(), rg.finalTarget)
	}

	return rg.finalTarget, nil
}

func encodeIntoReduceFiles(mapTaskId TaskIdentity, groups []*mapTaskResultGroup) ([]string, error) {
	var reduceFiles []string

	lastReduceGroup, _ := makeNewReduceGroup(mapTaskId, NeverUsedReduceTaskId)

	completeReduceGroup := func() error {
		target, err := lastReduceGroup.complete()
		if err != nil {
			return err
		}

		if target != "" {
			reduceFiles = append(reduceFiles, target)
		}

		return nil
	}

	for _, group := range groups {
		if lastReduceGroup.reduceTaskId != group.TaskId {
			err := completeReduceGroup()
			if err != nil {
				return nil, err
			}

			lastReduceGroup, err = makeNewReduceGroup(mapTaskId, group.TaskId)
			if err != nil {
				return nil, err
			}
		}

		lastReduceGroup.appendData(group.KeyValues)
	}

	err := completeReduceGroup()
	if err != nil {
		return nil, err
	}

	return reduceFiles, nil
}
