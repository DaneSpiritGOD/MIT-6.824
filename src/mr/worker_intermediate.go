package mr

import (
	"encoding/json"
	"fmt"
	"os"
)

type encoder interface {
}

type fileEncoder struct {
	file *os.File
	*json.Encoder
}

func (e fileEncoder) Write(kvs KeyValues) error {
	err := e.Encoder.Encode(kvs)
	if err != nil {
		e.file.Close()
		return fmt.Errorf("error:%v occurs when encoding to json", err)
	}

	return nil
}

func (e fileEncoder) Rename(target string) error {
	tempFile := e.file.Name()
	e.file.Close()
	err := os.Rename(tempFile, target)
	if err != nil {
		return fmt.Errorf("error:%v occurs when renaming file from %s to %s", err, tempFile, target)
	}

	return nil
}

func encodeIntoReduceFiles(mapTaskId TaskIdentity, groups []*mapTaskResultGroup) ([]string, error) {
	var reduceFiles []string

	const NeverUsedReduceTaskId = TaskIdentity(-1)
	curReduceTaskId := NeverUsedReduceTaskId

	var curTempFile *os.File
	var curTargetFile string
	var curEncoder *json.Encoder

	trySwitchReduceGroup := func(lastReduceId TaskIdentity, newReduceId TaskIdentity) error {
		cleanupAndCompleteLastReduceResultGroup := func(lastTempFile *os.File, lastTargetFile string) error {
			if lastReduceId == NeverUsedReduceTaskId {
				return nil
			}

			curTempFileName := curTempFile.Name()
			curTempFile.Close()
			err := os.Rename(curTempFileName, curTargetFile)
			if err != nil {
				return fmt.Errorf("error:%v occurs when renaming file from %s to %s", err, curTempFileName, curTargetFile)
			}

			reduceFiles = append(reduceFiles, curTargetFile)
			return nil
		}

		createNewReduceResultGroup := func(newReduceId TaskIdentity) error {
			curTargetFile = createMapTaskOutputFileName(mapTaskId, newReduceId)
			curTempFile, err := os.CreateTemp("", "")
			if err != nil {
				return fmt.Errorf("error:%v occurs when creating temp file of %s", err, curTargetFile)
			}

			curEncoder = json.NewEncoder(curTempFile)
			return nil
		}

		if lastReduceId != newReduceId {
			err := cleanupAndCompleteLastReduceResultGroup()
			if err != nil {
				return err
			}

			err = createNewReduceResultGroup(newReduceId)
			if err != nil {
				return err
			}
		}
	}

	encodeCore := func(groupData KeyValues) error {
		err := curEncoder.Encode(groupData)
		if err != nil {
			curTempFile.Close()
			return fmt.Errorf("error:%v occurs when encoding to json", err)
		}
		return nil
	}

	for _, group := range groups {

		encodeCore(group.KeyValues)
	}

	return reduceFiles, nil
}
