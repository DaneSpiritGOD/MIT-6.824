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

func encode(mapTaskId TaskIdentity, groups []*reduceKeyValues) ([]string, error) {
	var outputs []string
	curReduceTaskId := TaskIdentity(-1)
	var curFile *os.File
	var curTargetFile string
	var curEncoder *json.Encoder
	var err error
	for _, g := range groups {
		if g.TaskId != curReduceTaskId {
			curTempFile := curFile.Name()
			curFile.Close()
			if os.Rename(curTempFile, curTargetFile) != nil {
				return nil, fmt.Errorf("error:%v occurs when renaming file from %s to %s", err, curTempFile, curTargetFile)
			}

			outputs = append(outputs, curTargetFile)

			curTargetFile = createMapTaskOutputFileName(mapTaskId, g.TaskId)
			curFile, err = os.CreateTemp("", "")
			if err != nil {
				return nil, fmt.Errorf("error:%v occurs when creating temp file of %s", err, curTargetFile)
			}

			curEncoder = json.NewEncoder(curFile)
		}

		err = curEncoder.Encode(g.KeyValues)
		if err != nil {
			curFile.Close()
			return nil, fmt.Errorf("error:%v occurs when encoding to json", err)
		}
	}

	return outputs, nil
}
