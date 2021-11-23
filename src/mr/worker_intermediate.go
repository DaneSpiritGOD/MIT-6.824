package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const NeverUsedReduceTaskId = TaskIdentity(-1)

type cacheTarget interface {
	io.WriteCloser
	Complete() (string, error)
}

type fileCache struct {
	cacheFile      *os.File
	targetFilePath string
}

func (e *fileCache) Write(p []byte) (n int, err error) {
	return e.cacheFile.Write(p)
}

func (e *fileCache) Close() error {
	return e.cacheFile.Close()
}

func (e *fileCache) Complete() (string, error) {
	err := os.Rename(e.cacheFile.Name(), e.targetFilePath)
	if err != nil {
		return "", fmt.Errorf("error:%v occurs when renaming file from %s to %s", err, e.cacheFile.Name(), e.targetFilePath)
	}

	return e.targetFilePath, nil
}

type createCacheTarget func(
	mapTaskId TaskIdentity,
	reduceTaskId TaskIdentity) (cacheTarget, error)

var fileCacheCreator createCacheTarget = func(
	mapTaskId TaskIdentity,
	reduceTaskId TaskIdentity) (cacheTarget, error) {
	file, err := os.CreateTemp("", "")
	if err != nil {
		return &fileCache{}, fmt.Errorf("error:%v occurs when creating temp file", err)
	}

	return &fileCache{file, createMapTaskOutputFileName(mapTaskId, reduceTaskId)}, nil
}

type reduceGroup struct {
	mapTaskId    TaskIdentity
	reduceTaskId TaskIdentity
	encoder      *json.Encoder
	cacheTarget
}

func makeNewReduceGroup(
	mapTaskId TaskIdentity,
	reduceTaskId TaskIdentity,
	cacheFunc createCacheTarget) (reduceGroup, error) {
	if reduceTaskId == NeverUsedReduceTaskId {
		return reduceGroup{reduceTaskId: NeverUsedReduceTaskId}, nil
	}

	cache, err := cacheFunc(mapTaskId, reduceTaskId)
	if err != nil {
		return reduceGroup{}, err
	}

	return reduceGroup{
		mapTaskId,
		reduceTaskId,
		json.NewEncoder(cache),
		cache,
	}, nil
}

func (rg reduceGroup) appendData(kvs KeyValues) error {
	err := rg.encoder.Encode(kvs)
	if err != nil {
		rg.Close()
		return fmt.Errorf("error:%v occurs when encoding to json", err)
	}
	return nil
}

func (rg reduceGroup) complete() (string, error) {
	if rg.reduceTaskId == NeverUsedReduceTaskId {
		return "", nil
	}

	err := rg.Close()
	if err != nil {
		return "", err
	}

	output, err := rg.cacheTarget.Complete()
	if err != nil {
		return "", err
	}

	return output, nil
}

func encodeIntoReduceFiles(
	mapTaskId TaskIdentity,
	groups []*mapTaskResultGroup,
	cacheFunc createCacheTarget) ([]string, error) {
	var reduceFiles []string

	lastReduceGroup, _ := makeNewReduceGroup(mapTaskId, NeverUsedReduceTaskId, cacheFunc)

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

			lastReduceGroup, err = makeNewReduceGroup(mapTaskId, group.TaskId, cacheFunc)
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
