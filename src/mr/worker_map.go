package mr

import (
	"encoding/json"
	"fmt"
)

const NeverUsedReduceTaskId = TaskIdentity(-1)

type reduceGroup struct {
	mapTaskId    TaskIdentity
	reduceTaskId TaskIdentity
	encoder      *json.Encoder
	cacheTarget
}

func makeNewReduceGroup(
	mapTaskId TaskIdentity,
	reduceTaskId TaskIdentity,
	createCache createCacheTarget) (reduceGroup, error) {
	if reduceTaskId == NeverUsedReduceTaskId {
		return reduceGroup{reduceTaskId: NeverUsedReduceTaskId}, nil
	}

	cache, err := createCache(mapTaskId, reduceTaskId)
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

	output, err := rg.cacheTarget.Complete()
	if err != nil {
		return "", err
	}

	return output, nil
}

func encodeIntoReduceFiles(
	mapTaskId TaskIdentity,
	groups []*mapTaskResultGroup,
	createCache createCacheTarget) ([]string, error) {
	var reduceFiles []string

	lastReduceGroup, _ := makeNewReduceGroup(mapTaskId, NeverUsedReduceTaskId, createCache)

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

			lastReduceGroup, err = makeNewReduceGroup(mapTaskId, group.TaskId, createCache)
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
