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

type createReduceGroup func(mapTaskId TaskIdentity, reduceTaskId TaskIdentity) (reduceGroup, error)

func createReduceGroupWithCache(
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

func createReduceGroupWithFileCache(
	mapTaskId TaskIdentity,
	reduceTaskId TaskIdentity) (reduceGroup, error) {
	return createReduceGroupWithCache(mapTaskId, reduceTaskId, createFileCacheTarget)
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
	createGroup createReduceGroup) ([]string, error) {
	var reduceFiles []string

	lastReduceGroup, _ := createGroup(mapTaskId, NeverUsedReduceTaskId)

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

			lastReduceGroup, err = createGroup(mapTaskId, group.TaskId)
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
