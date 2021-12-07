package mr

import "sort"

type Values []string

type KeyValues struct {
	Key string
	Values
}

type getReduceTaskIdFunc func(key string) TaskIdentity

func (info *workerInfo) getHashId(key string) TaskIdentity {
	return TaskIdentity(ihash(key) % info.reduceCount)
}

type mapOutput struct {
	reduceTaskId  TaskIdentity
	sortedResults []KeyValues
}

func reorganizeMapOutputs(
	getReduceTaskId getReduceTaskIdFunc,
	originalData []KeyValue) []*mapOutput {
	reduceIdWithKeys := make(map[TaskIdentity][]string)
	keyWithValues := make(map[string]Values)

	for _, kv := range originalData {
		key := kv.Key

		values, ok := keyWithValues[key]
		if !ok {
			reduceTaskId := getReduceTaskId(key)
			reduceIdWithKeys[reduceTaskId] = append(reduceIdWithKeys[reduceTaskId], key)
		}
		keyWithValues[key] = append(values, kv.Value)
	}

	var results []*mapOutput
	for reduceTaskId, keys2 := range reduceIdWithKeys {
		sort.Strings(keys2)

		result := &mapOutput{reduceTaskId: reduceTaskId}
		for _, key := range keys2 {
			result.sortedResults = append(result.sortedResults, KeyValues{key, keyWithValues[key]})
		}
		results = append(results, result)
	}
	return results
}
