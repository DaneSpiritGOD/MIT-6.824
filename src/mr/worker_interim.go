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

func reorganizeMapOutputs(
	getReduceTaskId getReduceTaskIdFunc,
	originalData []KeyValue) map[TaskIdentity][]KeyValues {
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

	results := make(map[TaskIdentity][]KeyValues)
	for reduceTaskId, keys2 := range reduceIdWithKeys {
		sort.Strings(keys2)

		for _, key := range keys2 {
			results[reduceTaskId] = append(results[reduceTaskId], KeyValues{key, keyWithValues[key]})
		}
	}
	return results
}
