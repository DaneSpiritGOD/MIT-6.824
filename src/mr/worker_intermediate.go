package mr

import "sort"

type KeyValues struct {
	Key    string
	Values []string
}

type getReduceTaskIdFunc func(key string) TaskIdentity

func (info *workerInfo) getHashId(key string) TaskIdentity {
	return TaskIdentity(ihash(key) % info.reduceCount)
}

type mapTaskResult struct {
	reduceTaskId  TaskIdentity
	sortedResults []KeyValues
}

func organizeMapTaskResults(
	getReduceTaskId getReduceTaskIdFunc,
	originalData []KeyValue) []*mapTaskResult {
	reduceTaskIds := make(map[TaskIdentity][]string)
	keys := make(map[string][]string)

	for _, kv := range originalData {
		key := kv.Key

		values, ok := keys[key]
		if !ok {
			reduceTaskId := getReduceTaskId(key)
			reduceTaskIds[reduceTaskId] = append(reduceTaskIds[reduceTaskId], key)
		}
		keys[key] = append(values, kv.Value)
	}

	var results []*mapTaskResult
	for reduceTaskId, keys2 := range reduceTaskIds {
		sort.Strings(keys2)

		result := &mapTaskResult{reduceTaskId: reduceTaskId}
		for _, key := range keys2 {
			result.sortedResults = append(result.sortedResults, KeyValues{key, keys[key]})
		}
		results = append(results, result)
	}
	return results
}
