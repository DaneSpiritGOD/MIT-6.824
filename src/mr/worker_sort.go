package mr

import "sort"

type reduceKeyValues struct {
	TaskId TaskIdentity
	KeyValues
}

type ByIdKey []*reduceKeyValues

func (a ByIdKey) Len() int      { return len(a) }
func (a ByIdKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByIdKey) Less(i, j int) bool {
	return a[i].TaskId <= a[j].TaskId && a[i].Key < a[j].Key
}

func (info *workerInfo) getHashId(key string) TaskIdentity {
	return TaskIdentity(ihash(key) % info.reduceCount)
}

func sortByIdKey(
	getReduceTaskId func(string) TaskIdentity,
	kvs []KeyValue) []*reduceKeyValues {
	m := make(map[string]*reduceKeyValues)
	var result []*reduceKeyValues

	for _, kv := range kvs {
		key := kv.Key
		g, ok := m[key]
		if !ok {
			g = &reduceKeyValues{getReduceTaskId(key), KeyValues{Key: key}}
			m[key] = g
			result = append(result, g)
		}
		g.Values = append(g.Values, kv.Value)
	}

	sort.Sort(ByIdKey(result))
	return result
}
