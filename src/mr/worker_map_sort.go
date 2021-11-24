package mr

import "sort"

type mapTaskResultGroup struct {
	TaskId TaskIdentity
	KeyValues
}

type ByIdKey []*mapTaskResultGroup

func (a ByIdKey) Len() int      { return len(a) }
func (a ByIdKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByIdKey) Less(i, j int) bool {
	return a[i].TaskId <= a[j].TaskId && a[i].Key < a[j].Key
}

func (info *workerInfo) getHashId(key string) TaskIdentity {
	return TaskIdentity(ihash(key) % info.reduceCount)
}

func getOrderedMapResultGroups(
	reduceTaskIdFunc func(string) TaskIdentity,
	kvs []KeyValue) []*mapTaskResultGroup {
	m := make(map[string]*mapTaskResultGroup)
	var result []*mapTaskResultGroup

	for _, kv := range kvs {
		key := kv.Key
		g, ok := m[key]
		if !ok {
			g = &mapTaskResultGroup{reduceTaskIdFunc(key), KeyValues{Key: key}}
			m[key] = g
			result = append(result, g)
		}
		g.Values = append(g.Values, kv.Value)
	}

	sort.Sort(ByIdKey(result))
	return result
}
