package mr

import (
	"bytes"
	"errors"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"
)

func TestMapTaskOutputCreater(t *testing.T) {
	expected := "mr-1-2"
	actual := createOutputFileNameForMapTask(1, 2)
	if actual != expected {
		t.Error("output file name of map task not correct")
	}
}

func TestReduceIdExtractor(t *testing.T) {
	actual := extractReduceIdFromMapOutputFileName("mr-1-2")
	if actual != 2 {
		t.Errorf("reduce id calculated error: %d", actual)
	}
}

func TestMapTaskResults(t *testing.T) {
	getHashIdFunc := func(key string) TaskIdentity {
		if key == "" {
			return 0
		}

		d := key[0] - '0'
		if d <= 9 {
			return TaskIdentity(d)
		}
		return 0
	}

	data := []KeyValue{
		{"3", "1"},
		{"23", "1"},
		{"1", "1"},
		{"1", "1"},
		{"14", "1"},
		{"16", "1"},
		{"2354", "1"},
		{"368", "1"},
		{"245", "1"},
		{"1", "1"},
	}

	expectedResults := map[TaskIdentity][]KeyValues{
		1: {
			{"1", []string{"1", "1", "1"}},
			{"14", []string{"1"}},
			{"16", []string{"1"}},
		},
		2: {
			{"23", []string{"1"}},
			{"2354", []string{"1"}},
			{"245", []string{"1"}},
		},
		3: {
			{"3", []string{"1"}},
			{"368", []string{"1"}},
		},
	}

	actualResults := organizeMapTaskResults(getHashIdFunc, data)
	if len(expectedResults) != len(actualResults) {
		t.Errorf("expected len: %d, actual len: %d", len(expectedResults), len(actualResults))
	}

	for _, a := range actualResults {
		if !reflect.DeepEqual(expectedResults[a.reduceTaskId], a.sortedResults) {
			t.Errorf("expected item: %v, actual item: %v not equal", expectedResults[a.reduceTaskId], a.sortedResults)
		}
	}
}

func TestMapCache(t *testing.T) {
	const expected = "hello"
	expectedTargetPath := createOutputFileNameForMapTask(1, 1)

	cache, err := createFileMapCacheTarget(1, 1)
	if err != nil {
		t.Errorf("err: %v in creating cache", err)
	}

	if _, err = cache.Write([]byte(expected)); err != nil {
		t.Errorf("err: %v in writing to cache", err)
	}

	if _, err = os.Stat(expectedTargetPath); !errors.Is(err, os.ErrNotExist) {
		os.Remove(expectedTargetPath)
	}

	targetPath, err := cache.Complete()
	if err != nil {
		t.Errorf("err: %v in completing cache", err)
	}

	targetContent, err := os.ReadFile(targetPath)
	if err != nil {
		t.Errorf("err: %v in opening target file", err)
	}

	if string(targetContent) != expected {
		t.Errorf("expected: %s, actual: %s", expected, targetContent)
	}

	os.Remove(targetPath)
}

type memoryCache struct{ *bytes.Buffer }

func (e *memoryCache) Read(p []byte) (n int, err error)  { return e.Buffer.Read(p) }
func (e *memoryCache) Write(p []byte) (n int, err error) { return e.Buffer.Write(p) }
func (e *memoryCache) Close() error                      { return nil }
func (e *memoryCache) Complete() (string, error)         { return e.String(), nil }

func createMemoryCacheTarget(
	mapTaskId TaskIdentity,
	reduceTaskId TaskIdentity) (mapCacheTarget, error) {
	return &memoryCache{new(bytes.Buffer)}, nil
}

func createMemoryReduceReader(s string) (reduceInputReader, error) {
	return &memoryCache{bytes.NewBufferString(s)}, nil
}

func TestEncodeIntoReduceFiles(t *testing.T) {
	mapResults := []*mapTaskResult{{
		1,
		[]KeyValues{
			{"1", []string{"1", "1", "1"}},
			{"14", []string{"1"}},
			{"16", []string{"1"}},
		},
	}, {
		2,
		[]KeyValues{
			{"23", []string{"1"}},
			{"2354", []string{"1"}},
			{"245", []string{"1"}},
		},
	}, {
		3,
		[]KeyValues{
			{"3", []string{"1"}},
			{"368", []string{"1"}},
		},
	}}

	expectedContents := []string{
		"[{\"Key\":\"1\",\"Values\":[\"1\",\"1\",\"1\"]}," +
			"{\"Key\":\"14\",\"Values\":[\"1\"]}," +
			"{\"Key\":\"16\",\"Values\":[\"1\"]}]\n",
		"[{\"Key\":\"23\",\"Values\":[\"1\"]}," +
			"{\"Key\":\"2354\",\"Values\":[\"1\"]}," +
			"{\"Key\":\"245\",\"Values\":[\"1\"]}]\n",
		"[{\"Key\":\"3\",\"Values\":[\"1\"]}," +
			"{\"Key\":\"368\",\"Values\":[\"1\"]}]\n",
	}

	actualContents, err := encodeIntoReduceFiles(1, mapResults, createMemoryCacheTarget)
	if err != nil {
		t.Error(err)
	}

	if len(actualContents) != len(mapResults) {
		t.Errorf("expected len: %d, actual len: %d", len(mapResults), len(actualContents))
	}

	sort.Strings(actualContents)

	for index, actualContent := range actualContents {
		if actualContent != expectedContents[index] {
			t.Errorf("expected: %s, actual: %s", expectedContents[index], actualContent)
		}
	}
}

func TestDecodeReduceTaskInput(t *testing.T) {
	inputs := []string{
		"[{\"Key\":\"1\",\"Values\":[\"1\",\"1\",\"1\"]}," +
			"{\"Key\":\"2354\",\"Values\":[\"1\"]}," +
			"{\"Key\":\"14\",\"Values\":[\"1\"]}," +
			"{\"Key\":\"16\",\"Values\":[\"1\"]}]\n",
		"[{\"Key\":\"23\",\"Values\":[\"1\"]}," +
			"{\"Key\":\"2354\",\"Values\":[\"1\"]}," +
			"{\"Key\":\"368\",\"Values\":[\"1\"]}," +
			"{\"Key\":\"245\",\"Values\":[\"1\",\"1\"]}]\n",
		"[{\"Key\":\"3\",\"Values\":[\"1\"]}," +
			"{\"Key\":\"14\",\"Values\":[\"1\"]}," +
			"{\"Key\":\"368\",\"Values\":[\"1\"]}]\n",
	}

	expectedResults := []KeyValue{
		{"1", "3"},
		{"14", "2"},
		{"16", "1"},
		{"23", "1"},
		{"2354", "2"},
		{"245", "2"},
		{"3", "1"},
		{"368", "2"},
	}

	reduceFuncBackup := reduceFunc
	reduceFunc = func(key string, values []string) string {
		return strconv.Itoa(len(values))
	}

	defer func() {
		reduceFunc = reduceFuncBackup
	}()

	actualResults, err := decodeInputThenReduce(inputs, createMemoryReduceReader)
	if err != nil {
		t.Errorf("%v", err)
	}

	if !reflect.DeepEqual(expectedResults, actualResults) {
		t.Errorf("expected: %v, actual: %v", expectedResults, actualResults)
	}
}
