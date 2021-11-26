package mr

import (
	"bytes"
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestMapTaskOutputCreater(t *testing.T) {
	expected := "mr-1-2"
	actual := createMapTaskOutputFileName(1, 2)
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

func TestSortByGroup(t *testing.T) {
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

	expectedResults := [...]mapTaskResultGroup{
		{1, KeyValues{"1", []string{"1", "1", "1"}}},
		{1, KeyValues{"14", []string{"1"}}},
		{1, KeyValues{"16", []string{"1"}}},
		{2, KeyValues{"23", []string{"1"}}},
		{2, KeyValues{"2354", []string{"1"}}},
		{2, KeyValues{"245", []string{"1"}}},
		{3, KeyValues{"3", []string{"1"}}},
		{3, KeyValues{"368", []string{"1"}}},
	}

	actualResults := getOrderedMapResultGroups(getHashIdFunc, data)
	if len(expectedResults) != len(actualResults) {
		t.Errorf("expected len: %d, actual len: %d", len(expectedResults), len(actualResults))
	}

	for i, e := range expectedResults {
		if !reflect.DeepEqual(e, *actualResults[i]) {
			t.Errorf("expected item: %v, actual item: %v not equal", e, *actualResults[i])
		}
	}
}

func TestMapCache(t *testing.T) {
	const expected = "hello"
	expectedTargetPath := createMapTaskOutputFileName(1, 1)

	cache, err := createFileCacheTarget(1, 1)
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

func (e *memoryCache) Write(p []byte) (n int, err error) { return e.Buffer.Write(p) }

func (e *memoryCache) Close() error { return nil }

func (e *memoryCache) Complete() (string, error) { return e.String(), nil }

func createMemoryCacheTarget(
	mapTaskId TaskIdentity,
	reduceTaskId TaskIdentity) (cacheTarget, error) {
	return &memoryCache{new(bytes.Buffer)}, nil
}

func createReduceGroupWithMemoryCache(
	mapTaskId TaskIdentity,
	reduceTaskId TaskIdentity) (reduceGroup, error) {
	return createReduceGroupWithCache(mapTaskId, reduceTaskId, createMemoryCacheTarget)
}

func TestEncodeIntoReduceFiles(t *testing.T) {
	mapResults := []*mapTaskResultGroup{
		{1, KeyValues{"1", []string{"1", "1", "1"}}},
		{1, KeyValues{"14", []string{"1"}}},
		{1, KeyValues{"16", []string{"1"}}},
		{2, KeyValues{"23", []string{"1"}}},
		{2, KeyValues{"2354", []string{"1"}}},
		{2, KeyValues{"245", []string{"1"}}},
		{3, KeyValues{"3", []string{"1"}}},
		{3, KeyValues{"368", []string{"1"}}},
	}

	expectedContents := []string{
		"{\"Key\":\"1\",\"Values\":[\"1\",\"1\",\"1\"]}\n" +
			"{\"Key\":\"14\",\"Values\":[\"1\"]}\n" +
			"{\"Key\":\"16\",\"Values\":[\"1\"]}\n",
		"{\"Key\":\"23\",\"Values\":[\"1\"]}\n" +
			"{\"Key\":\"2354\",\"Values\":[\"1\"]}\n" +
			"{\"Key\":\"245\",\"Values\":[\"1\"]}\n",
		"{\"Key\":\"3\",\"Values\":[\"1\"]}\n" +
			"{\"Key\":\"368\",\"Values\":[\"1\"]}\n",
	}

	fileContents, err := encodeIntoReduceFiles(1, mapResults, createReduceGroupWithMemoryCache)
	if err != nil {
		t.Error(err)
	}

	for index, actualContent := range fileContents {
		if actualContent != expectedContents[index] {
			t.Errorf("expected: %s, actual: %s", expectedContents[index], actualContent)
		}
	}
}
