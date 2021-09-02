package mr

import (
	"encoding/json"
	"reflect"
	"strings"
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

	expectedResults := [...]reduceKeyValues{
		{1, KeyValues{"1", []string{"1", "1", "1"}}},
		{1, KeyValues{"14", []string{"1"}}},
		{1, KeyValues{"16", []string{"1"}}},
		{2, KeyValues{"23", []string{"1"}}},
		{2, KeyValues{"2354", []string{"1"}}},
		{2, KeyValues{"245", []string{"1"}}},
		{3, KeyValues{"3", []string{"1"}}},
		{3, KeyValues{"368", []string{"1"}}},
	}

	actualResults := sortByIdKey(getHashIdFunc, data)
	if len(expectedResults) != len(actualResults) {
		t.Errorf("expected len: %d, actual len: %d", len(expectedResults), len(actualResults))
	}

	for i, e := range expectedResults {
		if !reflect.DeepEqual(e, *actualResults[i]) {
			t.Errorf("expected item: %v, actual item: %v not equal", e, *actualResults[i])
		}
	}
}

type TestToEncode struct {
	A, B string
}

func TestJsonEncoder(t *testing.T) {
	buf := new(strings.Builder)
	e := json.NewEncoder(buf)
	e.Encode(TestToEncode{"a", "b"})

	expected1 := "{\"A\":\"a\",\"B\":\"b\"}\n"
	actual1 := buf.String()
	if actual1 != expected1 {
		t.Errorf("expected: %v (len = %d), actual: %v (len = %d)", expected1, len(expected1), actual1, len(actual1))
	}

	e.Encode(TestToEncode{"aa", "bb"})

	expected2 := "{\"A\":\"a\",\"B\":\"b\"}\n{\"A\":\"aa\",\"B\":\"bb\"}\n"
	actual2 := buf.String()
	if buf.String() != expected2 {
		t.Errorf("expected: %v (len = %d), actual: %v (len = %d)", expected2, len(expected2), actual2, len(actual2))
	}
}
