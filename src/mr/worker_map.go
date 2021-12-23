package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func decodeInputThenMap(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot open %v", filename)
	}

	defer func() {
		file.Close()
	}()

	content, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("cannot read %v", filename)
	}

	return mapFunc(filename, string(content)), nil
}

func encodeMapOutputs(
	mapTaskId TaskIdentity,
	mapTaskResults map[TaskIdentity][]KeyValues,
	createCache createCacheForMap) ([]string, error) {
	var mapOutputs []string

	for reduceTaskId, mapResult := range mapTaskResults {
		cache, err := createCache(mapTaskId, reduceTaskId)
		if err != nil {
			return nil, fmt.Errorf("error in creating cache: %v", err)
		}

		encoder := json.NewEncoder(cache)
		err = encoder.Encode(mapResult)
		if err != nil {
			cache.Close()
			return nil, fmt.Errorf("error in encoding into cache: %v", err)
		}

		// TODO: if another running worker is executing same but time-out task and has created files already,
		// what we should do here?
		// The same goes for reduce task.
		content, _ := cache.Complete() // ignore the error here
		mapOutputs = append(mapOutputs, content)
	}

	return mapOutputs, nil
}
