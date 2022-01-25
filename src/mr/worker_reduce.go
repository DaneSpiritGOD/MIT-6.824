package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

type reduceInputReader interface {
	io.ReadCloser
}

type createReduceReader func(string) (reduceInputReader, error)

func decodeInputThenReduce(files []string, create createReduceReader) ([]KeyValue, error) {
	var keys []string
	keyMap := make(map[string]Values)

	decodeFile := func(file_ string) ([]KeyValues, error) {
		f, err := create(file_)
		if err != nil {
			return nil, err
		}

		defer f.Close()

		var kvs []KeyValues
		decoder := json.NewDecoder(f)
		err = decoder.Decode(&kvs)
		if err != nil {
			return nil, fmt.Errorf("error: %v in encoding file", err)
		}

		return kvs, nil
	}

	for _, file := range files {
		kvs, err := decodeFile(file)
		if err != nil {
			return nil, err
		}

		for _, kv := range kvs {
			values, ok := keyMap[kv.Key]
			if !ok {
				keys = append(keys, kv.Key)
			}

			keyMap[kv.Key] = append(values, kv.Values...)
		}
	}

	sort.Strings(keys)
	var results []KeyValue
	for _, key := range keys {
		value := reduceFunc(key, keyMap[key])
		results = append(results, KeyValue{key, value})
	}

	return results, nil
}

func encodeReduceOutputs(keyValues []KeyValue, cache outputCache) (string, error) {
	for _, kv := range keyValues {
		_, err := io.WriteString(cache, formatInLine(kv.Key, kv.Value))
		if err != nil {
			cache.Close()
			return "", err
		}
	}

	return cache.Complete()
}
