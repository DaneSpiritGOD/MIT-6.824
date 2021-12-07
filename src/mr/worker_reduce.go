package mr

import (
	"encoding/json"
	"fmt"
	"sort"
)

func decodeInputThenReduce(files []string, create createReduceReader) ([]KeyValue, error) {
	var keys []string
	keyMap := make(map[string]Values)

	readFile := func(file_ string) error {
		f, err := create(file_)
		if err != nil {
			return err
		}

		defer f.Close()

		var obj []KeyValues
		decoder := json.NewDecoder(f)
		err = decoder.Decode(&obj)
		if err != nil {
			return fmt.Errorf("error: %v in encoding file", err)
		}

		for _, kv := range obj {
			values, ok := keyMap[kv.Key]
			if !ok {
				keys = append(keys, kv.Key)
			}

			keyMap[kv.Key] = append(values, kv.Values...)
		}

		return nil
	}

	for _, file := range files {
		if err := readFile(file); err != nil {
			return nil, err
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
