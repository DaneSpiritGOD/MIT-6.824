package mr

import (
	"encoding/json"
	"fmt"
	"os"
)

func decodeFilesOfReduceTask(files []string) error {
	keys := make(map[string]Values)

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			return fmt.Errorf("error in opening file: %v", err)
		}

		var obj []KeyValues
		decoder := json.NewDecoder(f)
		decoder.Decode(&obj)

		for _, kv := range obj {
			keys[kv.Key] = append(keys[kv.Key], kv.Values...)
		}
	}

	return nil
}
