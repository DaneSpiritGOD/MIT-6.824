package mr

import (
	"fmt"
	"io"
	"os"
)

type reduceInputReader interface {
	io.ReadCloser
}

type createReduceReader func(string) (reduceInputReader, error)

func createFileReduceReader(file string) (reduceInputReader, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("error: %v in opening file: %v", err, file)
	}

	return f, nil
}
