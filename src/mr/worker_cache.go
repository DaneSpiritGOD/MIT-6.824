package mr

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

type outputCache interface {
	io.WriteCloser
	Complete() (string, error)
}

type fileCache struct {
	temp      *os.File
	finalPath string

	closed bool
}

func (e *fileCache) Write(p []byte) (n int, err error) {
	return e.temp.Write(p)
}

func (e *fileCache) Close() error {
	defer func() {
		e.closed = true
	}()
	return e.temp.Close()
}

func (e *fileCache) Complete() (string, error) {
	if !e.closed {
		err := e.Close()
		if err != nil {
			return "", fmt.Errorf("err: %v in closing the file handle", err)
		}
	}

	err := os.Rename(e.temp.Name(), e.finalPath)
	if err != nil {
		return "", fmt.Errorf("error:%v occurs when renaming file from %s to %s", err, e.temp.Name(), e.finalPath)
	}

	return e.finalPath, nil
}

type createCacheForMap func(mapTaskId TaskIdentity, reduceTaskId TaskIdentity) (outputCache, error)

func createFileCacheForMap(
	mapTaskId TaskIdentity,
	reduceTaskId TaskIdentity) (outputCache, error) {
	file, err := os.CreateTemp("", "")
	if err != nil {
		return &fileCache{}, fmt.Errorf("error:%v occurs when creating temp file", err)
	}

	return &fileCache{file, getOutputFileNameForMap(mapTaskId, reduceTaskId), false}, nil
}

func createFileInputReaderForReduce(file string) (reduceInputReader, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("error: %v in opening file: %v", err, file)
	}

	return f, nil
}

func createFileOutputWriterForReduce(reduceTaskId TaskIdentity) (outputCache, error) {
	file, err := os.CreateTemp("", "")
	if err != nil {
		return &fileCache{}, fmt.Errorf("error:%v occurs when creating temp file", err)
	}

	return &fileCache{file, getOutputFileNameForReduceTask(reduceTaskId), false}, nil
}

type memoryCache struct{ *bytes.Buffer }

func (e *memoryCache) Read(p []byte) (n int, err error)  { return e.Buffer.Read(p) }
func (e *memoryCache) Write(p []byte) (n int, err error) { return e.Buffer.Write(p) }
func (e *memoryCache) Close() error                      { return nil }
func (e *memoryCache) Complete() (string, error)         { return e.String(), nil }

func createMemoryCacheForMap(
	mapTaskId TaskIdentity,
	reduceTaskId TaskIdentity) (outputCache, error) {
	return &memoryCache{new(bytes.Buffer)}, nil
}

func createMemoryInputReaderForReduce(s string) (reduceInputReader, error) {
	return &memoryCache{bytes.NewBufferString(s)}, nil
}
