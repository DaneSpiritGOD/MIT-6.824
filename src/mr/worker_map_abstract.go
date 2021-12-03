package mr

import (
	"fmt"
	"io"
	"os"
)

type cacheTarget interface {
	io.WriteCloser
	Complete() (string, error)
}

type fileCache struct {
	cacheFile      *os.File
	targetFilePath string

	closed bool
}

func (e *fileCache) Write(p []byte) (n int, err error) {
	return e.cacheFile.Write(p)
}

func (e *fileCache) Close() error {
	defer func() {
		e.closed = true
	}()
	return e.cacheFile.Close()
}

func (e *fileCache) Complete() (string, error) {
	if !e.closed {
		err := e.Close()
		if err != nil {
			return "", fmt.Errorf("err: %v in closing the file handle", err)
		}
	}

	err := os.Rename(e.cacheFile.Name(), e.targetFilePath)
	if err != nil {
		return "", fmt.Errorf("error:%v occurs when renaming file from %s to %s", err, e.cacheFile.Name(), e.targetFilePath)
	}

	return e.targetFilePath, nil
}

type createCacheTarget func(
	mapTaskId TaskIdentity,
	reduceTaskId TaskIdentity) (cacheTarget, error)

func createFileCacheTarget(
	mapTaskId TaskIdentity,
	reduceTaskId TaskIdentity) (cacheTarget, error) {
	file, err := os.CreateTemp("", "")
	if err != nil {
		return &fileCache{}, fmt.Errorf("error:%v occurs when creating temp file", err)
	}

	return &fileCache{file, createOutputFileNameForMapTask(mapTaskId, reduceTaskId), false}, nil
}
