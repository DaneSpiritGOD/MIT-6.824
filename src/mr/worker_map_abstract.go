package mr

import (
	"fmt"
	"io"
	"os"
)

type mapCacheTarget interface {
	io.WriteCloser
	Complete() (string, error)
}

type fileMapCache struct {
	cacheFile      *os.File
	targetFilePath string

	closed bool
}

func (e *fileMapCache) Write(p []byte) (n int, err error) {
	return e.cacheFile.Write(p)
}

func (e *fileMapCache) Close() error {
	defer func() {
		e.closed = true
	}()
	return e.cacheFile.Close()
}

func (e *fileMapCache) Complete() (string, error) {
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

type createMapCacheTarget func(
	mapTaskId TaskIdentity,
	reduceTaskId TaskIdentity) (mapCacheTarget, error)

func createFileMapCacheTarget(
	mapTaskId TaskIdentity,
	reduceTaskId TaskIdentity) (mapCacheTarget, error) {
	file, err := os.CreateTemp("", "")
	if err != nil {
		return &fileMapCache{}, fmt.Errorf("error:%v occurs when creating temp file", err)
	}

	return &fileMapCache{file, createOutputFileNameForMapTask(mapTaskId, reduceTaskId), false}, nil
}
