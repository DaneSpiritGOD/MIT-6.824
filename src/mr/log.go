package mr

import (
	"io"
	"log"
)

func init() {
	configLog()
}

func configLog() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetOutput(io.Discard)
}
