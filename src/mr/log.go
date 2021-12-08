package mr

import (
	"io"
	"log"
)

func init() {
	configLog(false)
}

func configLog(enable bool) {
	if enable {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	} else {
		log.SetFlags(0)
		log.SetOutput(io.Discard)
	}
}
