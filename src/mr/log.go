package mr

import (
	"io"
	"log"
)

func init() {
	configLog(true)
}

func configLog(enable bool) {
	if enable {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	} else {
		log.SetFlags(0)
		log.SetOutput(io.Discard)
	}
}
