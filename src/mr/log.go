package mr

import (
	"io"
	"log"
)

// cd MIT-6.824/src/main
// bash test-mr.sh &> mr-log
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
