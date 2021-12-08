package mr

import "log"

func init() {
	configLog()
}

func configLog() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}
