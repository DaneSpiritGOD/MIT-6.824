package mr

import (
	"testing"
)

func TestSortByGroup(t *testing.T) {
	getHashIdFunc := func(key string) int {
		if key == "" {
			return 0
		}

		d := key[0] - '0'
		if d < 0 || d > 9 {
			return 0
		}
		return int(d)
	}
}
