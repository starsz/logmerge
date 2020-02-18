package logmerge

import (
	"fmt"
	"time"
)

/*
	Easy way to get timehandler to deal with logs starting with date.
*/
func TimeStartHandler(layout string) TimeHandler {
	f := func(line []byte) (int64, Action, error) {
		if len(line) < len(layout) {
			return 0, SKIP, nil
		}

		tm, err := time.Parse(layout, string(line[:len(layout)]))
		if err != nil {
			fmt.Printf("err: %s\n", err.Error())
			return 0, SKIP, nil
		}

		return tm.Unix(), NOP, nil
	}

	return f
}
