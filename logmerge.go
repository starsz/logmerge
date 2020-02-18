/*
Package logmerge provides a method to merge multiple log files by timestamp.

Example:

	filePath := []string{"1.log", "2.log", "3.log"}
	outputPath := "output.log"
	getTime := func(line []byte) (int64, logmerge.Action, error) {
				tm, err := time.Parse("20060102150405", string(line[:14]))
				if err != nil {
					return 0, logmerge.SKIP, nil
			    }

				return tm.Unix(), logmerge.NOP, nil
			}

	err := logMerge.Merge(filepath, outputPath, getTime)
*/
package logmerge

import (
	"bufio"
	"container/heap"
	"os"
	"path/filepath"
)

// Define the timeHandler action
type Action int

/*
	NOP: no extra option
	SKIP: skip this line
	STOP: stop file merging
*/
const (
	NOP = iota
	SKIP
	STOP
)

/*
	Define handlers for getting timestam from each line.
  	You can do somethings like print log in this function.
*/
type TimeHandler = func([]byte) (int64, Action, error)

type fileUnit struct {
	filename  string
	scanner   *bufio.Scanner
	getTime   TimeHandler
	timestamp int64
	line      []byte
	eof       bool
}

type fileHeap []*fileUnit

func (fh fileHeap) Len() int { return len(fh) }

func (fh fileHeap) Less(i, j int) bool { return fh[i].timestamp < fh[j].timestamp }

func (fh fileHeap) Swap(i, j int) {
	fh[i], fh[j] = fh[j], fh[i]
}

func (fh *fileHeap) Push(h interface{}) {
	*fh = append(*fh, h.(*fileUnit))
}

func (fh *fileHeap) Pop() interface{} {
	n := len(*fh)
	fu := (*fh)[n-1]
	*fh = (*fh)[:n-1]
	return fu
}

func (fu *fileUnit) readLine() error {
	var action Action
	var tm int64
	var line []byte
	var err error

	scanner := fu.scanner
	for {
		if ok := scanner.Scan(); !ok {
			if err = scanner.Err(); err != nil {
				return err
			}

			// EOF
			fu.eof = true
			return nil
		}

		line = scanner.Bytes()
		tm, action, err = fu.getTime(line)
		if action == SKIP {
			continue
		} else if action == STOP {
			return err
		}

		break
	}

	fu.timestamp = tm
	fu.line = line

	return nil
}

// Merge files to output file, and use getTime function to get timestmap.
func Merge(filePath []string, outputFile string, getTime TimeHandler) error {
	fHeap := new(fileHeap)

	// init heap
	heap.Init(fHeap)
	for _, fp := range filePath {
		fd, err := os.Open(fp)
		if err != nil {
			return err
		}

		defer fd.Close()

		scanner := bufio.NewScanner(fd)

		fu := &fileUnit{
			scanner:  scanner,
			filename: filepath.Base(fp),
			getTime:  getTime,
		}

		err = fu.readLine()
		if err != nil {
			return err
		}

		if !fu.eof {
			heap.Push(fHeap, fu)
		}
	}

	outputFd, err := os.Create(outputFile)
	if err != nil {
		return err
	}

	defer outputFd.Close()

	writer := bufio.NewWriter(outputFd)

	for (*fHeap).Len() > 0 {
		fu := heap.Pop(fHeap).(*fileUnit)

		if _, err := writer.Write(append(fu.line, '\n')); err != nil {
			return err
		}

		writer.Flush()

		err := fu.readLine()
		if err != nil {
			return err
		}

		if !fu.eof {
			heap.Push(fHeap, fu)
		}
	}

	return nil
}
