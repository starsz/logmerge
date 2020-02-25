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
	"compress/gzip"
	"container/heap"
	"errors"
	"os"
	"path/filepath"
)

// Define the timeHandler action.
type Action int

const (
	// NOP: no extra option
	NOP = iota
	// SKIP: skip this line
	SKIP
	// STOP: stop file merging
	STOP
)

var (
	// NEED_TIMEHANDLER returned when the getTime function is nil.
	NEED_TIMEHANDLER = errors.New("need time handler")
)

/*
	Define handlers for getting timestamp from each line.
*/
type TimeHandler = func([]byte) (int64, Action, error)

type fileReader struct {
	filename  string
	scanner   *bufio.Scanner
	timestamp int64
	line      []byte
	eof       bool
	getTime   TimeHandler
}

type Option struct {
	SrcPath []string    // Merge src File Path
	DstPath string      // The filePath merge to
	SrcGzip bool        // Wheter src file is in gzip format
	DstGzip bool        // Merge file in gzip format
	GetTime TimeHandler // The function to getTime from each line
}

type fileHeap struct {
	readers []*fileReader
	writer  *bufio.Writer
}

func (fh fileHeap) Len() int { return len(fh.readers) }

func (fh fileHeap) Less(i, j int) bool { return fh.readers[i].timestamp < fh.readers[j].timestamp }

func (fh fileHeap) Swap(i, j int) {
	fh.readers[i], fh.readers[j] = fh.readers[j], fh.readers[i]
}

func (fh *fileHeap) Push(h interface{}) {
	(*fh).readers = append((*fh).readers, h.(*fileReader))
}

func (fh *fileHeap) Pop() interface{} {
	n := len((*fh).readers)
	fr := (*fh).readers[n-1]
	(*fh).readers = (*fh).readers[:n-1]
	return fr
}

func (fu *fileReader) readLine() error {
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

func (fh *fileHeap) merge() error {
	writer := fh.writer
	for (*fh).Len() > 0 {
		fr := heap.Pop(fh).(*fileReader)
		if _, err := writer.Write(append(fr.line, '\n')); err != nil {
			return err
		}

		writer.Flush()

		err := fr.readLine()
		if err != nil {
			return err
		}

		if !fr.eof {
			heap.Push(fh, fr)
		}
	}

	return nil
}

// Merge files to output file, and use getTime function to get timestmap.
func Merge(srcPath []string, dstPath string, getTime TimeHandler) error {
	option := Option{
		SrcPath: srcPath,
		DstPath: dstPath,
		GetTime: getTime,
	}

	return MergeByOption(option)
}

// Use option to control merge behavior.
func MergeByOption(option Option) error {
	if option.GetTime == nil {
		return NEED_TIMEHANDLER
	}

	fHeap := new(fileHeap)

	heap.Init(fHeap)

	for _, fp := range option.SrcPath {
		fd, err := os.Open(fp)
		if err != nil {
			return err
		}

		defer fd.Close()

		var scanner *bufio.Scanner
		if option.SrcGzip {
			gzReader, err := gzip.NewReader(fd)
			if err != nil {
				return err
			}

			defer gzReader.Close()

			scanner = bufio.NewScanner(gzReader)
		} else {
			scanner = bufio.NewScanner(fd)
		}

		fr := &fileReader{
			scanner:  scanner,
			filename: filepath.Base(fp),
			getTime:  option.GetTime,
		}

		err = fr.readLine()
		if err != nil {
			return err
		}

		if !fr.eof {
			heap.Push(fHeap, fr)
		}
	}

	dstFd, err := os.Create(option.DstPath)
	if err != nil {
		return err
	}

	defer dstFd.Close()

	var writer *bufio.Writer
	if option.DstGzip {
		gzWriter := gzip.NewWriter(dstFd)

		defer gzWriter.Close()

		writer = bufio.NewWriter(gzWriter)
	} else {
		writer = bufio.NewWriter(dstFd)
	}

	fHeap.writer = writer

	return fHeap.merge()
}
