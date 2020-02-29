package logmerge

import (
	"compress/gzip"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	EXPECTED1 = `2020/01/18 12:20:30 [error] 177003#0: *1004128358 recv() failed (104: Connection reset by peer)
2020/01/18 12:20:33 [error] 177003#0: *1004128358 recv() failed (104: Connection reset by peer)
2020/01/18 12:21:25 [error] 177004#0: *1004127283 recv() failed (104: Connection reset by peer)
2020/01/18 12:21:55 [error] 177004#0: *1004127283 recv() failed (104: Connection reset by peer)
2020/01/18 12:24:38 [error] 176995#0: *1004136348 [lua] heartbeat.lua:107: cb_heartbeat(): failed to connect: 127.0.0.1:403, timeout, context: ngx.timer
2020/01/18 12:26:38 [error] 176995#0: *1004136348 [lua] heartbeat.lua:107: cb_heartbeat(): failed to connect: 127.0.0.1:403, timeout, context: ngx.timer
2020/01/18 12:31:05 [error] 177004#0: *1004144640 recv() failed (104: Connection reset by peer)
2020/01/18 12:40:05 [error] 177004#0: *1004144640 recv() failed (104: Connection reset by peer)
`

	EXPECTED2 = `2020/01/18 12:20:30 [error] 177003#0: *1004128358 recv() failed (104: Connection reset by peer)
2020/01/18 12:21:55 [error] 177004#0: *1004127283 recv() failed (104: Connection reset by peer)
2020/01/18 12:24:38 [error] 176995#0: *1004136348 [lua] heartbeat.lua:107: cb_heartbeat(): failed to connect: 127.0.0.1:403, timeout, context: ngx.timer
2020/01/18 12:31:05 [error] 177004#0: *1004144640 recv() failed (104: Connection reset by peer)
`
)

func readFile(path string, isGzip bool) ([]byte, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer os.Remove(path)
	defer fd.Close()

	if isGzip {
		reader, err := gzip.NewReader(fd)
		if err != nil {
			return nil, err
		}

		defer reader.Close()
		return ioutil.ReadAll(reader)
	} else {
		return ioutil.ReadAll(fd)
	}
}

func doMerge(filePath []string, outputPath string, getTime TimeHandler) (string, error) {
	err := Merge(filePath, outputPath, getTime)
	if err != nil {
		return "", err
	}

	outputFd, err := os.Open(outputPath)
	if err != nil {
		return "", err
	}
	defer os.Remove(outputPath)
	defer outputFd.Close()

	outputContent, err := ioutil.ReadAll(outputFd)
	if err != nil {
		return "", err
	}

	return string(outputContent), nil
}

func TestBaseMerge(t *testing.T) {
	filePath := []string{"./testdata/base1.log", "./testdata/base2.log"}
	outputPath := "./testdata/output.log"

	getTime := TimeStartHandler("2006/01/02 15:04:05")

	res, err := doMerge(filePath, outputPath, getTime)
	if err != nil {
		t.Errorf("Merge file error: %s", err.Error())
		return
	}

	if string(res) != EXPECTED1 {
		t.Errorf("Different content, merge failed\n%s\n%s", string(res), EXPECTED1)
	}
}

func TestEmptyMerge(t *testing.T) {
	filePath := []string{"./testdata/empty1.log", "./testdata/empty2.log"}
	outputPath := "./testdata/output.log"
	getTime := TimeStartHandler("2006/01/02 15:04:05")

	res, err := doMerge(filePath, outputPath, getTime)
	if err != nil {
		t.Errorf("Merge file error: %s", err.Error())
		return
	}

	expected := ""

	if string(res) != expected {
		t.Errorf("Different content, merge failed\n%s\n%s", string(res), expected)
	}
}

func TestNilMerge(t *testing.T) {
	getTime := TimeStartHandler("2006/01/02 15:04:05")

	err := Merge(nil, "", getTime)

	if !strings.Contains(err.Error(), "no such file or directory") {
		t.Errorf("Merge empty file error: %s", err.Error())
	}

	res, err := doMerge(nil, "./testdata/output.log", getTime)
	if err != nil {
		t.Errorf("Merge file error: %s", err.Error())
	}

	expected := ""
	if res != expected {
		t.Errorf("Different content, merge failed\n%s\n%s", string(res), expected)
	}
}

func TestMixMerge(t *testing.T) {
	filePath := []string{"./testdata/base1.log", "./testdata/empty2.log"}
	outputPath := "./testdata/output.log"

	getTime := TimeStartHandler("2006/01/02 15:04:05")

	res, err := doMerge(filePath, outputPath, getTime)
	if err != nil {
		t.Errorf("Merge file error: %s", err.Error())
		return
	}

	if string(res) != EXPECTED2 {
		t.Errorf("Different content, merge failed\n%s\n%s", string(res), EXPECTED2)
	}
}

func TestStopMerge(t *testing.T) {
	filePath := []string{"./testdata/base1.log", "./testdata/empty2.log"}
	outputPath := "./testdata/output.log"

	gettime := func(line []byte) (int64, Action, error) {
		return 0, STOP, errors.New("test for stop")
	}

	_, err := doMerge(filePath, outputPath, gettime)

	if err.Error() != "test for stop" {
		t.Errorf("Test stopping merge error")
	}
}

func TestMidStopMerge(t *testing.T) {
	filePath := []string{"./testdata/base1.log", "./testdata/empty2.log"}
	outputPath := "./testdata/output.log"

	gettime := func(line []byte) (int64, Action, error) {
		if string(line[:19]) == "2020/01/18 12:21:55" {
			return 0, STOP, errors.New("test for stop")
		}

		tm, err := time.Parse("2006/01/02 15:04:05", string(line[:19]))
		if err != nil {
			return 0, SKIP, nil
		}

		return tm.Unix(), NOP, nil
	}

	_, err := doMerge(filePath, outputPath, gettime)
	if err.Error() != "test for stop" {
		t.Errorf("Test stopping merge error")
	}

	outputFd, err := os.Open(outputPath)
	if err != nil {
		t.Errorf("Open outputPath error: %s", err.Error())
		return
	}
	defer os.Remove(outputPath)
	defer outputFd.Close()

	outputContent, err := ioutil.ReadAll(outputFd)
	if err != nil {
		t.Errorf("Read output fd error: %s", err.Error())
		return
	}

	expected := `2020/01/18 12:20:30 [error] 177003#0: *1004128358 recv() failed (104: Connection reset by peer)
`

	if string(outputContent) != expected {
		t.Errorf("Different content, merge failed")
	}
}

func TestGzipMerge(t *testing.T) {
	filePath := []string{"./testdata/base1.log.gz", "./testdata/base2.log.gz"}
	dstPath := "./testdata/output.log"

	getTime := TimeStartHandler("2006/01/02 15:04:05")
	err := MergeByOption(Option{SrcPath: filePath, DstPath: dstPath,
		SrcGzip: true, GetTime: getTime})
	if err != nil {
		t.Errorf("Merge file error: %s", err.Error())
	}
	dstFd, err := os.Open(dstPath)
	if err != nil {
		t.Errorf("Open dstPath error: %s", err.Error())
	}
	defer dstFd.Close()

	res, err := ioutil.ReadAll(dstFd)
	if err != nil {
		t.Errorf("read dstfd error: %s", err.Error())
	}

	if string(res) != EXPECTED1 {
		t.Errorf("Different content, merge failed\n%s\n%s", string(res), EXPECTED1)
	}

	os.Remove(dstPath)

	dstPath = "./testdata/output.log.gz"
	err = MergeByOption(Option{SrcPath: filePath, DstPath: dstPath,
		SrcGzip: true, DstGzip: true, GetTime: getTime})
	if err != nil {
		t.Errorf("Merge file error: %s", err.Error())
	}

	dstFd, err = os.Open(dstPath)
	if err != nil {
		t.Errorf("Open dstPath error: %s", err.Error())
	}

	defer dstFd.Close()

	reader, err := gzip.NewReader(dstFd)
	if err != nil {
		t.Errorf("gzip NewReader error: %s", err.Error())
	}
	defer reader.Close()

	res, err = ioutil.ReadAll(reader)
	if err != nil {
		t.Errorf("Merge file error: %s", err.Error())
	}

	if string(res) != EXPECTED1 {
		t.Errorf("Different content, merge failed\n%s\n%s", string(res), EXPECTED1)
	}

	os.Remove(dstPath)

}

func TestDeleteSrcMerge(t *testing.T) {
	filePath := []string{"./testdata/base1.log", "./testdata/base2.log"}

	// prepare
	var copyPath []string
	for _, fp := range filePath {
		cp := fp + "_copy"
		copyFd, err := os.Create(cp)
		if err != nil {
			t.Errorf("create %s error: %s", cp, err.Error())
		}

		fd, err := os.Open(fp)
		if err != nil {
			t.Errorf("create %s error: %s", cp, err.Error())
		}

		_, err = io.Copy(copyFd, fd)
		if err != nil {
			t.Errorf("Create copyPath: %s", err.Error())
			return
		}

		copyFd.Close()
		fd.Close()
		copyPath = append(copyPath, cp)
	}
	outputPath := "./testdata/output.log"

	getTime := TimeStartHandler("2006/01/02 15:04:05")

	option := Option{
		SrcPath:   copyPath,
		DstPath:   outputPath,
		DeleteSrc: true,
		GetTime:   getTime,
	}

	err := MergeByOption(option)
	if err != nil {
		t.Errorf("Merge file error: %s", err.Error())
		return
	}

	res, err := readFile(outputPath, false)
	if err != nil {
		t.Errorf("readFile error: %s", err.Error())
	}

	if string(res) != EXPECTED1 {
		t.Errorf("Different content, merge failed\n%s\nexpected:\n%s", string(res), EXPECTED1)
	}

	for _, copyFp := range copyPath {
		fi, _ := os.Stat(copyFp)

		if fi != nil {
			t.Errorf("file exist")
		}
	}
}
