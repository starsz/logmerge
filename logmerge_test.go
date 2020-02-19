package logmerge

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

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

	expected := `2020/01/18 12:20:30 [error] 177003#0: *1004128358 recv() failed (104: Connection reset by peer)
2020/01/18 12:20:33 [error] 177003#0: *1004128358 recv() failed (104: Connection reset by peer)
2020/01/18 12:21:25 [error] 177004#0: *1004127283 recv() failed (104: Connection reset by peer)
2020/01/18 12:21:55 [error] 177004#0: *1004127283 recv() failed (104: Connection reset by peer)
2020/01/18 12:24:38 [error] 176995#0: *1004136348 [lua] heartbeat.lua:107: cb_heartbeat(): failed to connect: 127.0.0.1:403, timeout, context: ngx.timer
2020/01/18 12:26:38 [error] 176995#0: *1004136348 [lua] heartbeat.lua:107: cb_heartbeat(): failed to connect: 127.0.0.1:403, timeout, context: ngx.timer
2020/01/18 12:31:05 [error] 177004#0: *1004144640 recv() failed (104: Connection reset by peer)
2020/01/18 12:40:05 [error] 177004#0: *1004144640 recv() failed (104: Connection reset by peer)
`
	if string(res) != expected {
		t.Errorf("Different content, merge failed")
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
		t.Errorf("Different content, merge failed")
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

	expected := `2020/01/18 12:20:30 [error] 177003#0: *1004128358 recv() failed (104: Connection reset by peer)
2020/01/18 12:21:55 [error] 177004#0: *1004127283 recv() failed (104: Connection reset by peer)
2020/01/18 12:24:38 [error] 176995#0: *1004136348 [lua] heartbeat.lua:107: cb_heartbeat(): failed to connect: 127.0.0.1:403, timeout, context: ngx.timer
2020/01/18 12:31:05 [error] 177004#0: *1004144640 recv() failed (104: Connection reset by peer)
`

	if string(res) != expected {
		t.Errorf("Different content, merge failed")
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
