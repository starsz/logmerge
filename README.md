
logmerge
===========

This is a library that merges multiple ordered log files based on timestamp. Logmerge provides a customizable function to get the timestamp from each line, and use min heap for efficient sorting.It's used to merge nginx access log and error log for me.


For complete documentation, check out the [Godoc][1].


Usage
===========

First, define the getTime handler or use TimStartHandler in library.

```go
func getTime([]byte) (int64, logmerge.Action, error) {
        // do parse time in this
}
```

Second, start to merge.

```go
filePath := []string{"./testdata/base1.log", "./testdata/base2.log"}
outputPath := "./testdata/output.log"
getTime := logmerge.TimeStartHandler("2006/01/02 15:04:05")

err := logmerge.Merge(filePath, outputPath, getTime)
```


Example
=========

File #1

```
2020/01/18 12:20:30 [error] 177003#0: *1004128358 recv() failed (104: Connection reset by peer)
2020/01/18 12:21:55 [error] 177004#0: *1004127283 recv() failed (104: Connection reset by peer)
2020/01/18 12:24:38 [error] 176995#0: *1004136348 [lua] heartbeat.lua:107: cb_heartbeat(): failed to connect: 127.0.0.1:403, timeout, context: ngx.timer
2020/01/18 12:31:05 [error] 177004#0: *1004144640 recv() failed (104: Connection reset by peer)
```

File #2

```
2020/01/18 12:20:33 [error] 177003#0: *1004128358 recv() failed (104: Connection reset by peer)
2020/01/18 12:21:25 [error] 177004#0: *1004127283 recv() failed (104: Connection reset by peer)
2020/01/18 12:26:38 [error] 176995#0: *1004136348 [lua] heartbeat.lua:107: cb_heartbeat(): failed to connect: 127.0.0.1:403, timeout, context: ngx.timer
2020/01/18 12:40:05 [error] 177004#0: *1004144640 recv() failed (104: Connection reset by peer)
```

Output File

```
2020/01/18 12:20:30 [error] 177003#0: *1004128358 recv() failed (104: Connection reset by peer)
2020/01/18 12:20:33 [error] 177003#0: *1004128358 recv() failed (104: Connection reset by peer)
2020/01/18 12:21:25 [error] 177004#0: *1004127283 recv() failed (104: Connection reset by peer)
2020/01/18 12:21:55 [error] 177004#0: *1004127283 recv() failed (104: Connection reset by peer)
2020/01/18 12:24:38 [error] 176995#0: *1004136348 [lua] heartbeat.lua:107: cb_heartbeat(): failed to connect: 127.0.0.1:403, timeout, context: ngx.timer
2020/01/18 12:26:38 [error] 176995#0: *1004136348 [lua] heartbeat.lua:107: cb_heartbeat(): failed to connect: 127.0.0.1:403, timeout, context: ngx.timer
2020/01/18 12:31:05 [error] 177004#0: *1004144640 recv() failed (104: Connection reset by peer)
2020/01/18 12:40:05 [error] 177004#0: *1004144640 recv() failed (104: Connection reset by peer)
```

[1]: https://godoc.org/github.com/starsz/logmerge
