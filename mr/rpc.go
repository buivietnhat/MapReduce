package mr

//
// RPC definitions.
//
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type RequestWorkArgs struct {
	M int // worker number
}

type RequestWorkReply struct {
	WorkType    WorkType
	Index       int
	NReduce     int
	MapFileList []string
}

type ReportWorkArgs struct {
	WType WorkType // work type
	Index int      // work index
}

type ReportWorkReply struct {
	Ok bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
