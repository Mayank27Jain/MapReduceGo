package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

type WorkArgs struct {
	WorkerIndex int
}

type WorkReply struct {
	Operation int
	FileIndex int
	FileName  string
	AltNum    int
}

type ReportArgs struct {
	WorkerIndex int
}

type ReportReply struct {
	WorkerIndex int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
