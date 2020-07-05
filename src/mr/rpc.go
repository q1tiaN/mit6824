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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskArgs struct {
	Aim string
}

type TaskReply struct {
	Tasktype    string // type of a task map or reduce or all jobs finished
	Filename    string // the input file name
	InterPrefix string // the prefix of intermediate files
	NnReduce    int    // reduce task numbers
	Taskindex   int
	MapTaskTot  int
}

type FinishArgs struct {
	Tasktype  string
	Filename  string
	Taskindex int
}

type FinishReply struct {
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
