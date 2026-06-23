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
const (
	MapTask = iota
	ReduceTask
	WaitTask
	ExitTask
)

const (
	Idle = iota
	InProgress
	Done
)

type AskTaskArgs struct {
}

type AskTaskReply struct {
	TaskType int

	TaskId   int
	Filename string

	NReduce int
	NMap    int
}

type ReportTaskArgs struct {
	TaskType int
	TaskId   int
}

type ReportTaskReply struct {
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
