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

// Add your RPC definitions here.

// Does nothing for now
type NewJobArgs struct {
	Nothing int
}

type JobReply struct {
	JobType           string
	Index             int
	R                 uint
	N                 uint
	InputFileLocation string
	MapFileLocations  []string
}

type FileLocationArgs struct {
	Index            int
	MapFileLocations []string
}

type AckReply struct {
	Received bool
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
