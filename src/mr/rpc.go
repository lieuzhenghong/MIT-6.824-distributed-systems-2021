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

// NewJobArgs ...
// Does nothing for now
type NewJobArgs struct {
	Nothing int
}

// JobReply ...
// Reply given from the server
// when workers ask it for a new job.
// JobType is the most important and must not be nil.
// Other fields are optional.
// JobType can be any of "map", "reduce", "null", "exit".
type JobReply struct {
	JobType           string
	Index             int  // index of map/reduce worker
	R                 uint // number of reduce workers
	N                 uint // number of map workers (not used)
	InputFileLocation string
	MapFileLocations  []string
}

// FileLocationArgs ...
// Given by a Map worker when it completes.
// Lists the locations of intermediate files it produced.
type FileLocationArgs struct {
	Index            int
	MapFileLocations []string
}

// AckReply ...
// Reply to the FileLocationArgs
// Not really used.
type AckReply struct {
	Received bool
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
