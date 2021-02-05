package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// TaskStatus ...
// Contains a Status field where 0 = idle, 1 = in progress, 2 = completed
type TaskStatus struct {
	Status uint8
}

// Master ...
// Contains MapTasksStatus, MapFileLocations, and ReduceTasksStatus.
// MapTasksStatus is a slice where...
type Master struct {
	// Your definitions here.
	N                  uint // number of map tasks
	R                  uint // number of reduce tasks
	InputFileLocations []string
	MapTasksStatus     []TaskStatus
	MapFileLocations   [][]string
	ReduceTasksStatus  []TaskStatus
	mu                 sync.Mutex
}

func (m *Master) bAllMapJobsComplete() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < len(m.MapTasksStatus); i++ {
		if m.MapTasksStatus[i].Status != 2 {
			// println("Map jobs not yet complete")
			return false
		}
	}
	// println("All map jobs complete")
	return true
}

func (m *Master) bAllReduceJobsComplete() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < len(m.ReduceTasksStatus); i++ {
		fileNameToFind := "mr-out-" + fmt.Sprint(i)
		_, err := os.Stat(fileNameToFind)
		if os.IsNotExist(err) {
			// pass
		} else {
			m.ReduceTasksStatus[i].Status = 2
		}
		if m.ReduceTasksStatus[i].Status != 2 {
			// println("Reduce jobs not yet complete")
			return false
		}
	}
	// println("All reduce jobs complete")
	return true
}

func (m *Master) bAllJobsComplete() bool {
	// Returns true if and only if all jobs have completed
	// println("Checking if all jobs are complete...")
	return m.bAllMapJobsComplete() && m.bAllReduceJobsComplete()
}

// FindFirstIdleTask ...
// Finds the first idle task of a TaskStatus slice.
// Returns the smallest index of the first idle map task.
// Returns -1 if no idle tasks exist.
func FindFirstIdleTask(taskstatusslice *[]TaskStatus, m *Master) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < len(*taskstatusslice); i++ {
		if (*taskstatusslice)[i].Status == 0 {
			return i
		}
	}
	return -1
}

// StartTenSecondTimeout ...
// This function checks if the worker
// has completed the job (map/reduce).
// For Map tasks it suffices to check the MapFileLocations slice,
// while for reduce tasks we must look for the filename mr-out-Y.
// If it has, mark job as completed;
// otherwise, mark job as idle again.
func StartTenSecondTimeout(i int, jobType string, m *Master) {
	time.Sleep(10 * time.Second)
	m.mu.Lock()
	defer m.mu.Unlock()
	if jobType == "map" {
		// check if mapfilelocations is empty or full
		if len(m.MapFileLocations[i]) > 0 { // ok good news, completed
			m.MapTasksStatus[i].Status = 2
		} else {
			m.MapTasksStatus[i].Status = 0
		}
		return
	}
	if jobType == "reduce" {
		fileNameToFind := "mr-out-" + fmt.Sprint(i)
		_, err := os.Stat(fileNameToFind)
		if os.IsNotExist(err) {
			m.ReduceTasksStatus[i].Status = 0
		} else {
			m.ReduceTasksStatus[i].Status = 2
		}
		return
	}
	// received unknown jobType
	log.Fatal("StartTimeSecondTimeout function received an unknown jobType")
}

// GrabAllFileNamesForReduceJobI ...
// For Reduce worker i, get all filenames in the i-th index
// of each []FileLocation in m.MapFileLocations
func GrabAllFileNamesForReduceJobI(i int, m *Master) []string {
	// NOTE you cannot lock here because that will give you a deadlock
	filenames := make([]string, 0)
	// Look for filenames with i int
	for _, a := range m.MapFileLocations {
		filenames = append(filenames, a[i])
	}

	return filenames
}

// AssignMapJobToWorker ...
// Assign idle map task to worker
// Send a reply with JobType "map",
// index i,
// R r, (number of reduce workers)
// and filelocation InputFileLocations[i]
// Then starts a new ten-second timeout thread
// that monitors whether the task has been complete
// or has been timed out.
func AssignMapJobToWorker(i int, reply *JobReply, m *Master) error {
	reply.JobType = "map" // "map", "reduce", "null", "exit"
	reply.Index = i
	m.mu.Lock()
	reply.R = m.R
	reply.InputFileLocation = m.InputFileLocations[i]
	m.MapTasksStatus[i].Status = 1 // in progress
	m.mu.Unlock()
	// Start a ten second listener
	go StartTenSecondTimeout(i, "map", m)
	return nil
}

// AssignReduceJobToWorker ...
// Assign idle reduce task to worker
// Send a reply with JobType "reduce",
// index i,
// N n, (number of map workers) (YAGNI tho!!)
// and locations of the files to reduce.
// Then starts a new ten-second timeout thread
// that monitors whether the task has been complete
// or has been timed out.
func AssignReduceJobToWorker(i int, reply *JobReply, m *Master) error {
	reply.JobType = "reduce" // "map", "reduce", "null", "exit"
	reply.Index = i
	m.mu.Lock()
	reply.N = m.N
	reply.MapFileLocations = GrabAllFileNamesForReduceJobI(i, m) // TODO -- pick all the -R out
	m.ReduceTasksStatus[i].Status = 1                            // in progress
	/*
		fmt.Printf("Current Reduce Task Status: [")
		for _, status := range m.ReduceTasksStatus {
			fmt.Printf("%v , ", status.Status)
		}
		fmt.Printf("]\n")
	*/
	m.mu.Unlock()
	// Start a ten second listener
	go StartTenSecondTimeout(i, "reduce", m)
	return nil
}

// AssignNullJobToWorker ...
// Assigns a "null" job to the worker.
// This is to tell the worker that there are no jobs
// available at this time. The worker should ask again later.
func AssignNullJobToWorker(reply *JobReply) error {
	reply.JobType = "null"
	return nil
}

// AssignExitJobToWorker ...
// Assigns an "exit" job to the worker.
// This lets the worker know that all jobs have been completed
// and it should now exit.
func AssignExitJobToWorker(reply *JobReply) error {
	reply.JobType = "exit"
	return nil
}

// ReceiveFileLocations ...
// Receives file locations and index from a successful map job
// It should update the master data structure appropriately.
// If it receives something it has already gotten, ignore it
// (or override it, whatever)
func (m *Master) ReceiveFileLocations(args *FileLocationArgs, reply *AckReply) error {
	m.mu.Lock()
	m.MapFileLocations[args.Index] = args.MapFileLocations
	m.MapTasksStatus[args.Index].Status = 2 // Complete
	/*
		fmt.Printf("File locations received from index %v\n", args.Index)
		fmt.Printf("[")
		for _, status := range m.MapTasksStatus {
			fmt.Printf("%v , ", status.Status)
		}
		fmt.Printf("]\n")
	*/
	reply.Received = true
	m.mu.Unlock()
	return nil
}

// GetNewTask ...
// RPC to be called by worker nodes.
func (m *Master) GetNewTask(args *NewJobArgs, reply *JobReply) error {
	// When I receive a new task request,

	// First check if all jobs have completed. If so, send an exit job.
	if m.bAllJobsComplete() {
		AssignExitJobToWorker(reply)
		return nil
	}

	// Otherwise, prioritise idle map tasks over reduce tasks
	indexOfFirstIdleMapTask := FindFirstIdleTask(&m.MapTasksStatus, m)
	indexOfFirstIdleReduceTask := FindFirstIdleTask(&m.ReduceTasksStatus, m)
	/*
		fmt.Printf("First Idle Map Task: %v . First Idle Reduce: %v\n",
			indexOfFirstIdleMapTask,
			indexOfFirstIdleReduceTask)
	*/

	// If there are no idle tasks, tell the worker to come again.
	if indexOfFirstIdleMapTask == -1 && indexOfFirstIdleReduceTask == -1 {
		AssignNullJobToWorker(reply)
		return nil
	}
	// If there are idle map tasks, give the worker a map task
	if indexOfFirstIdleMapTask != -1 {
		AssignMapJobToWorker(indexOfFirstIdleMapTask, reply, m)
		return nil
	}
	// If all map jobs are complete and there are idle reduce tasks,
	// give the worker a reduce task
	if m.bAllMapJobsComplete() && indexOfFirstIdleReduceTask != -1 {
		// println("Giving the worker a reduce task...")
		AssignReduceJobToWorker(indexOfFirstIdleReduceTask, reply, m)
		return nil
	}
	// Otherwise, give the worker a null task
	AssignNullJobToWorker(reply)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done ...
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Return true if we have R completed files
	// technically this returns true when all N map jobs and R reduce jobs have completed
	return m.bAllJobsComplete()
}

// MakeMaster ...
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.InputFileLocations = files
	m.N = uint(len(files))
	m.R = uint(nReduce)
	m.MapTasksStatus = make([]TaskStatus, m.N)
	m.MapFileLocations = make([][]string, len(files))
	m.ReduceTasksStatus = make([]TaskStatus, m.R)
	m.server()
	return &m
}
