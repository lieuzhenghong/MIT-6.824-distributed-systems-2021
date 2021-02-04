package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

// KeyValue ...
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker ...
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Periodically ask for jobs
	// using the CallGetNewTask RPC
	// If you receive a non-null job,
	// perform it.
	for true {
		reply := CallGetNewTask()
		if reply.JobType == "exit" {
			break
		}
		if reply.JobType == "map" {
			PerformMap(mapf, reply)
		}
		if reply.JobType == "reduce" {
			PerformReduce(reducef, reply)
		} else {
			// println("Received null job.")
		}
		time.Sleep(1 * time.Second)
	}
}

// PerformReduce ...
// Performs the Reduce operation.
func PerformReduce(reducef func(string, []string) string, reply JobReply) {
	var kva []KeyValue

	for _, filename := range reply.MapFileLocations {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// Use a Map to combine all similar keys together

	// kvmap: a dictionary of Dict[Key, List[Value]]
	var kvmap map[string][]string
	kvmap = make(map[string][]string)

	for _, kv := range kva {
		kvmap[kv.Key] = append(kvmap[kv.Key], kv.Value)
	}

	// Now call Reduce on each distinct key in kva[]
	// and print the result to mr-out-R
	// We ensure nobody observes partially written files
	// in the event of a crash by using a temporary file
	// and then atomically renaming it.
	oname := "mr-out-" + fmt.Sprint(reply.Index)
	ofile, _ := ioutil.TempFile(".", "")

	for key, values := range kvmap {
		output := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	// This rename is guaranteed atomic
	os.Rename(ofile.Name(), oname)
	ofile.Close()
}

// PerformMap ...
// Performs the Map operation.
func PerformMap(mapf func(string, string) []KeyValue, reply JobReply) {
	// First, read in the file from fileLocation
	filename := reply.InputFileLocation
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// Next, run the map function.
	// kva is a []KeyValue
	kva := mapf(filename, string(content))
	fileData := make([][]KeyValue, reply.R)

	// Now we want to partition the keys in kva to R partitions by using ihash(key)
	for i := 0; i < len(kva); i++ {
		r := ihash(kva[i].Key) % int(reply.R)
		fileData[r] = append(fileData[r], kva[i])
	}

	var intermediateFileLocations []string

	// TODO save the R partitions to JSON as mr-X-Y on local disk
	for r := 0; r < len(fileData); r++ {
		oname := "mr-" + fmt.Sprint(reply.Index) + "-" + fmt.Sprint(r)
		// We ensure nobody observes partially written files
		// in the event of a crash by using a temporary file
		// and then atomically renaming it.
		ofile, _ := ioutil.TempFile(".", "")
		enc := json.NewEncoder(ofile)
		for _, kv := range fileData[r] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode json %v", kv.Key)
			}
		}
		// This rename is guaranteed atomic
		os.Rename(ofile.Name(), oname)
		ofile.Close()
		intermediateFileLocations = append(intermediateFileLocations, oname)

	}

	// Call the CallReturnFileLocations RPC
	// to tell the master where you've saved the files

	CallReturnFileLocations(reply.Index, intermediateFileLocations)
}

// CallGetNewTask ...
// Remote RPC call to the master to ask the master for a new job.
func CallGetNewTask() JobReply {
	args := ExampleArgs{}
	reply := JobReply{}
	call("Master.GetNewTask", &args, &reply)
	return reply
}

// CallReturnFileLocations ...
// Remote RPC call to the master to tell the master
// where the intermediate files built by the map operation
// are saved.
func CallReturnFileLocations(index int, fileLocations []string) {
	args := FileLocationArgs{}
	args.Index = index
	args.MapFileLocations = fileLocations
	reply := AckReply{}
	call("Master.ReceiveFileLocations", &args, &reply)
}

// CallExample ...
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply typNs are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
