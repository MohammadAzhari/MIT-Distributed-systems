package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// uncomment to send the Example RPC to the coordinator.
		reply := CallGetTask()

		fmt.Printf("reply %+v\n", reply)

		if reply.DoNothing {
			time.Sleep(time.Second)
			continue
		}

		if reply.IsMap {
			_, err := handleMap(mapf, reply.Filename, reply.NReduce)
			if err != nil {
				fmt.Printf("Error while handle map, e: %s", err.Error())
			}

			CallMarkTaskAsDone(MarkTaskAsDoneArgs{
				IsMap:    true,
				Filename: reply.Filename,
			})
		}

		if reply.IsReduce {
			err := handleReduce(reducef, reply.ReduceIndex)
			if err != nil {
				fmt.Printf("Error while handle reduce, e: %s", err.Error())
			}

			CallMarkTaskAsDone(MarkTaskAsDoneArgs{
				IsReduce:    true,
				ReduceIndex: reply.ReduceIndex,
			})
		}

		time.Sleep(time.Second)
	}

}

// example function to show how to make an RPC call to the coordinator.
func CallGetTask() GetTaskReply {

	// declare an argument structure.
	args := GetTaskArgs{}

	// declare a reply structure.
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		log.Fatal("coordinator is disconnected")
	}
	return reply
}

func CallMarkTaskAsDone(args MarkTaskAsDoneArgs) (reply MarkTaskAsDoneReply) {

	ok := call("Coordinator.MarkTaskAsDone", &args, &reply)

	if !ok {
		log.Fatal("coordinator is disconnected")
	}
	return reply
}

// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

func handleMap(mapf func(string, string) []KeyValue, filename string, nReduce int) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	file.Close()
	intermediate := mapf(filename, string(content))

	files := make([]*os.File, nReduce)
	filesEnc := make([]*json.Encoder, nReduce)

	rand.Seed(time.Now().UnixNano())
	randomNum := rand.Intn(10000000)

	// create nReduce Files
	for i := 0; i < nReduce; i++ {
		file, err := os.Create(fmt.Sprintf("mr-%d-%d", i, randomNum))
		if err != nil {
			return nil, err
		}
		files[i] = file
		filesEnc[i] = json.NewEncoder(file)
	}

	for _, kv := range intermediate {
		reduceTaskNumber := ihash(kv.Key) % nReduce
		enc := filesEnc[reduceTaskNumber]
		enc.Encode(&kv)
	}

	filenames := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		filenames[i] = files[i].Name()
		files[i].Close()
	}

	return filenames, nil
}

func handleReduce(reducef func(string, []string) string, reduceIndex int) error {
	// get all the files starts with mr-index-*
	dirFiles, err := os.ReadDir(".")

	if err != nil {
		return err
	}

	filesToReduce := []string{}

	for _, dirFile := range dirFiles {
		if strings.HasPrefix(dirFile.Name(), fmt.Sprintf("mr-%d-", reduceIndex)) {
			filesToReduce = append(filesToReduce, dirFile.Name())
		}
	}

	// read all the files and pass them to reduce func
	intermediate := []KeyValue{}

	for _, filename := range filesToReduce {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue

			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	// write the output to the mr-out-* file
	ofile, _ := os.Create(fmt.Sprintf("mr-out-%d", reduceIndex))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	return nil
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
