package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

const (
	MAP       = "map"
	REDUCE    = "reduce"
	WAIT      = "wait"
	DONE      = "done"
	OUTPREFIX = "mr-out-"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := TaskArgs{"task"}
		reply := TaskReply{}
		if call("Master.TaskHandler", &args, &reply) {
			if reply.Tasktype == MAP {
				if doMap(reply.Filename, reply.NnReduce, reply.Taskindex, mapf) {
					sargs := FinishArgs{
						MAP,
						reply.Filename,
						reply.Taskindex}
					sreply := FinishReply{}
					call("Master.FinishHandler", &sargs, &sreply)
				}
			}
			if reply.Tasktype == REDUCE {
				if doReduce(reply.Filename, reply.Taskindex, reply.MapTaskTot, reducef) {
					sargs := FinishArgs{
						REDUCE,
						reply.Filename,
						reply.Taskindex}
					sreply := FinishReply{}
					call("Master.FinishHandler", &sargs, &sreply)
				}
			}
			if reply.Tasktype == DONE {
				break
			}
			if reply.Tasktype == WAIT {
				time.Sleep(time.Second)
			}
		}

	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func doMap(filename string, nReduce int, taskindex int, mapf func(string, string) []KeyValue) bool {
	ret := false
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return ret
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return ret
	}
	file.Close()
	kva := mapf(filename, string(content))

	switchMap := make([][]KeyValue, nReduce)
	if kva != nil {
		for _, kv := range kva {
			hashIndex := ihash(kv.Key) % nReduce
			switchMap[hashIndex] = append(switchMap[hashIndex], kv)
		}

		j := 0
		for j < nReduce {
			oname := INTERPREFIX + strconv.Itoa(j) + "-" + strconv.Itoa(taskindex)
			ofile, err := os.Create(oname)
			if err != nil {
				log.Fatalf("cannot creat %v\n", oname)
				return ret
			}
			enCoder := json.NewEncoder(ofile)
			for _, kv := range switchMap[j] {
				enCoder.Encode(&kv)
			}
			defer ofile.Close()
			j++
		}

		ret = true
	}

	return ret
}

// func doReduce(filename string, index int, mapTaskTot int, reducef func(string, []string) string) bool {
// 	ret := false
// 	intermediate := []KeyValue{}
// 	k := 0
// 	for k < mapTaskTot {
// 		intName := filename + "-" + strconv.Itoa(k)
// 		ifile, err := os.Open(intName)
// 		if err != nil {
// 			log.Fatalf("cannot open %v\n", intName)
// 			return ret
// 		}
// 		dec := json.NewDecoder(ifile)
// 		for {
// 			var kv KeyValue
// 			if err := dec.Decode(&kv); err != nil {
// 				break
// 			}
// 			intermediate = append(intermediate, kv)
// 		}
// 		ifile.Close()
// 		k++
// 	}
// 	sort.Sort(ByKey(intermediate))

// 	oname := OUTPREFIX + strconv.Itoa(index)
// 	ofile, err := os.Create(oname)
// 	if err != nil {
// 		log.Fatalf("cannot create %v\n", oname)
// 		return ret
// 	}
// 	defer ofile.Close()

// 	i := 0
// 	for i < len(intermediate) {
// 		j := i + 1
// 		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
// 			j++
// 		}
// 		values := []string{}
// 		for k := i; k < j; k++ {
// 			values = append(values, intermediate[k].Value)
// 		}
// 		output := reducef(intermediate[i].Key, values)

// 		// this is the correct format for each line of Reduce output.
// 		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

// 		i = j
// 	}
// 	ret = true
// 	return ret
// }
func doReduce(filename string, index int, mapTaskTot int, reducef func(string, []string) string) bool {
	ret := false
	maps := make(map[string][]string)
	k := 0
	for k < mapTaskTot {
		intName := filename + "-" + strconv.Itoa(k)
		ifile, err := os.Open(intName)
		if err != nil {
			log.Fatalf("cannot open %v\n", intName)
			return ret
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
		ifile.Close()
		k++
	}

	oname := OUTPREFIX + strconv.Itoa(index)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v\n", oname)
		return ret
	}
	defer ofile.Close()

	for k, v := range maps {
		output := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}

	ret = true
	return ret
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
