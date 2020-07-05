package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	INTERPREFIX = "inter_file_"
	TIMEOUT     = 10
)

type MapTask struct {
	filename string //file name
	index    int
	state    int // task state 0 - waitting; 1 - running; 2 - finished
	emittime int64
}

type ReduceTask struct {
	interfile string //intermediate file name
	index     int    //the index of tast
	state     int    //task state 0 - waitting; 1 - running; 2 - finished
	emittime  int64
}

type Master struct {
	mu             sync.Mutex
	mapTasks       []MapTask //each file as a task
	nextmaptask    int
	reduceTasks    []ReduceTask //each intermediate file as a task
	nextreducetask int
	nReduce        int  //itermidiate keys
	done           bool //mr is done
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) TaskHandler(args *TaskArgs, reply *TaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.haveDone() {
		reply.Tasktype = DONE
	} else if !m.mapfinished() {
		if (m.nextmaptask >= len(m.mapTasks)) || (m.mapTasks[m.nextmaptask].state != 0) {
			reply.Tasktype = WAIT
		} else {
			reply.Tasktype = MAP
			reply.Filename = m.mapTasks[m.nextmaptask].filename
			reply.NnReduce = m.nReduce
			reply.Taskindex = m.nextmaptask
			m.mapTasks[m.nextmaptask].state = 1
			m.mapTasks[m.nextmaptask].emittime = time.Now().Unix()
		}
	} else {
		if m.nextreducetask >= len(m.reduceTasks) || m.reduceTasks[m.nextreducetask].state != 0 {
			reply.Tasktype = WAIT
		} else {
			reply.Tasktype = REDUCE
			reply.Filename = m.reduceTasks[m.nextreducetask].interfile
			reply.Taskindex = m.nextreducetask
			reply.MapTaskTot = len(m.mapTasks)
			m.reduceTasks[m.nextreducetask].state = 1
			m.reduceTasks[m.nextreducetask].emittime = time.Now().Unix()
		}
	}
	return nil
}

func (m *Master) FinishHandler(args *FinishArgs, reply *FinishReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.Tasktype == MAP {
		m.mapTasks[args.Taskindex].state = 2
	}

	if args.Tasktype == REDUCE {
		m.reduceTasks[args.Taskindex].state = 2
	}
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

/// should hold lock before enter this func
func (m *Master) mapfinished() bool {
	t := time.Now().Unix()
	ret := true
	j := 0
	for j < len(m.mapTasks) {
		if m.mapTasks[j].state == 1 {
			if t-m.mapTasks[j].emittime >= TIMEOUT {
				m.mapTasks[j].state = 0
			}
		}
		j++
	}
	i := 0
	for i < len(m.mapTasks) {
		if m.mapTasks[i].state == 0 {
			m.nextmaptask = i
			break
		}
		i++
	}
	for _, mapTask := range m.mapTasks {
		if mapTask.state != 2 {
			ret = false
			break
		}
	}
	return ret
}

/// should hold lock before enter this func
func (m *Master) haveDone() bool {
	ret := true
	t := time.Now().Unix()
	j := 0
	for j < len(m.reduceTasks) {
		if m.reduceTasks[j].state == 1 {
			if t-m.reduceTasks[j].emittime >= TIMEOUT {
				m.reduceTasks[j].state = 0
			}
		}
		j++
	}
	i := 0
	for _, reduceTask := range m.reduceTasks {
		if reduceTask.state == 0 {
			m.nextreducetask = i
			break
		}
		i++
	}
	for _, reduceTask := range m.reduceTasks {
		if reduceTask.state != 2 {
			ret = false
			break
		}
	}
	if ret {
		m.done = true
	}
	return ret
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	m.mu.Lock()
	defer m.mu.Unlock()
	ret = m.haveDone()
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	//record each file as a task
	mapTasks := []MapTask{}
	j := 0
	for _, filename := range files {
		mapTasks = append(mapTasks, MapTask{filename, j, 0, 0})
		j++
	}
	m.mapTasks = mapTasks
	m.nextmaptask = 0
	//generate nReduce reduce tasks each in an intermediate file
	reduceTasks := []ReduceTask{}
	i := 0
	for i < nReduce {
		reduceTasks = append(reduceTasks, ReduceTask{INTERPREFIX + strconv.Itoa(i), i, 0, 0})
		i++
	}
	m.reduceTasks = reduceTasks
	m.nextreducetask = 0
	m.nReduce = nReduce
	m.done = false

	m.server()
	return &m
}
