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

const (
	NOT_STARTED = "NOT_STARTED"
	PROCESSING  = "PROCESSING"
	DONE        = "DONE"
)

type File struct {
	filename string
	status   string
}

type Coordinator struct {
	// Your definitions here.
	MapFiles              []File
	MapFilesLock          sync.Mutex
	nReduce               int
	ReduceTasksStatus     []string
	ReduceTasksStatusLock sync.Mutex

	AllDone bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.MapFilesLock.Lock()

	// map stage
	for i := range c.MapFiles {
		if c.MapFiles[i].status == NOT_STARTED {
			reply.Filename = c.MapFiles[i].filename
			reply.IsMap = true
			reply.NReduce = c.nReduce
			c.MapFiles[i].status = PROCESSING

			fmt.Printf("Starting Proccessing Map Task i: %d\n", i)
			go c.returnMapTaskToNotStarted(i)

			c.MapFilesLock.Unlock()
			return nil
		}
	}
	// if no file found we need to check if all the map tasks are done before starting the reduce stage
	allDone := true

	for i := range c.MapFiles {
		if c.MapFiles[i].status != DONE {
			allDone = false
			break
		}
	}

	c.MapFilesLock.Unlock()

	if !allDone {
		reply.DoNothing = true
		return nil
	}

	c.ReduceTasksStatusLock.Lock()

	for i := range c.ReduceTasksStatus {
		if c.ReduceTasksStatus[i] == NOT_STARTED {
			reply.ReduceIndex = i
			reply.IsReduce = true
			c.ReduceTasksStatus[i] = PROCESSING

			fmt.Printf("Starting Proccessing Reduce Task i: %d\n", i)
			go c.returnReduceTaskToNotStarted(i)

			c.ReduceTasksStatusLock.Unlock()
			return nil
		}
	}

	allDone = true
	for i := range c.ReduceTasksStatus {
		if c.ReduceTasksStatus[i] != DONE {
			allDone = false
			break
		}
	}

	if allDone {
		c.AllDone = true
	}

	c.ReduceTasksStatusLock.Unlock()

	return nil
}

func (c *Coordinator) MarkTaskAsDone(args *MarkTaskAsDoneArgs, reply *MarkTaskAsDoneReply) error {
	if args.IsMap {
		for i := range c.MapFiles {
			if c.MapFiles[i].filename == args.Filename {
				c.MapFilesLock.Lock()
				fmt.Printf("Map Task %s is done\n", c.MapFiles[i].filename)
				c.MapFiles[i].status = DONE
				c.MapFilesLock.Unlock()
			}
		}
	}

	if args.IsReduce {
		for i := range c.ReduceTasksStatus {
			if i == args.ReduceIndex {
				c.ReduceTasksStatusLock.Lock()
				fmt.Printf("Reduce Task %d is done\n", i)
				c.ReduceTasksStatus[i] = DONE
				c.ReduceTasksStatusLock.Unlock()
			}
		}
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.AllDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapFiles:          make([]File, len(files)),
		nReduce:           nReduce,
		ReduceTasksStatus: make([]string, nReduce),
		AllDone:           false,
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasksStatus[i] = NOT_STARTED
	}

	for i := range files {
		c.MapFiles[i] = File{
			filename: files[i],
			status:   NOT_STARTED,
		}
	}

	c.server()
	return &c
}

func (c *Coordinator) returnMapTaskToNotStarted(index int) {
	time.Sleep(time.Second * 10)

	c.MapFilesLock.Lock()

	if c.MapFiles[index].status == PROCESSING {
		fmt.Printf("Map Task %d is returning to not started\n", index)
		c.MapFiles[index].status = NOT_STARTED
	}
	c.MapFilesLock.Unlock()
}

func (c *Coordinator) returnReduceTaskToNotStarted(index int) {
	time.Sleep(time.Second * 10)

	c.ReduceTasksStatusLock.Lock()

	if c.ReduceTasksStatus[index] == PROCESSING {
		fmt.Printf("Reduce Task %d is returning to not started\n", index)
		c.ReduceTasksStatus[index] = NOT_STARTED
	}
	c.ReduceTasksStatusLock.Unlock()
}
