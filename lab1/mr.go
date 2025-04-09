package lab1

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

var (
	socketMutex       sync.Mutex
	coordinatorSocket string
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
)

type Task struct {
	Type      TaskType
	TaskId    int
	Filename  string
	NReduce   int
	NMap      int
	Completed bool
}

type TaskStatus struct {
	TaskId    int
	StartTime time.Time
	Completed bool
	Filename  string
}

// Coordinator master node
type Coordinator struct {
	mu               sync.Mutex
	mapTasks         map[int]*TaskStatus
	reduceTasks      map[int]*TaskStatus
	nMap             int
	nReduce          int
	phase            TaskType
	completedMaps    int
	completedReduces int
	listener         net.Listener
	httpServer       *http.Server
}

// RequestTaskArgs request task arguments
type RequestTaskArgs struct {
}

// RequestTaskReply The response to a request task.
type RequestTaskReply struct {
	Task Task
}

// ReportTaskArgs Parameters for reporting task completion
type ReportTaskArgs struct {
	TaskId  int
	Success bool
}

// ReportTaskReply reports the task completion response
type ReportTaskReply struct {
}

// MakeCoordinator creates a master node.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		mapTasks:    make(map[int]*TaskStatus),
		reduceTasks: make(map[int]*TaskStatus),
		nMap:        len(files),
		nReduce:     nReduce,
		phase:       MapTask,
	}

	for i, file := range files {
		c.mapTasks[i] = &TaskStatus{
			TaskId:    i,
			StartTime: time.Now(),
			Completed: false,
			Filename:  file,
		}
	}

	c.serve()
	return c
}

// RequestTask RPC handler, handles the worker request task.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == MapTask {
		for taskId, status := range c.mapTasks {
			if !status.Completed && time.Since(status.StartTime) > 10*time.Second {
				status.StartTime = time.Now()
				reply.Task = Task{
					Type:     MapTask,
					TaskId:   taskId,
					Filename: status.Filename,
					NReduce:  c.nReduce,
					NMap:     c.nMap,
				}
				return nil
			}
		}
	} else if c.phase == ReduceTask {
		for taskId, status := range c.reduceTasks {
			if !status.Completed && time.Since(status.StartTime) > 10*time.Second {
				status.StartTime = time.Now()
				reply.Task = Task{
					Type:    ReduceTask,
					TaskId:  taskId,
					NReduce: c.nReduce,
					NMap:    c.nMap,
				}
				return nil
			}
		}
	}

	reply.Task = Task{Type: NoTask}
	return nil
}

// ReportTask RPC handler, handles the worker reporting the completion of the task.
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Success {
		if c.phase == MapTask {
			if status, ok := c.mapTasks[args.TaskId]; ok && !status.Completed {
				status.Completed = true
				c.completedMaps++
				if c.completedMaps == c.nMap {
					c.phase = ReduceTask
					for i := 0; i < c.nReduce; i++ {
						c.reduceTasks[i] = &TaskStatus{
							TaskId:    i,
							StartTime: time.Now(),
							Completed: false,
						}
					}
				}
			}
		} else if c.phase == ReduceTask {
			if status, ok := c.reduceTasks[args.TaskId]; ok && !status.Completed {
				status.Completed = true
				c.completedReduces++
				if c.completedReduces == c.nReduce {
					c.phase = NoTask
				}
			}
		}
	}
	return nil
}

// serve Starts the RPC server
func (c *Coordinator) serve() {
	rpc.Register(c)

	mux := http.NewServeMux()
	mux.Handle("/_goRPC_", rpc.DefaultServer)

	sockname := coordinatorSock()

	if err := os.RemoveAll(sockname); err != nil {
		log.Printf("Remove socket file error: %v", err)
	}

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	c.listener = l

	c.httpServer = &http.Server{
		Handler: mux,
	}

	go c.httpServer.Serve(l)
}

// Done Checks that all tasks are completed
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == NoTask
}

// cleanup cleaning up the RPC server and related resources
func (c *Coordinator) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.listener != nil {
		c.listener.Close()
	}

	if c.httpServer != nil {
		c.httpServer.Close()
	}

	// Clean up the socket file
	os.RemoveAll(coordinatorSock())
}

// coordinatorSock returns coordinator's socket file name
func coordinatorSock() string {
	socketMutex.Lock()
	defer socketMutex.Unlock()

	if coordinatorSocket == "" {
		timestamp := fmt.Sprintf("%d", time.Now().UnixNano())
		coordinatorSocket = fmt.Sprintf("/tmp/824-mr-%d-%s", os.Getuid(), timestamp)
	}
	return coordinatorSocket
}
