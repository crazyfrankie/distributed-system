package lab1

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
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

// Master controller node
type Master struct {
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

// NewMaster creates a master node.
func NewMaster(files []string, nReduce int) *Master {
	c := &Master{
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

func defaultMap(filename string, contents string) []KeyValue {
	words := strings.Fields(contents)
	var kva []KeyValue
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

func defaultReduce(key string, values []string) string {
	return fmt.Sprintf("%d", len(values))
}

// RequestTask RPC handler, handles the worker request task.
func (m *Master) RequestTask(reply *RequestTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.phase == MapTask {
		for taskId, status := range m.mapTasks {
			if !status.Completed && time.Since(status.StartTime) > 10*time.Second {
				status.StartTime = time.Now()
				reply.Task = Task{
					Type:     MapTask,
					TaskId:   taskId,
					Filename: status.Filename,
					NReduce:  m.nReduce,
					NMap:     m.nMap,
				}
				return nil
			}
		}
	} else if m.phase == ReduceTask {
		for taskId, status := range m.reduceTasks {
			if !status.Completed && time.Since(status.StartTime) > 10*time.Second {
				status.StartTime = time.Now()
				reply.Task = Task{
					Type:    ReduceTask,
					TaskId:  taskId,
					NReduce: m.nReduce,
					NMap:    m.nMap,
				}
				return nil
			}
		}
	}

	reply.Task = Task{Type: NoTask}
	return nil
}

// ReportTask RPC handler, handles the worker reporting the completion of the task.
func (m *Master) ReportTask(args *ReportTaskArgs) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.Success {
		if m.phase == MapTask {
			if status, ok := m.mapTasks[args.TaskId]; ok && !status.Completed {
				status.Completed = true
				m.completedMaps++
				if m.completedMaps == m.nMap {
					m.phase = ReduceTask
					for i := 0; i < m.nReduce; i++ {
						m.reduceTasks[i] = &TaskStatus{
							TaskId:    i,
							StartTime: time.Now(),
							Completed: false,
						}
					}
				}
			}
		} else if m.phase == ReduceTask {
			if status, ok := m.reduceTasks[args.TaskId]; ok && !status.Completed {
				status.Completed = true
				m.completedReduces++
				if m.completedReduces == m.nReduce {
					m.phase = NoTask
				}
			}
		}
	}
	return nil
}

// serve Starts the RPC server
func (m *Master) serve() {
	rpc.Register(m)

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

	m.listener = l

	m.httpServer = &http.Server{
		Handler: mux,
	}

	go m.httpServer.Serve(l)
}

// Done Checks that all tasks are completed
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.phase == NoTask
}

// cleanup cleaning up the RPC server and related resources
func (m *Master) cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.listener != nil {
		m.listener.Close()
	}

	if m.httpServer != nil {
		m.httpServer.Close()
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
