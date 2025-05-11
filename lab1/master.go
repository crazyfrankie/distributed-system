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
	socketMutex  sync.Mutex
	masterSocket string
)

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
	nextWorkerId     int               // For assigning unique worker IDs
	workers          map[int]time.Time // Track last heartbeat from each worker
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

// NewMaster creates a master node.
func NewMaster(files []string, nReduce int) *Master {
	c := &Master{
		mapTasks:    make(map[int]*TaskStatus),
		reduceTasks: make(map[int]*TaskStatus),
		nMap:        len(files),
		nReduce:     nReduce,
		phase:       MapTask,
		workers:     make(map[int]time.Time),
	}

	for i, file := range files {
		c.mapTasks[i] = &TaskStatus{
			TaskId:     i,
			State:      TaskIdle,
			StartTime:  time.Time{},
			Completed:  false,
			Filename:   file,
			RetryCount: 0,
			MaxRetries: 3, // Allow up to 3 retries
		}
	}

	// Start a goroutine to check for worker timeouts
	go c.checkTimeouts()

	c.serve()
	return c
}

// Heartbeat handles worker heartbeats to detect failures
func (m *Master) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workers[args.WorkerId] = time.Now()
	return nil
}

// RequestTask RPC handler, handles the worker request task.
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Handle new worker registration
	if args.WorkerId == -1 {
		// Assign new worker ID
		newWorkerId := m.nextWorkerId
		m.nextWorkerId++
		m.workers[newWorkerId] = time.Now()
		reply.WorkerId = newWorkerId
		log.Printf("New worker registered with ID: %d", newWorkerId)
	} else {
		// Update existing worker's last seen time
		m.workers[args.WorkerId] = time.Now()
	}

	if m.phase == MapTask {
		for taskId, status := range m.mapTasks {
			if status.State == TaskIdle || (status.State == TaskFailed && status.RetryCount < status.MaxRetries) {
				status.State = TaskInProgress
				status.StartTime = time.Now()
				status.WorkerId = args.WorkerId

				reply.Task = Task{
					Type:     MapTask,
					TaskId:   taskId,
					Filename: status.Filename,
					NReduce:  m.nReduce,
					NMap:     m.nMap,
				}
				log.Printf("Assigned map task %d to worker %d", taskId, args.WorkerId)
				return nil
			}
		}
	} else if m.phase == ReduceTask {
		for taskId, status := range m.reduceTasks {
			if status.State == TaskIdle || (status.State == TaskFailed && status.RetryCount < status.MaxRetries) {
				status.State = TaskInProgress
				status.StartTime = time.Now()
				status.WorkerId = args.WorkerId

				reply.Task = Task{
					Type:    ReduceTask,
					TaskId:  taskId,
					NReduce: m.nReduce,
					NMap:    m.nMap,
				}
				log.Printf("Assigned reduce task %d to worker %d", taskId, args.WorkerId)
				return nil
			}
		}
	}

	reply.Task = Task{Type: NoTask}
	return nil
}

// ReportTask RPC handler, handles the worker reporting the completion of the task.
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.Success {
		if m.phase == MapTask {
			if status, ok := m.mapTasks[args.TaskId]; ok && status.State == TaskInProgress {
				status.State = TaskCompleted
				status.Completed = true
				m.completedMaps++
				if m.completedMaps == m.nMap {
					m.phase = ReduceTask
					for i := 0; i < m.nReduce; i++ {
						m.reduceTasks[i] = &TaskStatus{
							TaskId:     i,
							State:      TaskIdle,
							StartTime:  time.Time{},
							Completed:  false,
							RetryCount: 0,
							MaxRetries: 3,
						}
					}
				}
			}
		} else if m.phase == ReduceTask {
			if status, ok := m.reduceTasks[args.TaskId]; ok && status.State == TaskInProgress {
				status.State = TaskCompleted
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

// checkTimeouts periodically checks for worker timeouts and reassigns tasks
func (m *Master) checkTimeouts() {
	for {
		time.Sleep(3 * time.Second) // Check every 3 seconds

		if m.Done() {
			return
		}

		m.mu.Lock()

		// Check for worker timeouts
		for workerId, lastSeen := range m.workers {
			if time.Since(lastSeen) > 10*time.Second {
				log.Printf("Worker %d appears to have died, reassigning its tasks", workerId)
				// Reassign all tasks from this worker
				m.reassignTasksFromWorker(workerId)
				delete(m.workers, workerId)
			}
		}

		// Check for task timeouts
		now := time.Now()

		if m.phase == MapTask {
			for _, task := range m.mapTasks {
				if task.State == TaskInProgress && now.Sub(task.StartTime) > 10*time.Second {
					log.Printf("Map task %d timed out, marking for reassignment", task.TaskId)
					task.State = TaskFailed
					task.RetryCount++
				}
			}
		} else if m.phase == ReduceTask {
			for _, task := range m.reduceTasks {
				if task.State == TaskInProgress && now.Sub(task.StartTime) > 10*time.Second {
					log.Printf("Reduce task %d timed out, marking for reassignment", task.TaskId)
					task.State = TaskFailed
					task.RetryCount++
				}
			}
		}

		m.mu.Unlock()
	}
}

// reassignTasksFromWorker marks all tasks assigned to a worker as failed
func (m *Master) reassignTasksFromWorker(workerId int) {
	if m.phase == MapTask {
		for _, task := range m.mapTasks {
			if task.State == TaskInProgress && task.WorkerId == workerId {
				task.State = TaskFailed
				task.RetryCount++
			}
		}
	} else if m.phase == ReduceTask {
		for _, task := range m.reduceTasks {
			if task.State == TaskInProgress && task.WorkerId == workerId {
				task.State = TaskFailed
				task.RetryCount++
			}
		}
	}
}

// serve Starts the RPC server
func (m *Master) serve() {
	rpc.Register(m)

	mux := http.NewServeMux()
	mux.Handle("/_goRPC_", rpc.DefaultServer)

	sockname := masterSock()

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
	os.RemoveAll(masterSock())
}

// masterSock returns coordinator's socket file name
func masterSock() string {
	socketMutex.Lock()
	defer socketMutex.Unlock()

	if masterSocket == "" {
		timestamp := fmt.Sprintf("%d", time.Now().UnixNano())
		masterSocket = fmt.Sprintf("/tmp/824-mr-%d-%s", os.Getuid(), timestamp)
	}
	return masterSocket
}
