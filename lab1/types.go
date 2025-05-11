package lab1

import (
	"time"
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
)

// TaskState represents the current state of a task
type TaskState int

const (
	TaskIdle       TaskState = iota // Task hasn't been assigned yet
	TaskInProgress                  // Task is currently being processed by a worker
	TaskCompleted                   // Task has been successfully completed
	TaskFailed                      // Task failed and needs to be reassigned
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
	TaskId     int
	State      TaskState
	StartTime  time.Time
	WorkerId   int // ID of worker currently processing this task
	Completed  bool
	Filename   string
	RetryCount int // Number of times this task has been retried
	MaxRetries int // Maximum number of retries allowed
}

// RequestTaskArgs request task arguments
type RequestTaskArgs struct {
	WorkerId int // Worker identifier for tracking
}

// RequestTaskReply The response to a request task.
type RequestTaskReply struct {
	Task     Task
	WorkerId int // Assigned worker ID if this is a new worker
}

// ReportTaskArgs Parameters for reporting task completion
type ReportTaskArgs struct {
	TaskId   int
	WorkerId int
	Success  bool
}

// ReportTaskReply reports the task completion response
type ReportTaskReply struct {
}

// HeartbeatArgs for worker to report they're still alive
type HeartbeatArgs struct {
	WorkerId int
}

// HeartbeatReply response to heartbeat
type HeartbeatReply struct {
}
