package lab1

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type Worker struct {
	id              int
	mapFunc         MapFunc
	reduceFunc      ReduceFunc
	lastTask        Task
	maxRetries      int
	heartbeatPeriod time.Duration
	stopHeartbeat   chan struct{}
}

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string

// StartWorker starts the worker process.
func StartWorker(opts ...Option) {
	opt := defaultOption()
	for _, o := range opts {
		o(opt)
	}

	w := &Worker{
		mapFunc:         opt.mapFunc,
		reduceFunc:      opt.reduceFunc,
		maxRetries:      3,
		heartbeatPeriod: 3 * time.Second,
		stopHeartbeat:   make(chan struct{}),
	}

	// Get worker ID from master
	w.register()

	// Start heartbeat goroutine
	go w.sendHeartbeats()

	defer func() {
		// Signal heartbeat goroutine to stop
		close(w.stopHeartbeat)
	}()

	w.run()
}

// register registers this worker with the master
// and obtains the corresponding unique ID
func (w *Worker) register() {
	args := RequestTaskArgs{WorkerId: -1} // -1 indicates new worker
	reply := RequestTaskReply{}

	// Keep retrying until we successfully connect
	for {
		ok := call("Master.RequestTask", &args, &reply)
		if ok {
			w.id = reply.WorkerId
			log.Printf("Worker registered with master, assigned ID: %d", w.id)
			return
		}
		log.Printf("Failed to register with master, retrying...")
		time.Sleep(time.Second)
	}
}

// sendHeartbeats periodically sends heartbeats to let the master know this worker is alive
func (w *Worker) sendHeartbeats() {
	for {
		select {
		case <-w.stopHeartbeat:
			return
		case <-time.After(w.heartbeatPeriod):
			args := HeartbeatArgs{WorkerId: w.id}
			reply := HeartbeatReply{}

			ok := call("Master.Heartbeat", &args, &reply)
			if !ok {
				log.Printf("Failed to send heartbeat to master")
			}
		}
	}
}

// run worker's main loop
func (w *Worker) run() {
	for {
		// request task
		task, success := w.requestTask()
		if !success {
			log.Printf("Failed to request task, retrying in 1 second...")
			time.Sleep(time.Second)
			continue
		}

		if task.Type == NoTask {
			time.Sleep(time.Second)
			continue
		}

		w.lastTask = task

		// execute task
		var taskSuccess bool
		if task.Type == MapTask {
			taskSuccess = w.Map(task)
		} else if task.Type == ReduceTask {
			taskSuccess = w.Reduce(task)
		}

		// reporting on mandate completion
		reportSuccess := w.reportTask(task.TaskId, taskSuccess)
		if !reportSuccess {
			log.Printf("Failed to report task completion, will retry in next loop")
		}

		// Brief pause to prevent hammering the master
		time.Sleep(100 * time.Millisecond)
	}
}

// requestTask request a task from the master
func (w *Worker) requestTask() (Task, bool) {
	args := RequestTaskArgs{WorkerId: w.id}
	reply := RequestTaskReply{}

	ok := call("Master.RequestTask", &args, &reply)
	if !ok {
		return Task{}, false
	}

	return reply.Task, true
}

// reportTask reports the completion of the task to the master.
func (w *Worker) reportTask(taskId int, success bool) bool {
	args := ReportTaskArgs{
		TaskId:   taskId,
		WorkerId: w.id,
		Success:  success,
	}
	reply := ReportTaskReply{}

	ok := call("Master.ReportTask", &args, &reply)
	return ok
}

// Map performs the map task
func (w *Worker) Map(task Task) bool {
	// read the input file
	content, err := os.ReadFile(task.Filename)
	if err != nil {
		log.Printf("Cannot read file %v", task.Filename)
		return false
	}

	// execute the map
	kva := w.mapFunc(task.Filename, string(content))

	// create intermediate files
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		reduceTask := ihash(kv.Key) % task.NReduce
		intermediate[reduceTask] = append(intermediate[reduceTask], kv)
	}

	// Use a more robust naming scheme for intermediate files that includes worker ID
	// This helps prevent different workers from overwriting each other's files
	tempFiles := make([]*os.File, task.NReduce)
	tempFilenames := make([]string, task.NReduce)

	for i := 0; i < task.NReduce; i++ {
		// First create temporary files
		tempFile, err := os.CreateTemp("", "map-")
		if err != nil {
			log.Printf("Cannot create temp file: %v", err)
			// Clean up any created files
			for j := 0; j < i; j++ {
				tempFiles[j].Close()
				os.Remove(tempFilenames[j])
			}
			return false
		}
		tempFiles[i] = tempFile
		tempFilenames[i] = tempFile.Name()

		// Write data to temp files
		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediate[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Printf("Cannot encode %v: %v", kv, err)
				// Clean up
				for j := 0; j <= i; j++ {
					tempFiles[j].Close()
					os.Remove(tempFilenames[j])
				}
				return false
			}
		}
		tempFile.Close()
	}

	// Now atomically rename the files to their final names
	for i := 0; i < task.NReduce; i++ {
		finalName := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		if err := os.Rename(tempFilenames[i], finalName); err != nil {
			log.Printf("Cannot rename temp file %s to %s: %v", tempFilenames[i], finalName, err)
			// We can't easily roll back at this point
			return false
		}
	}

	return true
}

// Reduce performs the reduce task.
func (w *Worker) Reduce(task Task) bool {
	// Read the output of all map tasks
	var intermediate []KeyValue
	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("Cannot open file %v: %v", filename, err)

			// Be more resilient - try a few times with backoff
			retryCount := 0
			for retryCount < w.maxRetries {
				time.Sleep(time.Second * time.Duration(retryCount+1))
				file, err = os.Open(filename)
				if err == nil {
					break
				}
				retryCount++
				log.Printf("Retry %d: Cannot open file %v: %v", retryCount, filename, err)
			}

			// If we still can't open it after retries, report failure
			if err != nil {
				return false
			}
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// Keystroke Sorting
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// Create a temporary file first for atomicity
	tempFile, err := os.CreateTemp("", "reduce-")
	if err != nil {
		log.Printf("Cannot create temp file: %v", err)
		return false
	}
	tempFilename := tempFile.Name()

	// Execute reduce and write to temp file
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reduceFunc(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	tempFile.Close()

	// Rename temp file to final output
	finalOutFile := fmt.Sprintf("mr-out-%d", task.TaskId)
	err = os.Rename(tempFilename, finalOutFile)
	if err != nil {
		log.Printf("Cannot rename temp file %s to %s: %v", tempFilename, finalOutFile, err)
		os.Remove(tempFilename) // Clean up the temp file
		return false
	}

	return true
}

// ihash calculates the hash value of the string
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// call sends an RPC request to the master
func call(rpcname string, args interface{}, reply interface{}) bool {
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		c, err := rpc.DialHTTP("unix", masterSock())
		if err != nil {
			log.Printf("dialing error (attempt %d/%d): %v", i+1, maxRetries, err)
			time.Sleep(time.Second)
			continue
		}
		defer c.Close()

		err = c.Call(rpcname, args, reply)
		if err == nil {
			return true
		}

		log.Printf("call error (attempt %d/%d): %v", i+1, maxRetries, err)
		time.Sleep(time.Second)
	}
	return false
}
