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
	id int
}

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string

// WorkerMain starts the worker process.
func WorkerMain(opts ...Option) {
	opt := defaultOption()
	for _, o := range opts {
		o(opt)
	}

	w := &Worker{}
	w.run(opt.mapFunc, opt.reduceFunc)
}

// run worker's main loop
func (w *Worker) run(mapf MapFunc, reducef ReduceFunc) {
	for {
		// request task
		task := w.requestTask()
		if task.Type == NoTask {
			time.Sleep(time.Second)
			continue
		}

		// execute task
		success := false
		if task.Type == MapTask {
			success = w.Map(mapf, task)
		} else if task.Type == ReduceTask {
			success = w.Reduce(reducef, task)
		}

		// reporting on mandate completion
		w.reportTask(task.TaskId, success)
	}
}

// requestTask request a task from the master
func (w *Worker) requestTask() Task {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		log.Fatal("RequestTask failed")
	}

	return reply.Task
}

// reportTask reports the completion of the task to the master.
func (w *Worker) reportTask(taskId int, success bool) {
	args := ReportTaskArgs{
		TaskId:  taskId,
		Success: success,
	}
	reply := ReportTaskReply{}

	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		log.Fatal("ReportTask failed")
	}
}

// Map performs the map task
func (w *Worker) Map(mapf MapFunc, task Task) bool {
	// read the input file
	content, err := os.ReadFile(task.Filename)
	if err != nil {
		log.Printf("Cannot read file %v", task.Filename)
		return false
	}

	// execute the map
	kva := mapf(task.Filename, string(content))

	// create intermediate files
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		reduceTask := ihash(kv.Key) % task.NReduce
		intermediate[reduceTask] = append(intermediate[reduceTask], kv)
	}

	// write in intermediate files
	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		file, err := os.Create(filename)
		if err != nil {
			log.Printf("Cannot create file %v", filename)
			return false
		}
		enc := json.NewEncoder(file)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Printf("Cannot encode %v", kv)
				return false
			}
		}
		file.Close()
	}

	return true
}

// Reduce performs the reduce task.
func (w *Worker) Reduce(reducef ReduceFunc, task Task) bool {
	// Read the output of all map tasks
	var intermediate []KeyValue
	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("Cannot open file %v", filename)
			return false
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

	// create the output file
	outFile := fmt.Sprintf("mr-out-%d", task.TaskId)
	ofile, err := os.Create(outFile)
	if err != nil {
		log.Printf("Cannot create file %v", outFile)
		return false
	}

	// execute reduce
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
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	ofile.Close()
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
		c, err := rpc.DialHTTP("unix", coordinatorSock())
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
