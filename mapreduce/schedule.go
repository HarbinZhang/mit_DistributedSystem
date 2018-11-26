package mapreduce

import (
	"fmt"
	"sync"
)

type WorkerFailureCount struct {
	sync.Mutex
	workers map[string]int
}

func (r *WorkerFailureCount) inc(srv string) {
	r.Lock()
	r.workers[srv]++
	r.Unlock()
}

func (r *WorkerFailureCount) get(srv string) int {
	r.Lock()
	defer r.Unlock()
	return r.workers[srv]
}

const MaxWorkerFailures = 5

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// Create task list
	var tasks []DoTaskArgs
	for i := 0; i < ntasks; i++ {
		task := DoTaskArgs{
			JobName:       jobName,
			File:          mapFiles[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other,
		}
		tasks = append(tasks, task)
	}

	readyChan := make(chan string, ntasks)
	retryChan := make(chan DoTaskArgs, ntasks)
	failureCounts := WorkerFailureCount{workers: make(map[string]int)}

	var waitGroup sync.WaitGroup

	// startTask
	startTask := func(worker string, task DoTaskArgs) {
		defer waitGroup.Done()
		fmt.Println(task)
		success := call(worker, "Worker.DoTask", &task, nil)
		readyChan <- worker
		if !success {
			fmt.Printf("Schedule: %v task #%d failed to execute by %s\n", phase, task.TaskNumber, worker)
			failureCounts.inc(worker)
			retryChan <- task
		}
	}

	// assign
	for len(tasks) > 0 {
		select {
		case worker := <-registerChan:
			readyChan <- worker
		case task := <-retryChan:
			tasks = append(tasks, task)
		case worker := <-readyChan:
			if failureCounts.get(worker) < MaxWorkerFailures {
				waitGroup.Add(1)
				task := tasks[0]
				tasks = tasks[1:]
				go startTask(worker, task)
				// waitGroup.Done()
			} else {
				// the worker will be dropped
			}

		}
	}

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	waitGroup.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
