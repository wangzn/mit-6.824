package mapreduce

import "fmt"

var (
	idleWorkderChan  chan string
	maxWorker        int = 10000
	invalidWorkerMap map[string]bool
	taskChan         chan *DoTaskArgs
)

func init() {
	idleWorkderChan = make(chan string, maxWorker)
	invalidWorkerMap = make(map[string]bool, maxWorker)
	taskChan = make(chan *DoTaskArgs, maxWorker)
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	//
	fmt.Println("Do schedule...")
	doneChan := make(chan int, ntasks*10)
	taskChan = make(chan *DoTaskArgs, maxWorker)
	go func() {
		for {
			debug("waiting for new worker to arrive")
			newWorker := <-mr.registerChannel
			idleWorkderChan <- newWorker
			fmt.Printf("add new worker %s to idle chan \n", newWorker)
		}
	}()

	for i := 0; i < ntasks; i++ {
		args := new(DoTaskArgs)
		args.JobName = mr.jobName
		if phase == mapPhase {
			args.File = mr.files[i]
		}
		args.Phase = phase
		args.TaskNumber = i
		args.NumOtherPhase = nios
		taskChan <- args
	}

	go func() {
		for {
			task, more := <-taskChan
			if more {
				validWorker := ""
				for {
					idleWorker := <-idleWorkderChan
					if isValidWorker(idleWorker) {
						validWorker = idleWorker
						break
					}
				}
				fmt.Printf("Valid worker %s doing task %d \n", validWorker, task.TaskNumber)
				go doTask(validWorker, task, idleWorkderChan, doneChan)
			} else {
				fmt.Println("received all jobs")
				break
			}
		}
	}()

	done := 0
	for {
		doneID := <-doneChan
		fmt.Printf("task done, id %d \n", doneID)
		done++
		if done == ntasks-1 {
			close(taskChan)
			break
		}
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}

func doTask(
	workerName string, args *DoTaskArgs,
	idleChan chan string, doneChan chan int,
) error {
	var err error
	ok := call(workerName, "Worker.DoTask", args, err)
	idleChan <- workerName
	if err != nil || !ok {
		//redo this task, and mark this worker as invalid
		fmt.Printf("Task %d fail with worker %s \n", args.TaskNumber, workerName)
		taskChan <- args
		invalidWorkerMap[workerName] = true
	} else {
		fmt.Printf("Task %d done with no error \n", args.TaskNumber)
		doneChan <- args.TaskNumber
		fmt.Printf("After insert done chan\n")
	}
	return err
}

func isValidWorker(worker string) bool {
	if _, ok := invalidWorkerMap[worker]; ok {
		return false
	}
	return true
}
