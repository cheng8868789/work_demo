package main

import (
	"fmt"
)

/*
	工作池编写
*/

type Job interface {
	Do() error
}

// define job channel
type JobChan chan Job

var (
	JobQueue   JobChan
	MaxWorkerSize = 32
	MaxJobChanLen = 32
)

func Init() {
	// Job队列
	JobQueue = make(chan  Job,1024)
}

type Worker struct {
	JobChannel JobChan
	index int
}

func (w *Worker) start() {
	go func() {
		for {
			select {
			case job := <-w.JobChannel:
				if err := job.Do(); err != nil {
					fmt.Printf("excute job failed with err: %v", err)
				}
			default:
				//fmt.Println("no job to worker")
			}
		}
	}()
	fmt.Println("worker start")
}

func (w *Worker) isFull() bool {
	if len(w.JobChannel) == MaxJobChanLen {
		fmt.Printf("Worker[%d] is full \n",w.index)
		return true
	}
	return false
}

func (d *Dispatcher) getWroker() *Worker {
	worker := <- d.WorkerPool
	if worker.isFull() {
		d.WorkerPool <- worker
	}
	return worker
}


type Dispatcher struct {
	WorkerPool chan *Worker
}

func GetDispatcher() *Dispatcher {
	dispatcher := new(Dispatcher)
	dispatcher.WorkerPool = make(chan *Worker,32)
	return dispatcher
}

func(d *Dispatcher) Run() {
	for i:=0;i<MaxWorkerSize;i++ {
		worker := NewWroker(i)
		worker.start()
		d.WorkerPool <- worker
	}
	for {
		var worker *Worker
		select {
			case job := <- JobQueue:
			go func(job Job) {
				worker = d.getWroker()
				worker.JobChannel <- job
				if worker.isFull() {
					worker = d.getWroker()
				}
				d.WorkerPool <- worker
			}(job)
		}
	}

}

func NewWroker(n int) *Worker {
	worker := new(Worker)
	worker.JobChannel = make(chan Job,MaxJobChanLen)
	worker.index = n
	return worker
}

type JobRequest struct {
}

func (j *JobRequest) Do() error {
	fmt.Println("Request done......")
	return nil
}

func NewJob() Job {
	return new(JobRequest)
}

func main() {

	Init()
	var foreverCh = make(chan int)

	dispatcher := GetDispatcher()
	go func() {
		for {
			job := NewJob()
			JobQueue <- job
			fmt.Println("send a job to JobQueue")
		}
	}()

	dispatcher.Run()

	<- foreverCh
}
