package main
import (
	"fmt"
	"time"
)

const (
	MaxJobQueue = 20000
	MaxWorker = 10000
)
var JobQueue chan Job
var WorkerPool chan Worker

type Job struct {
	Payload uint64
}

type Worker struct {
	JobChannel chan Job
	quit chan bool
}

func NewWorker() Worker {
	return Worker{
		JobChannel: make(chan Job),
		quit: make(chan bool)}
}
func (w Worker) Start(){
	go func() {
		for{
			WorkerPool<-w

			select {
			case job:= <-w.JobChannel:
				//do this job
				time.Sleep(time.Duration(1)*time.Millisecond)
				if j:=job.Payload/10000; j > currentJob {
					currentJob = j
					fmt.Printf("%d0000job is done\n", currentJob)
				}
			case <-w.quit:
				return
			}
		}
	}()
}
func (w Worker) Stop() {
	go func() {
		w.quit<-true
	}()
}

type Dispatcher struct {
	name string
}

func (d *Dispatcher) Dispatch() {
	for{
		select {
		case job:= <-JobQueue:
			go func(job Job) {
				worker := <-WorkerPool
				worker.JobChannel<-job
			}(job)
		}
	}
}
func (d *Dispatcher) Run(){
	for i:=0; i<MaxWorker; i++ {
		worker := NewWorker()
		worker.Start()
	}

	go d.Dispatch()
}

var currentJob uint64  = 0

func main (){

	fmt.Println("Init JobQueue:", MaxJobQueue, "WorkerPool:", MaxWorker)
	JobQueue = make(chan Job, MaxJobQueue)
	WorkerPool = make(chan Worker, MaxWorker)

	fmt.Println("Prepare Wokers and start workers")
	dispatcher := &Dispatcher{}
	dispatcher.Run()

	fmt.Println("Now is ready to wait 1 million requests")
	time.Sleep(time.Duration(3)*time.Second)

	cnt := uint64(0)
	for {
		cnt++
		job := Job{Payload: cnt}
		// request speed: 10K/s
		time.Sleep(time.Duration(100)*time.Microsecond)
		JobQueue<-job
	}
}