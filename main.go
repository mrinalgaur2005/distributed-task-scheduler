package main

import (
	"fmt"
	"time"

	"github.com/mrinalgaur2005/distributed-task-scheduler/producer"
	"github.com/mrinalgaur2005/distributed-task-scheduler/worker"
)

const (
	MaxRetries = 3
	MainStream = "tasks.stream"
	DLQStream  = "dlq.stream"
)

func main() {
	fmt.Println("Starting Distributed Task Scheduler")
	for i := 0; i < 3; i++ {
		sw := worker.NewStreamWorker(fmt.Sprintf("W%d", i+1))
		go sw.Start()
	}
	time.Sleep(1 * time.Second)
	producer.ProduceTasks(10)
	select {}
}
