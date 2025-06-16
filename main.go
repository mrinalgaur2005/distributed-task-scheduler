package main

import (
	"fmt"
	"time"

	"github.com/mrinalgaur2005/distributed-task-scheduler/model"
	"github.com/mrinalgaur2005/distributed-task-scheduler/queue"
	"github.com/mrinalgaur2005/distributed-task-scheduler/worker"
)

func main() {
	numWorkers := 3
	taskChannel := make(chan model.Task, 10)

	for i := 0; i < numWorkers; i++ {
		w := &worker.Worker{
			ID:       i,
			TaskChan: taskChannel,
			Circuit:  queue.NewCircuitBreaker(),
		}
		go w.Start()
	}
	for i := 0; i < 10; i++ {
		t := model.Task{
			ID:       fmt.Sprintf("task-%d", i),
			Type:     "EMAIL",
			Priority: 1,
			Payload: map[string]interface{}{
				"to":      "user@example.com",
				"subject": fmt.Sprintf("Email #%d", i),
			},
		}
		taskChannel <- t
		fmt.Println("Dispatched:", t.ID)
		time.Sleep(500 * time.Millisecond)
	}

	time.Sleep(5 * time.Second)
}
