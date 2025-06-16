package worker

import (
	"fmt"

	"github.com/mrinalgaur2005/distributed-task-scheduler/model"
	"github.com/mrinalgaur2005/distributed-task-scheduler/queue"
)

type Worker struct {
	ID       int
	TaskChan chan model.Task
	Circuit  *queue.CircuitBreaker
}

func (w *Worker) Start() {
	for task := range w.TaskChan {
		if w.Circuit.Ready() {
			fmt.Printf("Worker %d processing task %s\n", w.ID, task.ID)
			err := w.Process(task)
			if err != nil {
				w.Circuit.Fail()
				fmt.Println("Error processing task:", err)
			} else {
				w.Circuit.Success()
			}
		} else {
			fmt.Printf("Worker %d circuit open, requeuing task %s\n", w.ID, task.ID)
			// TODO add reque ka logic
		}
	}
}

func (w *Worker) Process(task model.Task) error {
	switch task.Type {
	case "EMAIL":
		fmt.Println("Sending email:", task.Payload)
	case "DATA_PROCESS":
		fmt.Println("Processing data:", task.Payload)
	default:
		return fmt.Errorf("unknown task type: %s", task.Type)
	}
	return nil
}
