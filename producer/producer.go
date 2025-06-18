package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mrinalgaur2005/distributed-task-scheduler/model"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func ProduceTasks(n int) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	for i := 0; i < n; i++ {
		task := model.Task{
			ID:       uuid.NewString(),
			Type:     "EMAIL",
			Priority: 1,
			Payload: map[string]interface{}{
				"to":      fmt.Sprintf("user%d@example.com", i),
				"subject": fmt.Sprintf("Welcome to Task #%d", i),
			},
			RetryCount: 0,
		}

		taskJSON, err := json.MarshalIndent(task, "", "  ")
		if err != nil {
			fmt.Printf("Failed to marshal task %s: %v\n", task.ID, err)
			continue
		}

		fmt.Println("------------------------------------------------------")
		fmt.Printf("Preparing Task #%d:\n", i+1)
		fmt.Printf("ID: %s\n", task.ID)
		fmt.Printf("Type: %s | Priority: %d\n", task.Type, task.Priority)
		fmt.Printf("Payload:\n%s\n", taskJSON)

		res, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "tasks:high_priority",
			Values: map[string]interface{}{
				"data": taskJSON,
			},
		}).Result()

		if err != nil {
			fmt.Printf("Failed to push task %s to Redis: %v\n", task.ID, err)
		} else {
			fmt.Printf("Task pushed to Redis with Stream ID: %s\n", res)
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Println("All tasks dispatched.")
}
