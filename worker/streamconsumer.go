package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/mrinalgaur2005/distributed-task-scheduler/model"
	"github.com/mrinalgaur2005/distributed-task-scheduler/queue"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

const MaxRetries = 3

type StreamWorker struct {
	ID         string
	Redis      *redis.Client
	Stream     string
	Group      string
	ConsumerID string
}

func NewStreamWorker(id string) *StreamWorker {
	return &StreamWorker{
		ID:         id,
		Redis:      redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
		Stream:     "tasks:high_priority",
		Group:      "workers",
		ConsumerID: "consumer-" + id,
	}
}

func (sw *StreamWorker) Start() {
	for {
		streams, err := sw.Redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    sw.Group,
			Consumer: sw.ConsumerID,
			Streams:  []string{sw.Stream, ">"},
			Count:    1,
			Block:    5 * time.Second,
		}).Result()

		if err != nil && err != redis.Nil {
			log.Printf("[%s] Error reading from stream: %v", sw.ID, err)
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				raw := msg.Values["data"].(string)
				var task model.Task
				if err := json.Unmarshal([]byte(raw), &task); err != nil {
					log.Printf("[%s] Failed to unmarshal task: %v", sw.ID, err)
					continue
				}

				log.Printf("[%s] Processing task %s (%s)", sw.ID, task.ID, task.Type)
				if err := processTask(task); err != nil {
					task.RetryCount++

					if task.RetryCount > MaxRetries {
						log.Printf("[%s] Task %s failed after max retries. Sending to DLQ.", sw.ID, task.ID)
						queue.SendToDLQ(task)
					} else {
						log.Printf("[%s] Retrying task %s (Attempt %d)", sw.ID, task.ID, task.RetryCount)
						updated, _ := json.Marshal(task)
						sw.Redis.XAdd(ctx, &redis.XAddArgs{
							Stream: sw.Stream,
							Values: map[string]interface{}{
								"data": updated,
							},
						})
					}
				} else {
					log.Printf("[%s] Completed task %s", sw.ID, task.ID)
				}
				sw.Redis.XAck(ctx, sw.Stream, sw.Group, msg.ID)
			}
		}
	}
}
func processTask(task model.Task) error {
	if task.Payload["subject"] == "Welcome to Task #6" {
		return fmt.Errorf("simulated failure")
	}
	if task.Type == "EMAIL" {
		fmt.Printf("Sending email to %s with subject '%s'\n", task.Payload["to"], task.Payload["subject"])
		time.Sleep(1 * time.Second)
		if rand.Intn(3) == 0 {
			return errors.New("simulated email failure")
		}
	}
	return nil
}
