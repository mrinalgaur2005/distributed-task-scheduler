package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/mrinalgaur2005/distributed-task-scheduler/model"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()
var rdb = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
})

const DLQStream = "tasks:dlq"

type RedisQueue struct {
	Client *redis.Client
	Stream string
	Group  string
	Name   string
}

func NewRedisQueue(stream, group, name string) *RedisQueue {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	rdb.XGroupCreateMkStream(ctx, stream, group, "$")
	return &RedisQueue{
		Client: rdb,
		Stream: stream,
		Group:  group,
		Name:   name,
	}
}

func (rq *RedisQueue) ReadTask() (*model.Task, string, error) {
	entries, err := rq.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    rq.Group,
		Consumer: rq.Name,
		Streams:  []string{rq.Stream, ">"},
		Count:    1,
		Block:    5 * time.Second,
	}).Result()

	if err != nil || len(entries) == 0 || len(entries[0].Messages) == 0 {
		return nil, "", err
	}

	msg := entries[0].Messages[0]
	raw := msg.Values["data"].(string)

	var task model.Task
	if err := json.Unmarshal([]byte(raw), &task); err != nil {
		return nil, "", err
	}

	return &task, msg.ID, nil
}

func (rq *RedisQueue) Ack(id string) {
	rq.Client.XAck(ctx, rq.Stream, rq.Group, id)
}

func SendToDLQ(task model.Task) error {
	payload, err := json.Marshal(task)
	if err != nil {
		return err
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: DLQStream,
		Values: map[string]interface{}{
			"data": payload,
		},
	}).Result()
	return err
}

// Called once per task (first submission)
func StoreTaskMetadata(client *redis.Client, task model.Task, status string) {
	key := "task:" + task.ID
	now := time.Now().Unix()

	data := map[string]interface{}{
		"id":         task.ID,
		"type":       task.Type,
		"priority":   task.Priority,
		"retryCount": task.RetryCount,
		"status":     status,
		"createdAt":  now,
		"updatedAt":  now,
	}

	client.HSet(ctx, key, data)
}

// Called to update status & retry count
func UpdateTaskStatus(client *redis.Client, taskID string, status string, retries int) {
	key := "task:" + taskID
	client.HSet(ctx, key, map[string]interface{}{
		"status":     status,
		"retryCount": retries,
		"updatedAt":  time.Now().Unix(),
	})
}
