package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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
	switch task.Type {
	case "EMAIL":
		if to, ok := task.Payload["to"].(string); ok {
			data["payload.to"] = to
		}
		if subject, ok := task.Payload["subject"].(string); ok {
			data["payload.subject"] = subject
		}
		// add more task types like SMS, PUSH etc. here if needed
	}
	client.HSet(ctx, key, data)
}

func UpdateTaskStatus(client *redis.Client, taskID string, status string, retries int) {
	key := "task:" + taskID
	client.HSet(ctx, key, map[string]interface{}{
		"status":     status,
		"retryCount": retries,
		"updatedAt":  time.Now().Unix(),
	})
}
func GetRedisClient() *redis.Client {
	return rdb
}
func RebuildTaskFromMetadata(data map[string]string) (model.Task, error) {
	if data["id"] == "" || data["type"] == "" {
		return model.Task{}, errors.New("invalid metadata")
	}

	retryCount, _ := strconv.Atoi(data["retryCount"])
	priority, _ := strconv.Atoi(data["priority"])

	return model.Task{
		ID:         data["id"],
		Type:       data["type"],
		Priority:   priority,
		RetryCount: retryCount,
		Payload:    map[string]interface{}{},
	}, nil
}
func RequeueTask(task model.Task) error {
	payload, err := json.Marshal(task)
	if err != nil {
		return err
	}

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "tasks:high_priority",
		Values: map[string]interface{}{
			"data": payload,
		},
	}).Result()

	if err == nil {
		UpdateTaskStatus(rdb, task.ID, "requeued", task.RetryCount)
	}
	return err
}
func FormatTaskMetadata(data map[string]string) map[string]interface{} {
	description := ""

	if data["type"] == "EMAIL" {
		to := data["payload.to"]
		subject := data["payload.subject"]
		description = fmt.Sprintf("EMAIL to %s about '%s'", to, subject)
	}

	return map[string]interface{}{
		"id":          data["id"],
		"type":        data["type"],
		"priority":    toInt(data["priority"]),
		"retryCount":  toInt(data["retryCount"]),
		"status":      data["status"],
		"createdAt":   toTime(data["createdAt"]),
		"updatedAt":   toTime(data["updatedAt"]),
		"description": description,
	}
}
func toInt(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0 // or handle error as needed
	}
	return n
}

func toTime(s string) string {
	sec, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return ""
	}
	return time.Unix(sec, 0).Format("2006-01-02 15:04:05")
}
