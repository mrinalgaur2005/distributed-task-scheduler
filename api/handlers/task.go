package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/mrinalgaur2005/distributed-task-scheduler/model"
	"github.com/mrinalgaur2005/distributed-task-scheduler/queue"
	"github.com/redis/go-redis/v9"
)

var rdb = redis.NewClient(&redis.Options{Addr: "localhost:6379"})

func GetAllTasks(w http.ResponseWriter, r *http.Request) {
	keys, err := rdb.Keys(r.Context(), "task:*").Result()
	if err != nil {
		http.Error(w, "Failed to fetch task keys", 500)
		return
	}

	tasks := []map[string]interface{}{}
	for _, key := range keys {
		data, err := rdb.HGetAll(r.Context(), key).Result()
		if err == nil && len(data) > 0 {
			formatted := queue.FormatTaskMetadata(data)
			tasks = append(tasks, formatted)
		}

	}

	json.NewEncoder(w).Encode(tasks)
}

func GetTaskStatus(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	data, err := rdb.HGetAll(r.Context(), "task:"+id).Result()
	if err != nil || len(data) == 0 {
		http.Error(w, "Task not found", 404)
		return
	}
	json.NewEncoder(w).Encode(data)
}

func RetryTask(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	taskData, err := rdb.HGetAll(r.Context(), "task:"+id).Result()
	if err != nil || len(taskData) == 0 {
		http.Error(w, "Task not found", 404)
		return
	}

	task, err := queue.RebuildTaskFromMetadata(taskData)
	if err != nil {
		http.Error(w, "Failed to parse task", 500)
		return
	}
	queue.RequeueTask(task)
	json.NewEncoder(w).Encode(map[string]string{"status": "requeued"})
}

type CreateTaskRequest struct {
	Type     string                 `json:"type"`
	Priority int                    `json:"priority"`
	Payload  map[string]interface{} `json:"payload"`
}

func CreateTask(w http.ResponseWriter, r *http.Request) {
	fmt.Println("heyyys")
	var req CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	task := model.Task{
		ID:         uuid.NewString(),
		Type:       req.Type,
		Priority:   req.Priority,
		Payload:    req.Payload,
		RetryCount: 0,
	}

	taskBytes, err := json.Marshal(task)
	if err != nil {
		http.Error(w, "Failed to serialize task", http.StatusInternalServerError)
		return
	}

	// Push to Redis stream
	streamID, err := rdb.XAdd(r.Context(), &redis.XAddArgs{
		Stream: "tasks:high_priority",
		Values: map[string]interface{}{"data": string(taskBytes)},
	}).Result()
	if err != nil {
		http.Error(w, "Failed to enqueue task", http.StatusInternalServerError)
		return
	}

	// Store metadata
	queue.StoreTaskMetadata(rdb, task, "queued")

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "queued",
		"task_id":  task.ID,
		"streamId": streamID,
		"created":  time.Now().Unix(),
	})
}
