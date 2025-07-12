package handler

import (
	"context"
	"encoding/json"
	"net/http"
)

var ctx = context.Background()

func HealthCheck(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func Metrics(w http.ResponseWriter, r *http.Request) {
	keys, _ := rdb.Keys(ctx, "task:*").Result()

	completed := 0
	for _, key := range keys {
		status, err := rdb.HGet(ctx, key, "status").Result()
		if err == nil && status == "completed" {
			completed++
		}
	}

	stats := map[string]interface{}{
		"active_tasks": len(keys),
		"in_dlq":       getKeysCount("tasks:dlq"),
		"completed":    completed,
	}
	json.NewEncoder(w).Encode(stats)
}

func getKeysCount(pattern string) int {
	keys, err := rdb.Keys(ctx, pattern).Result()
	if err != nil {
		return 0
	}
	return len(keys)
}
