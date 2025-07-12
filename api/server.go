package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	handler "github.com/mrinalgaur2005/distributed-task-scheduler/api/handlers"
)

func SetupRouter() http.Handler {
	r := chi.NewRouter()

	r.Get("/tasks", handler.GetAllTasks)
	r.Post("/create", handler.CreateTask)
	r.Get("/tasks/{id}", handler.GetTaskStatus)
	r.Post("/tasks/{id}/retry", handler.RetryTask)

	r.Get("/monitor/health", handler.HealthCheck)
	r.Get("/monitor/metrics", handler.Metrics)

	return r
}
