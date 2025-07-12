package main

import (
	"log"
	"net/http"

	"github.com/mrinalgaur2005/distributed-task-scheduler/api"
	"github.com/mrinalgaur2005/distributed-task-scheduler/internal/middleware"
)

func main() {
	log.Println("API Server listening on :8080")
	handler := middleware.CORSMiddleware(api.SetupRouter())
	http.ListenAndServe(":8080", handler)

}
