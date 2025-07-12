package main

import (
	"log"
	"net/http"

	"github.com/mrinalgaur2005/distributed-task-scheduler/api"
)

func main() {
	log.Println("API Server listening on :8080")
	http.ListenAndServe(":8080", api.SetupRouter())
}
