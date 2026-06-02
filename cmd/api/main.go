package main

import (
	"log"
	"net/http"
	"os"

	"ai-job-queue/internal/api"
	"ai-job-queue/internal/jobstore"
)

func main() {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	server := api.NewServer(jobstore.NewMemoryStore())

	log.Printf("api listening on %s", addr)
	if err := http.ListenAndServe(addr, server); err != nil {
		log.Fatal(err)
	}
}
