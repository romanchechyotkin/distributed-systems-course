package main

import (
	"log"

	"log_system/internal/server"
)

func main() {

	srv := server.NewHTTPServer(":8000")
	log.Println("server running")
	log.Fatal(srv.ListenAndServe())
}
