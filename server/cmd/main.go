package main

import (
	"log"
	"vivius/server/internals/server"
)

func main() {
	server := server.NewServer(
		"localhost:5000",
	)
	if err := server.Start(); err != nil {
		log.Fatalln(err)
	}
}