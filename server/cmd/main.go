package main

import (
	"fmt"
	"vivius/server/internals/server"
)

func main() {
	server := server.NewServer[string, any]()
	server.Add("rohan", struct{
		name string
		age int
	}{
		"Rohan Vimal Jaiswal",
		21,
	})
	fmt.Printf("the store contains key rohan %+v", server.Contains("brohan"))
}