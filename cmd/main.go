package main

import (
	"vivius/kvStore"
	"vivius/node"
	"log"
)

func main() {
	store := kvStore.NewStore()
	node1 := node.NewNode(1, store)

	val, exists := node1.Get("A")
	if exists {
		log.Println("Value for key A:", val)
	} else {
		log.Println("Key not found")
	}

	node1.Set("A", "Value for A")
	val, exists = node1.Get("A")
	if exists {
		log.Println("Value for key A:", val)
	} else {
		log.Println("Key not found")
	}
}