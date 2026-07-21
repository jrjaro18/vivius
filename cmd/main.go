package main

import (
	"vivius/store"
	"vivius/node"
	"log"
)

func main() {
	store := store.NewKvStore()
	node1 := node.NewNode(1, store, nil, nil)

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