package main

import (
	"vivius/store/internals/store"
)

func main() {
	store := store.NewStore()
	store.Add("rohan", struct{
		name string
		age int
	}{
		"Rohan Vimal Jaiswal",
		21,
	})
}
