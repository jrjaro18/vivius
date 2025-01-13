package server

import (
	"net/http"
	"encoding/json"
	"log"
)

type Request struct {
	Key   string `json:"Key"`
	Value any    `json:"Value"`
}

type ApiFunc func(w http.ResponseWriter, r *http.Request) ApiError

type ApiError struct {
	message    string
	statusCode int
}

func (a *ApiError) Error() string {
	return a.message
}

func MakeHandler(f ApiFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := f(w, r); err.Error() != "" {
			WriteJson(w, err.statusCode, err.message)
		}
	}
}

func WriteJson(w http.ResponseWriter, sc int, v any) {
	w.WriteHeader(sc)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Fatalf("Error in writing response, %v\n", err)
	}
}