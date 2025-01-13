package server

import (
	"encoding/json"
	"net/http"
)

// Handlers:

func (s *Server) AddHandler() ApiFunc {
	return func(w http.ResponseWriter, r *http.Request) ApiError {
		var body Request
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			return ApiError{
				message:    "Internal Server Error",
				statusCode: http.StatusInternalServerError,
			}
		}

		// log.Printf("%+v\n", body)

		if body.Key == "" || body.Value == "" {
			return ApiError{
				message:    "key or value not found in the body",
				statusCode: http.StatusBadRequest,
			}
		}

		s.Add(body.Key, body.Value)
		WriteJson(w, http.StatusAccepted, map[string]any{body.Key: body.Value})
		return ApiError{}
	}
}

func (s *Server) GetHandler() ApiFunc {
	return func(w http.ResponseWriter, r *http.Request) ApiError {
		key := r.URL.Query().Get("key")
		if key == "" {
			return ApiError{
				message:    "key not found in the query",
				statusCode: http.StatusBadRequest,
			}
		}
		v, present := s.Get(key)
		if !present {
			WriteJson(w, http.StatusOK, "key not present")
			return ApiError{}
		}
		WriteJson(w, http.StatusOK, v)
		return ApiError{}
	}
}

func (s *Server) ContainsHandler() ApiFunc {
	return func(w http.ResponseWriter, r *http.Request) ApiError {
		key := r.URL.Query().Get("key")
		if key == "" {
			return ApiError{
				message:    "key not found in the query",
				statusCode: http.StatusBadRequest,
			}
		}
		present := s.Contains(key)
		WriteJson(w, http.StatusOK, present)
		return ApiError{}
	}
}

func (s *Server) RemoveHandler () ApiFunc {
	return func(w http.ResponseWriter, r *http.Request) ApiError {
		key := r.URL.Query().Get("key")
		if key == "" {
			return ApiError{
				message:    "key not found in the query",
				statusCode: http.StatusBadRequest,
			}
		}
		if err := s.Remove(key); err != nil {
			WriteJson(w, http.StatusOK, "key not present")
			return ApiError{}
		}

		WriteJson(w, http.StatusOK, "removed the key and the corresponding value from the store")

		return ApiError{}
	}
}