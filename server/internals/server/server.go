package server

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
	"vivius/store/internals/store"
)

type Server struct {
	*store.Store
	Addr string
	App *http.Server
}

// Creates a New Server Instance
func NewServer(address string) *Server {
	return &Server{
		Store: store.NewStore(),
		Addr: address,
	}
}

// Starts the server
func (s *Server) Start() error {

    quit := make(chan os.Signal, 1)
    signal.Notify(quit, os.Interrupt)
	defer signal.Stop(quit)

    done := make(chan struct{})

    go func() {
        <-quit
        log.Println("initializing server shut down...")

        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()

        if err := s.App.Shutdown(ctx); err != nil {
            log.Fatalf("Server forced to shut down: %v", err)
        }

        close(done)
    }()

    log.Printf("initializing Server... on %v\n", s.Addr)
	s.App = &http.Server{
		Addr: s.Addr,
		Handler: s.InitHandler(),
	}
    if err := s.App.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        return err
    }

    <-done
    log.Println("server exited...")
    return nil
}

func (s *Server) InitHandler() *http.ServeMux{
	mux := http.NewServeMux()
	mux.HandleFunc("/add", MakeHandler(s.AddHandler()))
	mux.HandleFunc("/get", MakeHandler(s.GetHandler()))
	mux.HandleFunc("/contains", MakeHandler(s.ContainsHandler()))
	mux.HandleFunc("/remove", MakeHandler(s.RemoveHandler()))
	return mux
}