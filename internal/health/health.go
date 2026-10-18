// Package health serves liveness, readiness, and Prometheus metrics.
package health

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Checker reports whether the process is ready to forward traffic.
type Checker interface {
	Ready() (bool, string)
}

// Server is the metrics and health HTTP server.
type Server struct {
	addr string
	ln   net.Listener
	srv  *http.Server
}

// Listen binds addr and returns a server that is not yet serving.
func Listen(addr string, reg prometheus.Gatherer, ready Checker) (*Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		ok, msg := ready.Ready()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if !ok {
			if msg == "" {
				msg = "not ready"
			}
			http.Error(w, msg+"\n", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	return &Server{
		addr: ln.Addr().String(),
		ln:   ln,
		srv: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}, nil
}

// Addr is the address the server accepted, including an ephemeral port.
func (s *Server) Addr() string { return s.addr }

// Serve accepts connections until Shutdown.
func (s *Server) Serve() error {
	err := s.srv.Serve(s.ln)
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// Shutdown stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
