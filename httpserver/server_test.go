package httpserver_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/anuar-fest/tools/httpserver"
)

func get(t *testing.T, url string) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestServer_RunDrainsInFlightThenReturnsNil(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /slow", func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusNoContent)
	})
	srv, err := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0", Handler: mux, ShutdownTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Listen(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()

	respc := make(chan *http.Response, 1)
	go func() {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+srv.Addr().String()+"/slow", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			respc <- resp
		} else {
			close(respc)
		}
	}()
	<-started
	cancel()
	select {
	case err := <-runErr:
		t.Fatalf("Run returned while a request was in flight: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	resp, ok := <-respc
	if !ok || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("in-flight request must complete: %v", resp)
	}
	_ = resp.Body.Close()
	if err := <-runErr; err != nil {
		t.Fatalf("clean shutdown must return nil: %v", err)
	}
	// Everything after shutdown is refused.
	if err := srv.Listen(t.Context()); !errors.Is(err, httpserver.ErrClosed) {
		t.Fatalf("Listen after Shutdown = %v", err)
	}
}

func TestServer_ListenFailsOnBusyPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	srv, _ := httpserver.New(httpserver.Config{Addr: ln.Addr().String(), Handler: http.NotFoundHandler()})
	if err := srv.Listen(t.Context()); err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("busy port must fail at Listen: %v", err)
	}
}

func TestServer_ShutdownBudgetExceededClosesHard(t *testing.T) {
	entered := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done() // a handler that never finishes on its own
	})
	srv, _ := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0", Handler: h, ShutdownTimeout: 100 * time.Millisecond})
	if err := srv.Listen(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()
	go func() {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+srv.Addr().String()+"/", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-entered
	cancel()
	select {
	case err := <-runErr:
		if err == nil || !strings.Contains(err.Error(), "shutdown") {
			t.Fatalf("exceeded budget must be reported: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run must return once the budget is spent")
	}
}

func TestNew_RequiresAddrAndHandler(t *testing.T) {
	if _, err := httpserver.New(httpserver.Config{Handler: http.NotFoundHandler()}); err == nil {
		t.Fatal("missing Addr must fail")
	}
	if _, err := httpserver.New(httpserver.Config{Addr: ":0"}); err == nil {
		t.Fatal("missing Handler must fail")
	}
}

func TestServer_RunReturnsServeErrorWhenListenFails(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer func() { _ = ln.Close() }()
	srv, _ := httpserver.New(httpserver.Config{Addr: ln.Addr().String(), Handler: http.NotFoundHandler()})
	if err := srv.Run(context.Background()); err == nil {
		t.Fatal("Run must surface a listen failure instead of lingering unreachable")
	}
}

func TestServer_ServesRequests(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ping", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("pong")) })
	srv, _ := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0", Handler: mux})
	if err := srv.Listen(t.Context()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve() }()
	resp := get(t, "http://"+srv.Addr().String()+"/ping")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Serve after Shutdown must return nil: %v", err)
	}
}
