package lifecycle_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/anuar-fest/tools/lifecycle"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.IgnoreTopFunction("os/signal.signal_recv"), goleak.IgnoreTopFunction("os/signal.loop"))
}

type journal struct {
	mu      sync.Mutex
	entries []string
}

func (j *journal) add(s string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, s)
}

func (j *journal) list() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.entries...)
}

func blocking(j *journal, name string) lifecycle.Component {
	return lifecycle.Component{
		Name: name,
		Run: func(ctx context.Context) error {
			<-ctx.Done()
			j.add("run-exit " + name)
			return ctx.Err()
		},
		Stop: func(context.Context) error {
			j.add("stop " + name)
			return nil
		},
	}
}

func runAsync(ctx context.Context, app *lifecycle.App) <-chan error {
	errc := make(chan error, 1)
	go func() { errc <- app.Run(ctx) }()
	return errc
}

func TestRun_StopsInReverseOrderAndJoinsRuns(t *testing.T) {
	j := &journal{}
	app := lifecycle.New(lifecycle.Config{NoSignals: true, OnShutdown: func(string) { j.add("unready") }})
	app.Add(blocking(j, "telemetry"))
	app.Add(blocking(j, "db"))
	app.Add(blocking(j, "http"))
	errc := runAsync(context.Background(), app)
	time.Sleep(20 * time.Millisecond)
	app.Shutdown("test")
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	want := []string{"unready", "stop http", "run-exit http", "stop db", "run-exit db", "stop telemetry", "run-exit telemetry"}
	if got := j.list(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("order:\n got %v\nwant %v", got, want)
	}
}

func TestRun_ComponentFailureStopsEverythingAndIsReported(t *testing.T) {
	j := &journal{}
	app := lifecycle.New(lifecycle.Config{NoSignals: true})
	app.Add(blocking(j, "db"))
	app.Add(lifecycle.Component{Name: "consumer", Run: func(context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return errors.New("broker gone")
	}})
	err := app.Run(context.Background())
	var ce *lifecycle.ComponentError
	if !errors.As(err, &ce) || ce.Name != "consumer" || ce.Phase != "run" {
		t.Fatalf("cause must be the failing component: %v", err)
	}
	if lifecycle.ExitCode(err) != 1 {
		t.Fatal("a failure must exit non-zero")
	}
	if got := j.list(); len(got) != 2 || got[0] != "stop db" {
		t.Fatalf("db must still be stopped: %v", got)
	}
}

func TestRun_StopBudgetExceededIsReportedAndOthersStillStop(t *testing.T) {
	j := &journal{}
	app := lifecycle.New(lifecycle.Config{NoSignals: true, ShutdownTimeout: 80 * time.Millisecond})
	app.Add(blocking(j, "db"))
	app.Add(lifecycle.Component{Name: "slow", Stop: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}})
	app.Add(lifecycle.Component{Name: "stuck", Run: func(ctx context.Context) error {
		<-ctx.Done()
		time.Sleep(200 * time.Millisecond) // ignores the budget
		return nil
	}, StopTimeout: 30 * time.Millisecond})
	errc := runAsync(context.Background(), app)
	time.Sleep(10 * time.Millisecond)
	start := time.Now()
	app.Shutdown("test")
	err := <-errc
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("shutdown must not wait past its budget: %s", took)
	}
	if err == nil || !errors.Is(err, lifecycle.ErrStopTimeout) {
		t.Fatalf("budget overrun must be reported: %v", err)
	}
	if !strings.Contains(err.Error(), "stuck") || !strings.Contains(err.Error(), "slow") {
		t.Fatalf("both overruns named: %v", err)
	}
	if got := j.list(); len(got) == 0 || got[len(got)-1] != "run-exit db" {
		t.Fatalf("db must still be stopped after the overruns: %v", got)
	}
	time.Sleep(250 * time.Millisecond) // let the stuck goroutine finish for goleak
}

func TestRun_ParentContextCancelIsACleanShutdown(t *testing.T) {
	j := &journal{}
	app := lifecycle.New(lifecycle.Config{NoSignals: true})
	app.Add(blocking(j, "http"))
	ctx, cancel := context.WithCancel(context.Background())
	errc := runAsync(ctx, app)
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestRun_PanicInRunIsContained(t *testing.T) {
	j := &journal{}
	app := lifecycle.New(lifecycle.Config{NoSignals: true})
	app.Add(blocking(j, "db"))
	app.Add(lifecycle.Component{Name: "worker", Run: func(context.Context) error { panic("bug") }})
	err := app.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "panic in worker run") {
		t.Fatalf("%v", err)
	}
	if got := j.list(); len(got) != 2 {
		t.Fatalf("db must be stopped: %v", got)
	}
}

func TestRun_OneShotComponentDoesNotEndTheApp(t *testing.T) {
	j := &journal{}
	app := lifecycle.New(lifecycle.Config{NoSignals: true})
	app.Add(lifecycle.Component{Name: "migrate", Run: func(context.Context) error { return nil }})
	app.Add(blocking(j, "http"))
	errc := runAsync(context.Background(), app)
	select {
	case err := <-errc:
		t.Fatalf("one-shot finishing must not stop the app: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	app.Shutdown("done")
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestRun_SignalStartsShutdown(t *testing.T) {
	j := &journal{}
	app := lifecycle.New(lifecycle.Config{Signals: []os.Signal{syscall.SIGUSR1}})
	app.Add(blocking(j, "http"))
	errc := runAsync(context.Background(), app)
	time.Sleep(20 * time.Millisecond)
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("signal must start the shutdown")
	}
}

func TestAdd_RejectsBadComponentsAndLateAdds(t *testing.T) {
	app := lifecycle.New(lifecycle.Config{NoSignals: true})
	mustPanic := func(f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		f()
	}
	mustPanic(func() { app.Add(lifecycle.Component{Name: "x"}) })
	mustPanic(func() { app.Add(lifecycle.Component{Run: func(context.Context) error { return nil }}) })
	app.Add(blocking(&journal{}, "a"))
	errc := runAsync(context.Background(), app)
	time.Sleep(10 * time.Millisecond)
	mustPanic(func() { app.Add(blocking(&journal{}, "late")) })
	if err := app.Run(context.Background()); err == nil {
		t.Fatal("second Run must fail")
	}
	app.Shutdown("x")
	<-errc
}
