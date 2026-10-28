// Command lifecycle shows components started together and stopped in
// reverse order under one budget, with an exit code that tells the truth:
// a worker that fails starts the shutdown of everything else; a stop that
// overruns its budget is reported, not hidden.
//
//	go run ./lifecycle                 # then Ctrl-C: worker stops before the queue, exit 0
//	go run ./lifecycle -fail-after 3s  # the worker fails: everything stops, exit 1
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/anuar-fest/tools/lifecycle"
)

func main() {
	failAfter := flag.Duration("fail-after", 0, "make the worker fail after this long (0 = never)")
	slowStop := flag.Duration("slow-stop", 0, "make the queue take this long to stop (try 10s against the 5s budget)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	app := lifecycle.New(lifecycle.Config{
		Logger:          logger,
		ShutdownTimeout: 5 * time.Second, // keep it below terminationGracePeriodSeconds minus the preStop hook
		PreStopDelay:    500 * time.Millisecond,
		OnShutdown:      func(reason string) { logger.Info("shutdown starting", "reason", reason) }, // mark readiness draining here
	})

	// Add order is start order; stop order is the reverse: worker first (stop taking jobs), then the queue it reads from.
	queue := make(chan int, 16)
	app.Add(lifecycle.Component{
		Name: "queue",
		Run: func(ctx context.Context) error {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for i := 1; ; i++ {
				select {
				case <-ctx.Done():
					return nil
				case <-t.C:
					queue <- i
				}
			}
		},
		Stop: func(ctx context.Context) error {
			select { // a drain that overruns the budget surfaces as lifecycle.ErrStopTimeout, never as a silent exit 0
			case <-time.After(*slowStop):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		StopTimeout: 3 * time.Second,
	})
	app.Add(lifecycle.Component{
		Name: "worker",
		Run: func(ctx context.Context) error {
			start := time.Now()
			for {
				select {
				case <-ctx.Done():
					return nil
				case job := <-queue:
					logger.Info("job done", "job", job)
					if *failAfter > 0 && time.Since(start) > *failAfter {
						return errors.New("worker: simulated failure") // starts the shutdown of everything else
					}
				}
			}
		},
	})

	err := app.Run(context.Background())
	if err != nil {
		logger.Error("exited with errors", "err", err)
	}
	os.Exit(lifecycle.ExitCode(err)) // 0 for a clean signal-driven stop, non-zero otherwise
}
