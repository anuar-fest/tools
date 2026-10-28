// Command pgx shows Postgres tracing: OpenDB replaces sql.Open("pgx", dsn)
// and every query, batch and connect becomes a client span with the
// database semantic conventions; arguments never leave the process. It
// needs a Postgres database.
//
//	go run ./otelx/client/pgx -db-dsn postgres://user:pass@localhost:5432/app
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"

	"github.com/anuar-fest/tools/otelx"
	clientpgx "github.com/anuar-fest/tools/otelx/client/pgx"
)

func main() {
	dsn := flag.String("db-dsn", "", "Postgres DSN (required)")
	flag.Parse()
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "-db-dsn is required")
		os.Exit(2)
	}
	if err := run(context.Background(), *dsn); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, dsn string) error {
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return err
	}
	tel, err := otelx.New(ctx, otelx.Config{ServiceName: "clientpgx-example", Traces: otelx.TracesConfig{Exporter: exporter}})
	if err != nil {
		return err
	}
	defer func() { _ = tel.Shutdown(context.WithoutCancel(ctx)) }() // flush even if ctx ended

	tr, err := clientpgx.New(clientpgx.Config{SlowAcquire: time.Millisecond}) // waits above this become a span event
	if err != nil {
		return err
	}
	// Pair with metrics/pgx: clientpgx.OpenDB(dsn, multitracer.New(tr, mt)) runs both tracers on one connection.
	db, err := clientpgx.OpenDB(dsn, tr)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(4)

	ctx, root := tel.Tracer("example").Start(ctx, "list customers") // the query spans become its children
	defer root.End()
	rows, err := db.QueryContext(ctx, "SELECT datname FROM pg_database WHERE datname = $1", "postgres") // the span carries the text, never the argument
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return err
		}
		fmt.Println("database:", name)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "SELECT * FROM no_such_table") // a failed span with error.type=42P01 (the SQLSTATE)
	fmt.Println("expected failure:", err)
	return nil
}
