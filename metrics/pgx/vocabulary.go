package metricspgx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/anuar-fest/tools/metrics"
)

// ConnAttrs are the connection attributes every sample and span carries:
// db.system.name, db.namespace, server.address, server.port. nil is just
// the system.
func ConnAttrs(cc *pgx.ConnConfig) []attribute.KeyValue {
	attrs := []attribute.KeyValue{semconv.DBSystemNamePostgreSQL}
	if cc == nil {
		return attrs
	}
	if cc.Database != "" {
		attrs = append(attrs, semconv.DBNamespace(cc.Database))
	}
	if cc.Host != "" {
		attrs = append(attrs, semconv.ServerAddress(cc.Host))
	}
	if cc.Port != 0 {
		attrs = append(attrs, semconv.ServerPort(int(cc.Port)))
	}
	return attrs
}

// PoolName identifies a pool for db.client.connection.pool.name:
// "database@host:port".
func PoolName(pool *pgxpool.Pool) string {
	if pool == nil || pool.Config() == nil || pool.Config().ConnConfig == nil {
		return ""
	}
	cc := pool.Config().ConnConfig
	return fmt.Sprintf("%s@%s:%d", cc.Database, cc.Host, cc.Port)
}

var operations = map[string]struct{}{
	"SELECT": {}, "INSERT": {}, "UPDATE": {}, "DELETE": {}, "MERGE": {}, "WITH": {}, "VALUES": {},
	"BEGIN": {}, "START": {}, "COMMIT": {}, "ROLLBACK": {}, "SAVEPOINT": {}, "RELEASE": {}, "END": {},
	"SET": {}, "RESET": {}, "SHOW": {}, "DISCARD": {},
	"CREATE": {}, "ALTER": {}, "DROP": {}, "TRUNCATE": {}, "COMMENT": {}, "REINDEX": {}, "CLUSTER": {},
	"COPY": {}, "LOCK": {}, "LISTEN": {}, "NOTIFY": {}, "UNLISTEN": {},
	"EXPLAIN": {}, "ANALYZE": {}, "VACUUM": {}, "GRANT": {}, "REVOKE": {},
	"PREPARE": {}, "EXECUTE": {}, "DEALLOCATE": {}, "DO": {}, "CALL": {}, "REFRESH": {},
	"DECLARE": {}, "FETCH": {}, "MOVE": {}, "CLOSE": {},
}

// Operation returns the leading SQL keyword, upper-cased, skipping
// comments, or "OTHER" for anything outside the known set, so the
// db.operation.name label is a closed set.
func Operation(sql string) string {
	s := sql
	for {
		s = strings.TrimLeftFunc(s, unicode.IsSpace)
		switch {
		case strings.HasPrefix(s, "--"):
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = s[i+1:]
				continue
			}
			return "OTHER"
		case strings.HasPrefix(s, "/*"):
			if i := strings.Index(s, "*/"); i >= 0 {
				s = s[i+2:]
				continue
			}
			return "OTHER"
		}
		break
	}
	end := 0
	for end < len(s) && end < 16 && (s[end] >= 'a' && s[end] <= 'z' || s[end] >= 'A' && s[end] <= 'Z') {
		end++
	}
	op := strings.ToUpper(s[:end])
	if _, ok := operations[op]; ok {
		return op
	}
	return "OTHER"
}

// ErrorType is the low-cardinality error.type for err: the SQLSTATE of a
// server error, the context sentinel for cancellations, or the Go type.
func ErrorType(err error) string {
	var pgErr *pgconn.PgError
	switch {
	case errors.As(err, &pgErr):
		return pgErr.Code
	case errors.Is(err, context.Canceled):
		return "context.Canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "context.DeadlineExceeded"
	case errors.Is(err, pgx.ErrNoRows):
		return "pgx.ErrNoRows"
	default:
		return metrics.ErrorType(err)
	}
}
