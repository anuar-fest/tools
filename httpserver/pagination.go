package httpserver

import (
	"fmt"
	"net/http"
	"strconv"
)

// Page is a validated limit/offset pair.
type Page struct {
	Limit  int
	Offset int
}

// PageConfig bounds ParsePage. Zero values mean limit 50, max 200, query
// parameters "limit" and "offset".
type PageConfig struct {
	DefaultLimit int
	MaxLimit     int
	LimitParam   string
	OffsetParam  string
}

// ParsePage reads and bounds pagination from the query. Anything over
// MaxLimit or negative is a 400 Problem, never clamped silently: a client
// asking for a million rows must learn the cap.
func ParsePage(r *http.Request, cfg PageConfig) (Page, error) {
	if cfg.DefaultLimit <= 0 {
		cfg.DefaultLimit = 50
	}
	if cfg.MaxLimit <= 0 {
		cfg.MaxLimit = 200
	}
	if cfg.LimitParam == "" {
		cfg.LimitParam = "limit"
	}
	if cfg.OffsetParam == "" {
		cfg.OffsetParam = "offset"
	}
	q := r.URL.Query()
	p := Page{Limit: cfg.DefaultLimit}
	if s := q.Get(cfg.LimitParam); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			return Page{}, NewProblem(http.StatusBadRequest, fmt.Sprintf("%s must be a positive integer.", cfg.LimitParam))
		}
		if n > cfg.MaxLimit {
			return Page{}, NewProblem(http.StatusBadRequest, fmt.Sprintf("%s must not exceed %d.", cfg.LimitParam, cfg.MaxLimit)).With("max_limit", cfg.MaxLimit)
		}
		p.Limit = n
	}
	if s := q.Get(cfg.OffsetParam); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			return Page{}, NewProblem(http.StatusBadRequest, fmt.Sprintf("%s must be a non-negative integer.", cfg.OffsetParam))
		}
		p.Offset = n
	}
	return p, nil
}
