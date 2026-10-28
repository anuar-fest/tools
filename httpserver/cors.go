package httpserver

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// CORSConfig configures CORS. Origins are exact matches; use
// AllowOriginFunc for patterns. "*" is accepted only without credentials.
type CORSConfig struct {
	AllowedOrigins  []string
	AllowOriginFunc func(origin string) bool
	// AllowedMethods default to GET, HEAD, POST, PUT, PATCH, DELETE.
	AllowedMethods []string
	// AllowedHeaders default to Accept, Authorization, Content-Type, X-Request-Id.
	AllowedHeaders   []string
	ExposedHeaders   []string
	AllowCredentials bool
	// MaxAge caches the preflight; default 10 minutes.
	MaxAge time.Duration
}

// CORS answers preflights and stamps allowed responses. A request from an
// origin that is not allowed passes through without CORS headers (the
// browser blocks it); a preflight from one gets 403. It fails at
// construction with no origins, or with "*" plus credentials, which
// browsers refuse anyway.
func CORS(cfg CORSConfig) (func(http.Handler) http.Handler, error) {
	if len(cfg.AllowedOrigins) == 0 && cfg.AllowOriginFunc == nil {
		return nil, errors.New("httpserver: CORS needs AllowedOrigins or AllowOriginFunc")
	}
	any := false
	origins := map[string]struct{}{}
	for _, o := range cfg.AllowedOrigins {
		if o == "*" {
			any = true
			continue
		}
		origins[strings.ToLower(o)] = struct{}{}
	}
	if any && cfg.AllowCredentials {
		return nil, errors.New("httpserver: CORS cannot allow credentials for every origin")
	}
	methods := cfg.AllowedMethods
	if len(methods) == 0 {
		methods = []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete}
	}
	headers := cfg.AllowedHeaders
	if len(headers) == 0 {
		headers = []string{"Accept", "Authorization", "Content-Type", HeaderRequestID}
	}
	allowedMethods := map[string]struct{}{}
	for _, m := range methods {
		allowedMethods[strings.ToUpper(m)] = struct{}{}
	}
	allowedMethods[http.MethodOptions] = struct{}{}
	allowedHeaders := map[string]struct{}{}
	for _, h := range headers {
		allowedHeaders[strings.ToLower(h)] = struct{}{}
	}
	methodList := strings.Join(methods, ", ")
	headerList := strings.Join(headers, ", ")
	exposeList := strings.Join(cfg.ExposedHeaders, ", ")
	maxAge := cfg.MaxAge
	if maxAge <= 0 {
		maxAge = 10 * time.Minute
	}
	maxAgeSecs := strconv.Itoa(int(maxAge.Seconds()))

	allowed := func(origin string) bool {
		if any {
			return true
		}
		if _, ok := origins[strings.ToLower(origin)]; ok {
			return true
		}
		return cfg.AllowOriginFunc != nil && cfg.AllowOriginFunc(origin)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin == "" {
				next.ServeHTTP(w, r)
				return
			}
			h := w.Header()
			h.Add("Vary", "Origin")
			preflight := r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != ""
			if !allowed(origin) {
				if preflight {
					Fail(w, r, http.StatusForbidden, "Origin not allowed.")
					return
				}
				next.ServeHTTP(w, r)
				return
			}
			allowOrigin := origin
			if any && !cfg.AllowCredentials {
				allowOrigin = "*"
			}
			h.Set("Access-Control-Allow-Origin", allowOrigin)
			if cfg.AllowCredentials {
				h.Set("Access-Control-Allow-Credentials", "true")
			}
			if !preflight {
				if exposeList != "" {
					h.Set("Access-Control-Expose-Headers", exposeList)
				}
				next.ServeHTTP(w, r)
				return
			}
			h.Add("Vary", "Access-Control-Request-Method")
			h.Add("Vary", "Access-Control-Request-Headers")
			if _, ok := allowedMethods[strings.ToUpper(r.Header.Get("Access-Control-Request-Method"))]; !ok {
				Fail(w, r, http.StatusForbidden, "Method not allowed by CORS policy.")
				return
			}
			for _, reqHeaders := range r.Header.Values("Access-Control-Request-Headers") {
				for _, name := range strings.Split(reqHeaders, ",") {
					name = strings.ToLower(strings.TrimSpace(name))
					if name == "" {
						continue
					}
					if _, ok := allowedHeaders[name]; !ok {
						Fail(w, r, http.StatusForbidden, "Header not allowed by CORS policy.")
						return
					}
				}
			}
			h.Set("Access-Control-Allow-Methods", methodList)
			h.Set("Access-Control-Allow-Headers", headerList)
			h.Set("Access-Control-Max-Age", maxAgeSecs)
			w.WriteHeader(http.StatusNoContent)
		})
	}, nil
}
