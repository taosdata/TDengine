package serve

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"tdsqlsmith/internal/config"
	"time"
)

//go:embed webdist/*
var webDist embed.FS

type server struct {
	cfg      Config
	webFiles fs.FS
}

func Execute(ctx context.Context, cfg Config) error {
	if strings.TrimSpace(cfg.Listen) == "" {
		cfg.Listen = ":8080"
	}
	if strings.TrimSpace(cfg.DataDir) == "" {
		cfg.DataDir = "data"
	}
	if strings.TrimSpace(cfg.OutDir) == "" {
		cfg.OutDir = config.DefaultOutDir
	}
	if strings.TrimSpace(cfg.AllowOrigin) == "" {
		cfg.AllowOrigin = "*"
	}
	if strings.TrimSpace(cfg.APIToken) == "" {
		cfg.APIToken = "tdsqlsmith-dev-token"
	}

	absData, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("resolve data dir: %w", err)
	}
	cfg.DataDir = absData
	absOut, err := filepath.Abs(cfg.OutDir)
	if err != nil {
		return fmt.Errorf("resolve out dir: %w", err)
	}
	cfg.OutDir = absOut

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		return fmt.Errorf("create out dir: %w", err)
	}

	sub, err := fs.Sub(webDist, "webdist")
	if err != nil {
		return fmt.Errorf("load embedded frontend: %w", err)
	}
	s := &server{cfg: cfg, webFiles: sub}

	mux := http.NewServeMux()
	s.registerRoutes(mux)
	h := s.withCORS(s.withRequestLog(s.withRecover(mux)))

	httpServer := &http.Server{
		Addr:              cfg.Listen,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
	}

	runCtx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		<-runCtx.Done()
		shutdownCtx, stop := context.WithTimeout(context.Background(), 8*time.Second)
		defer stop()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	fmt.Fprintf(os.Stderr, "serve listening on %s (token=%s)\n", cfg.Listen, maskToken(cfg.APIToken))
	err = httpServer.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func maskToken(token string) string {
	if token == "" {
		return "<empty>"
	}
	if len(token) <= 6 {
		return "***"
	}
	return token[:3] + "***" + token[len(token)-2:]
}

func (s *server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/auth/verify", s.handleAuthVerify)

	mux.HandleFunc("/api/v1/reports", s.handleReports)
	mux.HandleFunc("/api/v1/reports/", s.handleReportByID)

	mux.HandleFunc("/", s.handleStatic)
}

func (s *server) withRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": fmt.Sprintf("panic: %v", rec)})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *server) withRequestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if strings.HasPrefix(r.URL.Path, "/api/") {
			fmt.Fprintf(os.Stderr, "%s %s %s\n", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
		}
	})
}

func (s *server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := s.cfg.AllowOrigin
		if origin == "" {
			origin = "*"
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *server) requireAuth(w http.ResponseWriter, r *http.Request) bool {
	if !strings.HasPrefix(r.URL.Path, "/api/") {
		return true
	}
	if r.URL.Path == "/api/v1/health" {
		return true
	}
	if qToken := strings.TrimSpace(r.URL.Query().Get("api_token")); qToken != "" && qToken == s.cfg.APIToken {
		return true
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	const prefix = "Bearer "
	if !strings.HasPrefix(auth, prefix) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "missing bearer token"})
		return false
	}
	got := strings.TrimSpace(strings.TrimPrefix(auth, prefix))
	if got == "" || got != s.cfg.APIToken {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "invalid bearer token"})
		return false
	}
	return true
}

func (s *server) handleStatic(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/") {
		http.NotFound(w, r)
		return
	}
	p := path.Clean("/" + strings.TrimSpace(r.URL.Path))
	if p == "/" {
		p = "/index.html"
	}
	name := strings.TrimPrefix(p, "/")
	if strings.Contains(name, "..") {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid path"})
		return
	}
	if b, err := fs.ReadFile(s.webFiles, name); err == nil {
		if strings.HasSuffix(name, ".js") {
			w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		}
		if strings.HasSuffix(name, ".css") {
			w.Header().Set("Content-Type", "text/css; charset=utf-8")
		}
		if strings.HasSuffix(name, ".svg") {
			w.Header().Set("Content-Type", "image/svg+xml")
		}
		_, _ = w.Write(b)
		return
	}
	if b, err := fs.ReadFile(s.webFiles, "index.html"); err == nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(b)
		return
	}
	http.NotFound(w, r)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}
