// Package serve implements an HTTP server that exposes fuzz run reports through a JSON API and serves the embedded web frontend.
//
// Package serve 实现一个 HTTP 服务器，通过 JSON API 暴露 fuzz 运行报告，并提供内嵌的 Web 前端。
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

// webDist holds the embedded web frontend assets served under the static route.
//
// webDist 持有在静态路由下提供的内嵌 Web 前端资源。
//
//go:embed webdist/*
var webDist embed.FS

// server bundles the runtime configuration and the embedded frontend file system used by the HTTP handlers.
//
// server 将运行时配置与 HTTP 处理器使用的内嵌前端文件系统打包在一起。
type server struct {
	cfg      Config // server configuration (listen address, token, directories) / 服务器配置（监听地址、token、目录）
	webFiles fs.FS  // file system rooted at the embedded webdist directory / 以内嵌 webdist 目录为根的文件系统
}

// Execute applies defaults to cfg, prepares the data and output directories,
// then starts the HTTP server and blocks until the context is cancelled or the server stops.
//
// Execute 为 cfg 应用默认值，准备数据目录与输出目录，
// 随后启动 HTTP 服务器并阻塞，直到 context 被取消或服务器停止。
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

// maskToken returns a redacted form of token suitable for logging, hiding the middle characters.
//
// maskToken 返回适合用于日志记录的脱敏 token 形式，隐藏中间字符。
func maskToken(token string) string {
	if token == "" {
		return "<empty>"
	}
	if len(token) <= 6 {
		return "***"
	}
	return token[:3] + "***" + token[len(token)-2:]
}

// registerRoutes wires the API and static-file handlers onto the given mux.
//
// registerRoutes 将 API 与静态文件处理器挂载到给定的 mux 上。
func (s *server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/auth/verify", s.handleAuthVerify)

	mux.HandleFunc("/api/v1/reports", s.handleReports)
	mux.HandleFunc("/api/v1/reports/", s.handleReportByID)

	mux.HandleFunc("/", s.handleStatic)
}

// withRecover wraps next so that any panic is recovered and reported as a 500 JSON error response.
//
// withRecover 包装 next，使任何 panic 都被恢复，并以 500 JSON 错误响应上报。
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

// withRequestLog wraps next to log the method, path, and duration of each /api/ request to stderr.
//
// withRequestLog 包装 next，将每个 /api/ 请求的方法、路径与耗时记录到 stderr。
func (s *server) withRequestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if strings.HasPrefix(r.URL.Path, "/api/") {
			fmt.Fprintf(os.Stderr, "%s %s %s\n", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
		}
	})
}

// withCORS wraps next to add CORS headers and short-circuit OPTIONS preflight requests.
//
// withCORS 包装 next，添加 CORS 头并对 OPTIONS 预检请求进行短路处理。
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

// requireAuth reports whether the request is authorized, writing a 401 response when it is not.
// Non-API paths and the health endpoint are always allowed; otherwise the api_token query
// parameter or a Bearer Authorization header must match the configured API token.
//
// requireAuth 报告请求是否已授权，未授权时写入 401 响应。
// 非 API 路径与健康检查端点始终被允许；否则 api_token 查询参数
// 或 Bearer Authorization 头必须与配置的 API token 匹配。
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

// handleStatic serves files from the embedded frontend, falling back to index.html for SPA routes.
//
// handleStatic 从内嵌前端提供文件，对 SPA 路由回退到 index.html。
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

// writeJSON writes v as a JSON response with the given status code and content type.
//
// writeJSON 以给定的状态码与内容类型将 v 作为 JSON 响应写出。
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
