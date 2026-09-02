package serve

// api_reports.go implements the endpoints that list run report summaries and serve individual reports.
//
// api_reports.go 实现列出运行报告摘要以及返回单个报告的端点。

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"tdsqlsmith/internal/report"
)

// handleReports responds to GET requests with the list of available run report summaries.
//
// handleReports 对 GET 请求返回可用运行报告摘要的列表。
func (s *server) handleReports(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	items, err := s.listReportSummaries()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})
}

// handleReportByID responds to GET requests with the full run report identified by the trailing path segment.
//
// handleReportByID 对 GET 请求返回由路径末段标识的完整运行报告。
func (s *server) handleReportByID(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	tail := strings.TrimPrefix(r.URL.Path, "/api/v1/reports/")
	tail = strings.Trim(tail, "/")
	if tail == "" || strings.Contains(tail, "/") {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "missing run id"})
		return
	}
	runReport, err := s.readRunReport(tail)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, runReport)
}

// listReportSummaries scans the output directory and returns one summary per run report, newest first.
//
// listReportSummaries 扫描输出目录，为每个运行报告返回一条摘要，最新的排在前面。
func (s *server) listReportSummaries() ([]reportSummary, error) {
	entries, err := os.ReadDir(s.cfg.OutDir)
	if err != nil {
		return nil, fmt.Errorf("read out dir: %w", err)
	}
	out := make([]reportSummary, 0, len(entries))
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		p := filepath.Join(s.cfg.OutDir, ent.Name(), "run_report.json")
		r, err := report.ReadMinimalRunReport(p)
		if err != nil {
			continue
		}
		out = append(out, reportSummary{
			RunID:                   r.RunID,
			StartedAt:               r.StartedAt,
			GeneratedAt:             r.GeneratedAt,
			ExecutionDurationMS:     r.ExecutionDurationMS,
			Completed:               r.Completed,
			IncidentCount:           r.IncidentCount(),
			TaosdIncidentCount:      len(r.TaosdIncidents),
			TDsqlsmithIncidentCount: len(r.TDsqlsmithIncidents),
			TotalExecuted:           r.TotalExecuted,
			QueryRuleHit:            r.QueryRuleCoverage.Hit,
			QueryRuleRequired:       r.QueryRuleCoverage.Required,
			QueryRuleCoverageRatio:  r.QueryRuleCoverage.CoverageRatio,
			QueryRuleMissingCount:   len(r.QueryRuleCoverage.Missing),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].GeneratedAt.After(out[j].GeneratedAt)
	})
	return out, nil
}

// readRunReport loads the full run report for runID, rejecting empty or path-traversal identifiers.
//
// readRunReport 加载 runID 对应的完整运行报告，拒绝空标识或带路径穿越的标识。
func (s *server) readRunReport(runID string) (*report.MinimalRunReport, error) {
	if strings.TrimSpace(runID) == "" {
		return nil, fmt.Errorf("empty run id")
	}
	if strings.Contains(runID, "..") || strings.Contains(runID, "/") || strings.Contains(runID, "\\") {
		return nil, fmt.Errorf("invalid run id")
	}
	p := filepath.Join(s.cfg.OutDir, runID, "run_report.json")
	r, err := report.ReadMinimalRunReport(p)
	if err != nil {
		return nil, fmt.Errorf("run report not found")
	}
	return r, nil
}
