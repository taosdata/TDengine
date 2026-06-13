package serve

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"tdsqlsmith/internal/report"
)

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
