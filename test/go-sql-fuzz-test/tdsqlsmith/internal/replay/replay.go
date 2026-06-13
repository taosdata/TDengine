package replay

import (
	"context"
	"fmt"
	"strings"
	"time"

	"tdsqlsmith/internal/executor"
	"tdsqlsmith/internal/parsergate"
	"tdsqlsmith/internal/report"
)

type Config struct {
	DSN         string
	File        string
	Count       int
	StmtTimeout time.Duration
}

type Outcome struct {
	Class    string        `json:"class"`
	Duration time.Duration `json:"duration"`
	Err      string        `json:"err"`
}

type Result struct {
	File       string        `json:"file"`
	IncidentID string        `json:"incident_id,omitempty"`
	SetupCount int           `json:"setup_count,omitempty"`
	Count      int           `json:"count"`
	Duration   time.Duration `json:"duration"`
	Outcomes   []Outcome     `json:"outcomes"`
}

func Run(ctx context.Context, cfg Config) (*Result, error) {
	if cfg.Count <= 0 {
		cfg.Count = 1
	}
	if cfg.StmtTimeout <= 0 {
		cfg.StmtTimeout = 2 * time.Second
	}

	mini, err := report.ReadMinimalRunReport(cfg.File)
	if err != nil {
		return nil, fmt.Errorf("read run report: %w", err)
	}
	sqlText, incidentID, err := selectReplayIncidentSQL(mini)
	if err != nil {
		return nil, err
	}
	if pg := parsergate.Parse(sqlText); pg.Err != nil {
		return nil, fmt.Errorf("run report incident sql parse failed: %w", pg.Err)
	}

	exec, err := executor.New(ctx, cfg.DSN)
	if err != nil {
		return nil, err
	}
	defer exec.Close()

	setupCount, err := applySetupSQL(ctx, exec, mini.SetupSQL, cfg.StmtTimeout)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	res := &Result{
		File:       cfg.File,
		IncidentID: incidentID,
		SetupCount: setupCount,
		Count:      cfg.Count,
		Outcomes:   make([]Outcome, 0, cfg.Count),
	}
	for i := 0; i < cfg.Count; i++ {
		runCtx, cancel := context.WithTimeout(ctx, cfg.StmtTimeout)
		out := exec.Exec(runCtx, sqlText)
		cancel()
		res.Outcomes = append(res.Outcomes, Outcome{Class: string(out.Class), Duration: out.Duration, Err: errString(out.Err)})
	}
	res.Duration = time.Since(start)
	return res, nil
}

func selectReplayIncidentSQL(mini *report.MinimalRunReport) (sql string, incidentID string, err error) {
	if mini == nil {
		return "", "", fmt.Errorf("run report is nil")
	}

	type candidate struct {
		sql        string
		incidentID string
		occurredAt time.Time
	}
	var (
		best  candidate
		found bool
	)
	pick := func(items []report.CrashIncident) {
		for _, item := range items {
			s := strings.TrimSpace(item.CrashSQL)
			if s == "" {
				continue
			}
			c := candidate{
				sql:        s,
				incidentID: strings.TrimSpace(item.IncidentID),
				occurredAt: item.OccurredAt,
			}
			if !found || c.occurredAt.After(best.occurredAt) || c.occurredAt.Equal(best.occurredAt) {
				best = c
				found = true
			}
		}
	}

	pick(mini.TaosdIncidents)
	pick(mini.TDsqlsmithIncidents)
	if !found {
		return "", "", fmt.Errorf("run report has no replayable crash_sql in taosd_incidents/tdsqlsmith_incidents")
	}
	return best.sql, best.incidentID, nil
}

func applySetupSQL(ctx context.Context, exec *executor.Executor, setupSQL []string, stmtTimeout time.Duration) (int, error) {
	if len(setupSQL) == 0 {
		return 0, nil
	}
	count := 0
	for i, sqlText := range setupSQL {
		stmt := strings.TrimSpace(sqlText)
		if stmt == "" {
			continue
		}
		runCtx, cancel := context.WithTimeout(ctx, stmtTimeout)
		out := exec.Exec(runCtx, stmt)
		cancel()
		if out.Err != nil {
			return count, fmt.Errorf("run report setup_sql[%d] failed class=%s: %w", i, out.Class, out.Err)
		}
		count++
	}
	return count, nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
