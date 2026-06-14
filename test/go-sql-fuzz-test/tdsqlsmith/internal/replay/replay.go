// Package replay re-executes recorded SQL crash cases from a run report against a database to reproduce incidents.
//
// Package replay 针对数据库重新执行运行报告中记录的 SQL 崩溃用例，以复现事件。
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

// Config holds the settings for a replay session.
//
// Config 持有一次重放会话的设置。
type Config struct {
	DSN         string        // data source name of the target database / 目标数据库的数据源名称
	File        string        // path to the run report JSON file to replay / 待重放的运行报告 JSON 文件路径
	Count       int           // number of times to re-execute the crash statement / 重新执行崩溃语句的次数
	StmtTimeout time.Duration // per-statement execution timeout / 单条语句的执行超时
}

// Outcome records the result of a single statement execution during replay.
//
// Outcome 记录重放期间单条语句执行的结果。
type Outcome struct {
	Class    string        `json:"class"`    // classification of the execution result / 执行结果的分类
	Duration time.Duration `json:"duration"` // time taken to execute the statement / 执行该语句所耗费的时间
	Err      string        `json:"err"`      // error message, empty when successful / 错误信息，成功时为空
}

// Result summarizes the full replay run, including each individual outcome.
//
// Result 汇总整次重放运行，包括每一条独立的执行结果。
type Result struct {
	File       string        `json:"file"`                  // run report file that was replayed / 被重放的运行报告文件
	IncidentID string        `json:"incident_id,omitempty"` // identifier of the replayed incident / 被重放事件的标识
	SetupCount int           `json:"setup_count,omitempty"` // number of setup statements applied / 已应用的初始化语句数量
	Count      int           `json:"count"`                 // number of crash-statement executions attempted / 尝试执行崩溃语句的次数
	Duration   time.Duration `json:"duration"`              // total wall-clock duration of the replay / 重放的总挂钟时长
	Outcomes   []Outcome     `json:"outcomes"`              // per-execution outcomes / 每次执行的结果
}

// Run replays the most recent crash SQL from the run report referenced by cfg.File.
// It applies the report's setup SQL, then executes the crash statement cfg.Count times,
// returning a Result describing each outcome.
//
// Run 重放 cfg.File 所指向运行报告中最近一次的崩溃 SQL。
// 它先应用报告中的初始化 SQL，再将崩溃语句执行 cfg.Count 次，
// 并返回描述每次执行结果的 Result。
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

// selectReplayIncidentSQL picks the most recently occurred incident with a non-empty crash SQL
// from the report's taosd and tdsqlsmith incident lists, returning its SQL and incident id.
//
// selectReplayIncidentSQL 从报告的 taosd 与 tdsqlsmith 事件列表中，挑选最近发生且崩溃 SQL 非空的事件，
// 返回其 SQL 与事件 id。
func selectReplayIncidentSQL(mini *report.MinimalRunReport) (sql string, incidentID string, err error) {
	if mini == nil {
		return "", "", fmt.Errorf("run report is nil")
	}

	// candidate is the most recent replayable crash statement seen so far.
	//
	// candidate 是目前为止见到的最近一条可重放的崩溃语句。
	type candidate struct {
		sql        string    // crash SQL text / 崩溃 SQL 文本
		incidentID string    // originating incident id / 来源事件 id
		occurredAt time.Time // when the incident occurred / 事件发生的时间
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

// applySetupSQL executes each non-empty setup statement in order, returning the count applied
// and aborting on the first execution error.
//
// applySetupSQL 按顺序执行每条非空的初始化语句，返回已应用的数量，
// 并在首次执行出错时中止。
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

// errString returns the message of err, or an empty string when err is nil.
//
// errString 返回 err 的信息，当 err 为 nil 时返回空字符串。
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
