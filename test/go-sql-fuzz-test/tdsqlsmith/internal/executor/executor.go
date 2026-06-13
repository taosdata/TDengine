package executor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

type Class string

const (
	ClassOK       Class = "ok"
	ClassDBError  Class = "db_error"
	ClassTimeout  Class = "timeout"
	ClassConnLost Class = "conn_lost"
	ClassFatal    Class = "fatal"
)

type Output struct {
	Class    Class
	Err      error
	Duration time.Duration
}

type QueryResult struct {
	Columns []string
	Rows    [][]string
}

type Executor struct {
	mu        sync.Mutex
	db        *sql.DB
	currentDB string
}

func New(ctx context.Context, dsn string) (*Executor, error) {
	db, err := sql.Open("taosSql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open TDengine: %w", err)
	}
	// Keep a single session so `USE <db>` remains effective for all statements.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	// Disable forced pool recycling; reconnect is handled explicitly on conn-lost.
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping TDengine: %w", err)
	}
	return &Executor{db: db}, nil
}

func (e *Executor) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e == nil || e.db == nil {
		return nil
	}
	err := e.db.Close()
	e.db = nil
	return err
}

func (e *Executor) Exec(ctx context.Context, sqlText string) Output {
	start := time.Now()
	db, currentDB := e.snapshot()
	if e == nil || db == nil {
		return Output{Class: ClassFatal, Err: errors.New("nil executor"), Duration: time.Since(start)}
	}

	outErr := execOnce(db, ctx, sqlText)
	if shouldRetryWithDatabase(outErr, currentDB, sqlText) {
		if useErr := e.UseDatabase(ctx, currentDB); useErr == nil {
			db, _ = e.snapshot()
			if db != nil {
				outErr = execOnce(db, ctx, sqlText)
			}
		}
	}
	if outErr != nil {
		return Output{Class: classifyExecErr(ctx, outErr), Err: outErr, Duration: time.Since(start)}
	}

	if dbName := parseUseDatabase(sqlText); dbName != "" {
		e.mu.Lock()
		e.currentDB = dbName
		e.mu.Unlock()
	}
	return Output{Class: ClassOK, Duration: time.Since(start)}
}

func (e *Executor) QueryRows(ctx context.Context, sqlText string, maxRows int) (QueryResult, error) {
	db, currentDB := e.snapshot()
	if e == nil || db == nil {
		return QueryResult{}, errors.New("nil executor")
	}

	rows, err := db.QueryContext(ctx, sqlText)
	if err != nil {
		if shouldRetryWithDatabase(err, currentDB, sqlText) {
			if useErr := e.UseDatabase(ctx, currentDB); useErr == nil {
				db, _ = e.snapshot()
				if db != nil {
					rows, err = db.QueryContext(ctx, sqlText)
				}
			}
		}
	}
	if err != nil {
		return QueryResult{}, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return QueryResult{}, err
	}
	out := QueryResult{Columns: cols, Rows: make([][]string, 0, 32)}
	scanVals := make([]any, len(cols))
	scanPtrs := make([]any, len(cols))
	for i := range scanVals {
		scanPtrs[i] = &scanVals[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			return QueryResult{}, err
		}
		rec := make([]string, len(cols))
		for i := range scanVals {
			rec[i] = stringifyCell(scanVals[i])
		}
		out.Rows = append(out.Rows, rec)
		if maxRows > 0 && len(out.Rows) >= maxRows {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return QueryResult{}, err
	}
	return out, nil
}

func (e *Executor) Reconnect(ctx context.Context, dsn string) error {
	if e == nil {
		return errors.New("nil executor")
	}
	_, currentDB := e.snapshot()
	next, err := New(ctx, dsn)
	if err != nil {
		return err
	}
	if currentDB != "" {
		if err := next.UseDatabase(ctx, currentDB); err != nil {
			_ = next.Close()
			return err
		}
	}
	e.mu.Lock()
	old := e.db
	e.db = next.db
	e.currentDB = currentDB
	e.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}

func (e *Executor) UseDatabase(ctx context.Context, dbName string) error {
	dbName = strings.TrimSpace(dbName)
	if dbName == "" {
		return nil
	}
	db, _ := e.snapshot()
	if db == nil {
		return errors.New("nil executor")
	}
	if _, err := db.ExecContext(ctx, "use "+dbName); err != nil {
		return err
	}
	e.mu.Lock()
	e.currentDB = dbName
	e.mu.Unlock()
	return nil
}

func (e *Executor) snapshot() (*sql.DB, string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.db, e.currentDB
}

func isQuerySQL(sqlText string) bool {
	s := strings.TrimSpace(strings.ToLower(sqlText))
	return strings.HasPrefix(s, "select") ||
		strings.HasPrefix(s, "show") ||
		strings.HasPrefix(s, "describe") ||
		strings.HasPrefix(s, "explain")
}

func execOnce(db *sql.DB, ctx context.Context, sqlText string) error {
	if !isQuerySQL(sqlText) {
		_, err := db.ExecContext(ctx, sqlText)
		return err
	}
	rows, err := db.QueryContext(ctx, sqlText)
	if err != nil {
		return err
	}
	if rows != nil {
		_ = rows.Close()
	}
	return nil
}

func shouldRetryWithDatabase(err error, currentDB, sqlText string) bool {
	if err == nil || strings.TrimSpace(currentDB) == "" {
		return false
	}
	if !isDatabaseNotSpecified(err) {
		return false
	}
	if parseUseDatabase(sqlText) != "" {
		return false
	}
	return true
}

func isDatabaseNotSpecified(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "database not specified")
}

func parseUseDatabase(sqlText string) string {
	raw := strings.TrimSpace(sqlText)
	if raw == "" {
		return ""
	}
	low := strings.ToLower(raw)
	if !strings.HasPrefix(low, "use ") {
		return ""
	}
	rest := strings.TrimSpace(raw[3:])
	rest = strings.TrimSuffix(rest, ";")
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return ""
	}
	name := strings.TrimSpace(fields[0])
	name = strings.Trim(name, "`")
	name = strings.Trim(name, `"`)
	name = strings.Trim(name, `'`)
	return name
}

func classifyExecErr(ctx context.Context, err error) Class {
	if err == nil {
		return ClassOK
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return ClassTimeout
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "broken pipe") || strings.Contains(msg, "connection") || strings.Contains(msg, "closed network") || strings.Contains(msg, "eof") {
		return ClassConnLost
	}
	return ClassDBError
}

func stringifyCell(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(x)
	}
}
