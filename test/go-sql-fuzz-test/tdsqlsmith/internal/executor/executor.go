// Package executor runs generated SQL statements against TDengine via the
// driver-go taosSql driver, classifying outcomes and handling reconnects.
//
// executor 包通过 driver-go 的 taosSql 驱动对 TDengine 执行生成的 SQL 语句，
// 对结果进行分类并处理重连。
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

// Class is the classification of a statement execution outcome.
//
// Class 是语句执行结果的分类。
type Class string

const (
	ClassOK       Class = "ok"        // statement executed successfully / 语句执行成功
	ClassDBError  Class = "db_error"  // database returned an error / 数据库返回错误
	ClassTimeout  Class = "timeout"   // statement exceeded its deadline / 语句超过了截止时间
	ClassConnLost Class = "conn_lost" // connection to the server was lost / 与服务器的连接丢失
	ClassFatal    Class = "fatal"     // unrecoverable executor error / 不可恢复的执行器错误
)

// Output is the result of executing a single statement.
//
// Output 是执行单条语句的结果。
type Output struct {
	Class    Class         // outcome classification / 结果分类
	Err      error         // underlying error, if any / 底层错误（如有）
	Duration time.Duration // wall-clock execution time / 挂钟执行时间
}

// QueryResult holds the column names and stringified rows of a query.
//
// QueryResult 保存查询的列名和字符串化的行。
type QueryResult struct {
	Columns []string   // column names in order / 按顺序排列的列名
	Rows    [][]string // row values, each cell stringified / 行值，每个单元格已字符串化
}

// Executor wraps a single-session TDengine connection and tracks the active database.
//
// Executor 封装单会话的 TDengine 连接，并跟踪当前数据库。
type Executor struct {
	mu        sync.Mutex // guards db and currentDB / 保护 db 和 currentDB
	db        *sql.DB    // underlying database handle (single connection) / 底层数据库句柄（单连接）
	currentDB string     // database selected via the most recent USE / 通过最近一次 USE 选定的数据库
}

// New opens a TDengine connection for the given DSN and verifies it with a ping.
// The pool is pinned to a single connection so USE <db> stays effective across
// statements, and forced recycling is disabled since reconnects are explicit.
//
// New 为给定 DSN 打开一个 TDengine 连接，并通过 ping 验证。
// 连接池被固定为单个连接，使得 USE <db> 在多条语句间保持有效，
// 并且由于重连是显式进行的，禁用了强制回收。
func New(ctx context.Context, dsn string) (*Executor, error) {
	db, err := sql.Open("taosSql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open TDengine: %w", err)
	}
	// Keep a single session so `USE <db>` remains effective for all statements.
	// 保持单个会话，使 `USE <db>` 对所有语句持续有效。
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	// Disable forced pool recycling; reconnect is handled explicitly on conn-lost.
	// 禁用强制连接池回收；连接丢失时显式处理重连。
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

// Close closes the underlying database handle and is safe to call on a nil
// executor or after the handle has already been released.
//
// Close 关闭底层数据库句柄，对 nil 执行器或句柄已被释放后调用都是安全的。
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

// Exec runs a single statement and returns its classified Output. If the server
// reports a missing database, it re-selects the tracked database once and
// retries. A successful USE statement updates the tracked current database.
//
// Exec 执行单条语句并返回其分类后的 Output。如果服务器报告数据库缺失，
// 它会重新选定所跟踪的数据库一次并重试。成功的 USE 语句会更新所跟踪的当前数据库。
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

// QueryRows runs a query and returns up to maxRows rows with their column names,
// stringifying each cell. A non-positive maxRows reads all rows. It re-selects
// the tracked database and retries once if the server reports it as missing.
//
// QueryRows 执行查询并返回最多 maxRows 行及其列名，并将每个单元格字符串化。
// maxRows 为非正数时读取所有行。如果服务器报告数据库缺失，
// 它会重新选定所跟踪的数据库并重试一次。
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

// Reconnect opens a fresh connection for dsn, restores the previously selected
// database on it, then atomically swaps it in and closes the old handle.
//
// Reconnect 为 dsn 打开一个新连接，在其上恢复先前选定的数据库，
// 然后原子地替换进来并关闭旧句柄。
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

// UseDatabase issues USE dbName and records it as the tracked current database.
// An empty name is a no-op.
//
// UseDatabase 发出 USE dbName 并将其记录为所跟踪的当前数据库。
// 空名称为空操作。
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

// snapshot returns the current db handle and tracked database under the lock.
//
// snapshot 在持有锁的情况下返回当前 db 句柄和所跟踪的数据库。
func (e *Executor) snapshot() (*sql.DB, string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.db, e.currentDB
}

// isQuerySQL reports whether sqlText is a read statement (select/show/describe/explain)
// that should be run via Query rather than Exec.
//
// isQuerySQL 报告 sqlText 是否为读语句（select/show/describe/explain），
// 此类语句应通过 Query 而非 Exec 执行。
func isQuerySQL(sqlText string) bool {
	s := strings.TrimSpace(strings.ToLower(sqlText))
	return strings.HasPrefix(s, "select") ||
		strings.HasPrefix(s, "show") ||
		strings.HasPrefix(s, "describe") ||
		strings.HasPrefix(s, "explain")
}

// execOnce runs sqlText once, dispatching to Query for read statements (and
// discarding their rows) and to Exec for everything else.
//
// execOnce 执行一次 sqlText：读语句分派给 Query（并丢弃其行），
// 其余语句分派给 Exec。
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

// shouldRetryWithDatabase reports whether err is a "database not specified"
// failure that can be retried after re-selecting currentDB, given that sqlText
// is not itself a USE statement.
//
// shouldRetryWithDatabase 报告 err 是否为可在重新选定 currentDB 后重试的
// "database not specified" 失败，前提是 sqlText 本身不是 USE 语句。
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

// isDatabaseNotSpecified reports whether err is TDengine's "database not specified" error.
//
// isDatabaseNotSpecified 报告 err 是否为 TDengine 的 "database not specified" 错误。
func isDatabaseNotSpecified(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "database not specified")
}

// parseUseDatabase extracts the database name from a USE statement, stripping a
// trailing semicolon and surrounding quotes; it returns "" for non-USE input.
//
// parseUseDatabase 从 USE 语句中提取数据库名，去除尾部分号和两侧引号；
// 对非 USE 输入返回 ""。
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

// classifyExecErr maps an execution error to a Class, distinguishing timeouts,
// connection loss, and generic database errors.
//
// classifyExecErr 将执行错误映射为一个 Class，区分超时、连接丢失
// 和通用数据库错误。
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

// stringifyCell converts a scanned cell value to its string form, rendering nil
// as "" and byte slices as their string contents.
//
// stringifyCell 将扫描得到的单元格值转换为字符串形式，将 nil 渲染为 ""，
// 将字节切片渲染为其字符串内容。
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
