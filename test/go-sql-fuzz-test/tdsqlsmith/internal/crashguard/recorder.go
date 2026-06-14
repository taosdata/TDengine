// Package crashguard records crash incidents and durable artifacts so that the
// SQL statement executing at the moment of a taosd crash can be recovered.
//
// crashguard 包记录崩溃事件及持久化产物，以便在 taosd 崩溃发生的那一刻
// 正在执行的 SQL 语句可以被恢复。
package crashguard

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	dirName            = "crash_guard"        // subdirectory under the run dir holding crash-guard files / 运行目录下存放 crash-guard 文件的子目录
	pendingFileName    = "pending.json"       // statement currently in flight (may be the crash culprit) / 当前正在执行的语句（可能是崩溃元凶）
	windowFileName     = "window.json"        // rolling window of recently executed statements / 最近已执行语句的滚动窗口
	latestFileName     = "report.latest.json" // full latest snapshot of recorder state / 记录器状态的最新完整快照
	statusFileName     = "status.json"        // terminal status written on clean exit / 干净退出时写入的终止状态
	defaultWindowLimit = 64                   // default number of statements retained in the window / 窗口中保留的默认语句数量
)

// Phase identifies which processing stage a pending statement was in.
//
// Phase 标识待处理语句所处的处理阶段。
type Phase string

const (
	PhaseParse Phase = "parse" // statement was being parsed/generated / 语句正在被解析/生成
	PhaseExec  Phase = "exec"  // statement was being executed against TDengine / 语句正在对 TDengine 执行
)

// PendingStatement captures the statement that is currently in flight, persisted
// before execution so it survives a process crash.
//
// PendingStatement 捕获当前正在执行的语句，会在执行前持久化，
// 以便在进程崩溃后仍然保留。
type PendingStatement struct {
	OccurredAt time.Time `json:"occurred_at"`         // when the statement entered the pending state / 语句进入待处理状态的时间
	RunID      string    `json:"run_id,omitempty"`    // identifier of the owning run / 所属运行的标识符
	QueryNo    int64     `json:"query_no,omitempty"`  // sequence number of the statement within the run / 语句在本次运行中的序号
	CaseID     string    `json:"case_id,omitempty"`   // identifier of the generated case / 所生成用例的标识符
	Rule       string    `json:"rule,omitempty"`      // generation rule that produced the statement / 生成该语句的生成规则
	Phase      string    `json:"phase,omitempty"`     // processing phase (parse/exec) / 处理阶段（parse/exec）
	RNGState   string    `json:"rng_state,omitempty"` // serialized RNG state for reproduction / 用于复现的序列化 RNG 状态
	SQL        string    `json:"sql"`                 // the SQL text / SQL 文本
}

// ExecutedStmt records a statement that completed execution, kept in the rolling window.
//
// ExecutedStmt 记录一条已完成执行的语句，保存在滚动窗口中。
type ExecutedStmt struct {
	QueryNo    int64     `json:"query_no"`              // sequence number within the run / 本次运行中的序号
	OccurredAt time.Time `json:"occurred_at"`           // when execution happened / 执行发生的时间
	CaseID     string    `json:"case_id,omitempty"`     // identifier of the generated case / 所生成用例的标识符
	Rule       string    `json:"rule,omitempty"`        // generation rule that produced the statement / 生成该语句的生成规则
	ExecClass  string    `json:"exec_class"`            // execution outcome classification / 执行结果分类
	SQL        string    `json:"sql"`                   // the SQL text / SQL 文本
	Error      string    `json:"error,omitempty"`       // error message, if any / 错误信息（如有）
	DurationMS int64     `json:"duration_ms,omitempty"` // execution duration in milliseconds / 执行耗时（毫秒）
}

// Snapshot is the full persisted recorder state for a run.
//
// Snapshot 是一次运行的完整持久化记录器状态。
type Snapshot struct {
	RunID         string            `json:"run_id,omitempty"`         // identifier of the owning run / 所属运行的标识符
	RunDir        string            `json:"run_dir,omitempty"`        // run output directory / 运行输出目录
	UpdatedAt     time.Time         `json:"updated_at"`               // last update timestamp / 最后更新时间戳
	WorkerPID     int               `json:"worker_pid,omitempty"`     // PID of the worker process / 工作进程的 PID
	Pending       *PendingStatement `json:"pending,omitempty"`        // statement currently in flight, if any / 当前正在执行的语句（如有）
	Window        []ExecutedStmt    `json:"window,omitempty"`         // rolling window of recent statements / 最近语句的滚动窗口
	ExecutedTotal int64             `json:"executed_total,omitempty"` // total statements that reached the exec phase / 进入执行阶段的语句总数
	CleanExit     bool              `json:"clean_exit,omitempty"`     // true once the run exited cleanly / 运行干净退出后为 true
}

// statusPayload is the terminal status file written when the run finishes cleanly.
//
// statusPayload 是运行干净结束时写入的终止状态文件。
type statusPayload struct {
	RunID      string    `json:"run_id,omitempty"`      // identifier of the owning run / 所属运行的标识符
	CleanExit  bool      `json:"clean_exit"`            // whether the run exited cleanly / 运行是否干净退出
	UpdatedAt  time.Time `json:"updated_at"`            // timestamp of the status write / 状态写入的时间戳
	WorkerPID  int       `json:"worker_pid,omitempty"`  // PID of the worker process / 工作进程的 PID
	LatestFile string    `json:"latest_file,omitempty"` // path to the latest snapshot file / 最新快照文件的路径
}

// Recorder persists pending and recently executed statements to durable files.
//
// Recorder 将待处理语句和最近执行的语句持久化到可靠文件中。
type Recorder struct {
	mu          sync.Mutex // guards latest and the file writes / 保护 latest 字段及文件写入
	dir         string     // crash-guard directory / crash-guard 目录
	pendingPath string     // path to the pending snapshot file / 待处理快照文件路径
	windowPath  string     // path to the window snapshot file / 窗口快照文件路径
	latestPath  string     // path to the latest full snapshot file / 最新完整快照文件路径
	statusPath  string     // path to the terminal status file / 终止状态文件路径
	windowLimit int        // maximum number of statements kept in the window / 窗口中保留的最大语句数量
	latest      Snapshot   // in-memory copy of the latest snapshot / 最新快照的内存副本
}

// New creates a Recorder rooted under runDir, writing the initial snapshot.
// A non-positive windowLimit falls back to defaultWindowLimit.
//
// New 创建一个以 runDir 为根目录的 Recorder，并写入初始快照。
// windowLimit 为非正数时回退为 defaultWindowLimit。
func New(runID, runDir string, windowLimit int) (*Recorder, error) {
	runDir = strings.TrimSpace(runDir)
	if runDir == "" {
		return nil, fmt.Errorf("empty run dir")
	}
	if windowLimit <= 0 {
		windowLimit = defaultWindowLimit
	}

	dir := filepath.Join(runDir, dirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create crash guard dir: %w", err)
	}
	now := time.Now()
	latest := Snapshot{
		RunID:     strings.TrimSpace(runID),
		RunDir:    runDir,
		UpdatedAt: now,
		WorkerPID: os.Getpid(),
	}
	rec := &Recorder{
		dir:         dir,
		pendingPath: filepath.Join(dir, pendingFileName),
		windowPath:  filepath.Join(dir, windowFileName),
		latestPath:  filepath.Join(dir, latestFileName),
		statusPath:  filepath.Join(dir, statusFileName),
		windowLimit: windowLimit,
		latest:      latest,
	}
	if err := rec.writeLocked(); err != nil {
		return nil, err
	}
	return rec, nil
}

// Dir returns the crash-guard directory, or "" if the recorder is nil.
//
// Dir 返回 crash-guard 目录；若 recorder 为 nil 则返回 ""。
func (r *Recorder) Dir() string {
	if r == nil {
		return ""
	}
	return r.dir
}

// LatestPath returns the path to the latest snapshot file, or "" if nil.
//
// LatestPath 返回最新快照文件的路径；若为 nil 则返回 ""。
func (r *Recorder) LatestPath() string {
	if r == nil {
		return ""
	}
	return r.latestPath
}

// Before records meta as the pending statement and persists it before the
// statement runs, so a crash mid-execution leaves the culprit on disk.
//
// Before 将 meta 记录为待处理语句，并在该语句运行前持久化，
// 使得执行中途崩溃时元凶语句仍保留在磁盘上。
func (r *Recorder) Before(meta PendingStatement) error {
	if r == nil {
		return fmt.Errorf("nil recorder")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	meta.SQL = strings.TrimSpace(meta.SQL)
	meta.CaseID = strings.TrimSpace(meta.CaseID)
	meta.Rule = strings.TrimSpace(meta.Rule)
	meta.RunID = strings.TrimSpace(meta.RunID)
	meta.Phase = strings.TrimSpace(meta.Phase)
	meta.RNGState = strings.TrimSpace(meta.RNGState)
	if meta.OccurredAt.IsZero() {
		meta.OccurredAt = time.Now()
	}
	r.latest.Pending = &PendingStatement{
		OccurredAt: meta.OccurredAt,
		RunID:      meta.RunID,
		QueryNo:    meta.QueryNo,
		CaseID:     meta.CaseID,
		Rule:       meta.Rule,
		Phase:      meta.Phase,
		RNGState:   meta.RNGState,
		SQL:        meta.SQL,
	}
	if strings.EqualFold(meta.Phase, string(PhaseExec)) {
		r.latest.ExecutedTotal++
	}
	r.latest.CleanExit = false
	r.latest.UpdatedAt = time.Now()
	return r.writeLocked()
}

// After clears the pending statement and appends rec to the rolling window,
// trimming the window to windowLimit, then persists the new state.
//
// After 清除待处理语句，并将 rec 追加到滚动窗口，
// 将窗口裁剪到 windowLimit，然后持久化新状态。
func (r *Recorder) After(rec *ExecutedStmt) error {
	if r == nil {
		return fmt.Errorf("nil recorder")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.latest.Pending = nil
	if rec != nil {
		r.latest.Window = append(r.latest.Window, *rec)
		if len(r.latest.Window) > r.windowLimit {
			r.latest.Window = append([]ExecutedStmt(nil), r.latest.Window[len(r.latest.Window)-r.windowLimit:]...)
		}
	}
	r.latest.CleanExit = false
	r.latest.UpdatedAt = time.Now()
	return r.writeLocked()
}

// MarkCleanExit flags the run as having exited cleanly and writes the terminal
// status file, signalling that no crash recovery is needed.
//
// MarkCleanExit 将运行标记为干净退出并写入终止状态文件，
// 表示无需进行崩溃恢复。
func (r *Recorder) MarkCleanExit() error {
	if r == nil {
		return fmt.Errorf("nil recorder")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.latest.Pending = nil
	r.latest.CleanExit = true
	r.latest.UpdatedAt = time.Now()
	if err := r.writeLocked(); err != nil {
		return err
	}
	return writeJSONDurable(r.statusPath, statusPayload{
		RunID:      r.latest.RunID,
		CleanExit:  true,
		UpdatedAt:  r.latest.UpdatedAt,
		WorkerPID:  r.latest.WorkerPID,
		LatestFile: r.latestPath,
	})
}

// LoadLatest reads a Snapshot from pathOrDir, which may be either the snapshot
// file path or a directory containing the latest snapshot file.
//
// LoadLatest 从 pathOrDir 读取一个 Snapshot，pathOrDir 既可以是快照文件路径，
// 也可以是包含最新快照文件的目录。
func LoadLatest(pathOrDir string) (*Snapshot, error) {
	raw := strings.TrimSpace(pathOrDir)
	if raw == "" {
		return nil, fmt.Errorf("empty path")
	}
	path := raw
	if st, err := os.Stat(raw); err == nil && st.IsDir() {
		path = filepath.Join(raw, latestFileName)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read crash latest: %w", err)
	}
	var out Snapshot
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("unmarshal crash latest: %w", err)
	}
	return &out, nil
}

// writeLocked persists the pending, window, and latest snapshots to disk.
// Callers must hold r.mu.
//
// writeLocked 将待处理、窗口和最新快照持久化到磁盘。
// 调用方必须持有 r.mu。
func (r *Recorder) writeLocked() error {
	if err := writeJSONDurable(r.pendingPath, r.latest.Pending); err != nil {
		return fmt.Errorf("write pending snapshot: %w", err)
	}
	if err := writeJSONDurable(r.windowPath, r.latest.Window); err != nil {
		return fmt.Errorf("write window snapshot: %w", err)
	}
	if err := writeJSONDurable(r.latestPath, r.latest); err != nil {
		return fmt.Errorf("write latest snapshot: %w", err)
	}
	return nil
}

// writeJSONDurable atomically writes value as indented JSON to path by writing
// a temp file, fsync-ing it, renaming into place, and syncing the parent dir.
//
// writeJSONDurable 通过写入临时文件、对其执行 fsync、重命名到目标位置，
// 并同步父目录，以缩进 JSON 形式将 value 原子写入 path。
func writeJSONDurable(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create parent dir: %w", err)
	}
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal json: %w", err)
	}
	b = append(b, '\n')

	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open temp file: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync parent dir: %w", err)
	}
	return nil
}

// syncDir fsyncs the directory so a preceding rename is durably persisted.
//
// syncDir 对目录执行 fsync，使先前的重命名得到可靠持久化。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
