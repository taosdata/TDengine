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
	dirName            = "crash_guard"
	pendingFileName    = "pending.json"
	windowFileName     = "window.json"
	latestFileName     = "report.latest.json"
	statusFileName     = "status.json"
	defaultWindowLimit = 64
)

type Phase string

const (
	PhaseParse Phase = "parse"
	PhaseExec  Phase = "exec"
)

type PendingStatement struct {
	OccurredAt time.Time `json:"occurred_at"`
	RunID      string    `json:"run_id,omitempty"`
	QueryNo    int64     `json:"query_no,omitempty"`
	CaseID     string    `json:"case_id,omitempty"`
	Rule       string    `json:"rule,omitempty"`
	Phase      string    `json:"phase,omitempty"`
	RNGState   string    `json:"rng_state,omitempty"`
	SQL        string    `json:"sql"`
}

type ExecutedStmt struct {
	QueryNo    int64     `json:"query_no"`
	OccurredAt time.Time `json:"occurred_at"`
	CaseID     string    `json:"case_id,omitempty"`
	Rule       string    `json:"rule,omitempty"`
	ExecClass  string    `json:"exec_class"`
	SQL        string    `json:"sql"`
	Error      string    `json:"error,omitempty"`
	DurationMS int64     `json:"duration_ms,omitempty"`
}

type Snapshot struct {
	RunID         string            `json:"run_id,omitempty"`
	RunDir        string            `json:"run_dir,omitempty"`
	UpdatedAt     time.Time         `json:"updated_at"`
	WorkerPID     int               `json:"worker_pid,omitempty"`
	Pending       *PendingStatement `json:"pending,omitempty"`
	Window        []ExecutedStmt    `json:"window,omitempty"`
	ExecutedTotal int64             `json:"executed_total,omitempty"`
	CleanExit     bool              `json:"clean_exit,omitempty"`
}

type statusPayload struct {
	RunID      string    `json:"run_id,omitempty"`
	CleanExit  bool      `json:"clean_exit"`
	UpdatedAt  time.Time `json:"updated_at"`
	WorkerPID  int       `json:"worker_pid,omitempty"`
	LatestFile string    `json:"latest_file,omitempty"`
}

type Recorder struct {
	mu          sync.Mutex
	dir         string
	pendingPath string
	windowPath  string
	latestPath  string
	statusPath  string
	windowLimit int
	latest      Snapshot
}

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

func (r *Recorder) Dir() string {
	if r == nil {
		return ""
	}
	return r.dir
}

func (r *Recorder) LatestPath() string {
	if r == nil {
		return ""
	}
	return r.latestPath
}

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

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
