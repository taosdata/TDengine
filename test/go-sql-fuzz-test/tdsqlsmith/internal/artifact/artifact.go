package artifact

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type FailureArtifact struct {
	RunID                 string    `json:"run_id"`
	QueryNo               int64     `json:"query_no,omitempty"`
	CaseID                string    `json:"case_id"`
	Rule                  string    `json:"rule"`
	IncidentID            string    `json:"incident_id,omitempty"`
	SQL                   string    `json:"sql"`
	Class                 string    `json:"class"`
	Error                 string    `json:"error"`
	DurationMS            int64     `json:"duration_ms"`
	OccurredAt            time.Time `json:"occurred_at"`
	ParseErrType          string    `json:"parse_err_type,omitempty"`
	MatchedCases          []string  `json:"matched_cases,omitempty"`
	Mutation              bool      `json:"mutation"`
	ExpectedCase          string    `json:"expected_case,omitempty"`
	NegativeCase          bool      `json:"negative_case"`
	NegativeError         string    `json:"negative_expected_error,omitempty"`
	TaosdCheck            bool      `json:"taosd_check,omitempty"`
	TaosdProcessExists    bool      `json:"taosd_process_exists,omitempty"`
	TaosdProcessCheck     string    `json:"taosd_process_check,omitempty"`
	TaosdExitReason       string    `json:"taosd_exit_reason,omitempty"`
	TaosdRestartAttempted bool      `json:"taosd_restart_attempted,omitempty"`
	TaosdRestartCommand   string    `json:"taosd_restart_command,omitempty"`
	TaosdRestartSucceeded bool      `json:"taosd_restart_succeeded,omitempty"`
	TaosdRestartError     string    `json:"taosd_restart_error,omitempty"`
	TaosdCoredump         bool      `json:"taosd_coredump,omitempty"`
	TaosdCoredumpEvidence string    `json:"taosd_coredump_evidence,omitempty"`
}

func FileName(seq int64) string {
	return fmt.Sprintf("failure_%08d.json", seq)
}

func Write(path string, item FailureArtifact) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create dir: %w", err)
	}
	b, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal artifact: %w", err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("write artifact: %w", err)
	}
	return nil
}

func Read(path string) (*FailureArtifact, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read artifact: %w", err)
	}
	var out FailureArtifact
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("unmarshal artifact: %w", err)
	}
	return &out, nil
}
