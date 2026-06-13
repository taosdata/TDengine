package branchmodel

import "time"

type PositiveCase struct {
	ID        string
	Rule      string
	BranchSig string
	SQL       string
	KeyAssert string
	Source    string
}

type NegativeCase struct {
	ID      string
	Rule    string
	SQL     string
	ErrType string
}

type Corpus struct {
	Positive []PositiveCase
	Negative []NegativeCase
	WriteSQL []string
}

type HitInfo struct {
	CaseID    string    `json:"case_id"`
	SQL       string    `json:"sql"`
	At        time.Time `json:"at"`
	Rule      string    `json:"rule"`
	BranchSig string    `json:"branch_sig,omitempty"`
	Source    string    `json:"source,omitempty"`
}

type CoverageSummary struct {
	Required       int      `json:"required"`
	Hit            int      `json:"hit"`
	Missing        []string `json:"missing"`
	RequiredNeg    int      `json:"required_negative"`
	HitNeg         int      `json:"hit_negative"`
	MissingNeg     []string `json:"missing_negative"`
	CoverageRatio  float64  `json:"coverage_ratio"`
	NegRejectRatio float64  `json:"negative_reject_ratio"`
}
