package coverage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"tdsqlsmith/internal/report"
)

func WriteMarkdown(in *report.RunReport, outPath string) (string, error) {
	if in == nil {
		return "", fmt.Errorf("nil run report")
	}
	if outPath == "" {
		outPath = filepath.Join(in.OutDir, "coverage.md")
	}
	md := renderMarkdown(in)
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return "", fmt.Errorf("create output dir: %w", err)
	}
	if err := os.WriteFile(outPath, []byte(md), 0o644); err != nil {
		return "", fmt.Errorf("write markdown: %w", err)
	}
	return outPath, nil
}

func renderMarkdown(in *report.RunReport) string {
	cov := in.Coverage
	b := &strings.Builder{}
	fmt.Fprintf(b, "# TDengine SQLsmith Coverage Report\n\n")
	fmt.Fprintf(b, "- Run ID: `%s`\n", in.RunID)
	fmt.Fprintf(b, "- Seed: `%d`\n", in.Seed)
	fmt.Fprintf(b, "- Duration: `%s`\n", in.Duration)
	fmt.Fprintf(b, "- Positive Coverage: **%d/%d (%.2f%%)**\n", cov.Hit, cov.Required, cov.CoverageRatio*100)
	fmt.Fprintf(b, "- Negative Reject Coverage: **%d/%d (%.2f%%)**\n\n", cov.HitNeg, cov.RequiredNeg, cov.NegRejectRatio*100)
	qrc := in.QueryRuleCoverage
	if qrc.Required > 0 {
		fmt.Fprintf(b, "- Query Rule Coverage: **%d/%d (%.2f%%)**\n\n", qrc.Hit, qrc.Required, qrc.CoverageRatio*100)
	}

	fmt.Fprintf(b, "## Missing Positive Cases\n")
	if len(cov.Missing) == 0 {
		fmt.Fprintf(b, "- none\n")
	} else {
		for _, id := range cov.Missing {
			fmt.Fprintf(b, "- %s\n", id)
		}
	}

	fmt.Fprintf(b, "\n## Missing Negative Cases\n")
	if len(cov.MissingNeg) == 0 {
		fmt.Fprintf(b, "- none\n")
	} else {
		for _, id := range cov.MissingNeg {
			fmt.Fprintf(b, "- %s\n", id)
		}
	}

	if qrc.Required > 0 {
		fmt.Fprintf(b, "\n## Missing Query Rules\n")
		if len(qrc.Missing) == 0 {
			fmt.Fprintf(b, "- none\n")
		} else {
			for _, id := range qrc.Missing {
				fmt.Fprintf(b, "- %s\n", id)
			}
		}
	}

	fmt.Fprintf(b, "\n## Top Errors\n")
	if len(in.TopErrors) == 0 {
		fmt.Fprintf(b, "- none\n")
	} else {
		for _, e := range in.TopErrors {
			fmt.Fprintf(b, "- `%d` %s\n", e.Count, e.Message)
		}
	}

	fmt.Fprintf(b, "\n## Crash Guard\n")
	if strings.TrimSpace(in.CrashGuardDir) == "" && strings.TrimSpace(in.CrashLatestReport) == "" && strings.TrimSpace(in.SupervisorCrashReport) == "" {
		fmt.Fprintf(b, "- none\n")
	} else {
		if strings.TrimSpace(in.CrashGuardDir) != "" {
			fmt.Fprintf(b, "- dir: `%s`\n", strings.TrimSpace(in.CrashGuardDir))
		}
		if strings.TrimSpace(in.CrashLatestReport) != "" {
			fmt.Fprintf(b, "- latest: `%s`\n", strings.TrimSpace(in.CrashLatestReport))
		}
		if strings.TrimSpace(in.SupervisorCrashReport) != "" {
			fmt.Fprintf(b, "- supervisor_report: `%s`\n", strings.TrimSpace(in.SupervisorCrashReport))
		}
	}

	fmt.Fprintf(b, "\n## TAOSD Coredump Statements\n")
	if len(in.CoredumpStatements) == 0 {
		fmt.Fprintf(b, "- none\n")
	} else {
		limit := len(in.CoredumpStatements)
		if limit > 10 {
			limit = 10
		}
		for i := 0; i < limit; i++ {
			s := in.CoredumpStatements[i]
			fmt.Fprintf(b, "- `%s` incident=`%s` q=`%d` case=`%s` rule=`%s` class=`%s`\n", s.OccurredAt.Format(time.RFC3339), s.IncidentID, s.QueryNo, s.CaseID, s.Rule, s.ExecClass)
			if strings.TrimSpace(s.CoredumpEvidence) != "" {
				fmt.Fprintf(b, "  - evidence: %s\n", s.CoredumpEvidence)
			}
			if strings.TrimSpace(s.FailureID) != "" {
				fmt.Fprintf(b, "  - failure: %s\n", s.FailureID)
			}
			if strings.TrimSpace(s.Error) != "" {
				fmt.Fprintf(b, "  - error: %s\n", s.Error)
			}
			if len(s.PrecedingWindow) > 0 {
				fmt.Fprintf(b, "  - preceding:\n")
				for _, p := range s.PrecedingWindow {
					fmt.Fprintf(b, "    - q%d `%s` %s\n", p.QueryNo, p.ExecClass, p.OccurredAt.Format(time.RFC3339))
				}
			}
			candidate := strings.TrimSpace(s.CandidateSQL)
			if candidate == "" {
				candidate = strings.TrimSpace(s.SQL)
			}
			fmt.Fprintf(b, "```sql\n%s\n```\n", candidate)
		}
	}

	fmt.Fprintf(b, "\n## Query Combos\n")
	if len(in.QueryComboCounts) == 0 {
		fmt.Fprintf(b, "- none\n")
	} else {
		type kv struct {
			key string
			val int64
		}
		arr := make([]kv, 0, len(in.QueryComboCounts))
		for k, v := range in.QueryComboCounts {
			arr = append(arr, kv{key: k, val: v})
		}
		sort.Slice(arr, func(i, j int) bool {
			if arr[i].val == arr[j].val {
				return arr[i].key < arr[j].key
			}
			return arr[i].val > arr[j].val
		})
		limit := len(arr)
		if limit > 20 {
			limit = 20
		}
		for i := 0; i < limit; i++ {
			fmt.Fprintf(b, "- `%d` %s\n", arr[i].val, arr[i].key)
		}
	}
	return b.String()
}
