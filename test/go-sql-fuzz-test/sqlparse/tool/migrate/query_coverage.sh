#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
out_dir="$repo_root/reports/query"
cover_profile="${COVER_PROFILE:-/tmp/query.cover.out}"
filtered_profile="${COVER_PROFILE_FILTERED:-/tmp/query.cover.filtered.out}"
threshold="${QUERY_COVERAGE_THRESHOLD:-100}"

mkdir -p "$out_dir"

cd "$repo_root"

GOCACHE="${GOCACHE:-/tmp/go-build}" \
GOMODCACHE="${GOMODCACHE:-/tmp/gomodcache}" \
GOPATH="${GOPATH:-/tmp/gopath}" \
go test ./... -count=1 -coverprofile="$cover_profile" >/tmp/query_cover_test.log

{
  head -n 1 "$cover_profile"
  tail -n +2 "$cover_profile" | awk -F: '
    {
      path=$1
      gsub(/^[ \t]+|[ \t]+$/, "", path)
      if (path ~ /^sqlparser\//) {
        sub(/^sqlparser\//, "", path)
      }
      if (path ~ /^\//) {
        cmd = "test -f \"" path "\""
        if (system(cmd) == 0) {
          sub($1, path, $0)
          print $0
        }
      } else if (path ~ /^[A-Za-z0-9_./-]+$/) {
        cmd = "test -f \"" path "\""
        if (system(cmd) == 0) {
          sub($1, path, $0)
          print $0
        }
      }
    }
  '
} > "$filtered_profile"

query_file_re='(sql.go|td_sql.y|stmt_select.go|stmt_select_stub.go|lexer.go|expr_alias.go|expr_colident.go|expr_star.go)'

summary_md="$out_dir/coverage_summary.md"

total_cov_line="$(awk -v re="$query_file_re" '
  NR == 1 { next }
  {
    split($1, a, ":")
    file = a[1]
    stmts = $2 + 0
    cnt = $3 + 0
    if (file ~ re) {
      total_stmts += stmts
      if (cnt > 0) {
        covered_stmts += stmts
      }
    }
  }
  END {
    if (total_stmts == 0) {
      print "0 0.00"
    } else {
      printf "%d %.2f\n", total_stmts, (covered_stmts * 100.0) / total_stmts
    }
  }
' "$filtered_profile")"

total_stmts="$(echo "$total_cov_line" | awk '{print $1}')"
weighted_pct="$(echo "$total_cov_line" | awk '{print $2}')"

{
  echo "# Query Coverage Summary"
  echo
  echo "Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo
  echo "Threshold: ${threshold}%"
  echo
  echo "## Weighted Coverage (query-related files)"
  echo "- Statements: $total_stmts"
  echo "- Coverage: ${weighted_pct}%"
  echo
  echo "## File Coverage"
  echo '```'
  awk -v re="$query_file_re" '
    NR == 1 { next }
    {
      split($1, a, ":")
      file = a[1]
      stmts = $2 + 0
      cnt = $3 + 0
      if (file ~ re) {
        file_total[file] += stmts
        if (cnt > 0) {
          file_cov[file] += stmts
        }
      }
    }
    END {
      for (f in file_total) {
        pct = 0
        if (file_total[f] > 0) {
          pct = (file_cov[f] * 100.0) / file_total[f]
        }
        printf "%s %.2f%% (%d/%d)\n", f, pct, file_cov[f], file_total[f]
      }
    }
  ' "$filtered_profile" | sort
  echo '```'
  echo
  echo "## Test Output"
  echo '```'
  cat /tmp/query_cover_test.log
  echo '```'
} > "$summary_md"

awk -v cur="$weighted_pct" -v thr="$threshold" 'BEGIN { exit !(cur+0 >= thr+0) }'

echo "query coverage report generated: $summary_md"
