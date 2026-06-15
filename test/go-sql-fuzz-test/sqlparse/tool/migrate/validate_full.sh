#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
out_dir="$repo_root/reports/final"
mkdir -p "$out_dir"

cd "$repo_root"

goyacc_cmd="${GOYACC_BIN:-}"
if [[ -z "$goyacc_cmd" ]]; then
  if command -v goyacc >/dev/null 2>&1; then
    goyacc_cmd="$(command -v goyacc)"
  elif [[ -x /tmp/bin/goyacc ]]; then
    goyacc_cmd="/tmp/bin/goyacc"
  else
    goyacc_cmd="goyacc"
  fi
fi

{
  echo "# Full Validation Summary"
  echo
  echo "## Timestamp"
  date -u +"%Y-%m-%dT%H:%M:%SZ"
  echo
  echo "## Commands"
  echo "- $goyacc_cmd -o sql.go -v y.output td_sql.y"
  echo "- GOCACHE=/tmp/go-build GOMODCACHE=/tmp/gomodcache go test ./... -count=1"
  echo "- tool/migrate/check_precedence_parity.sh lemon/sql.y td_sql.y"
  echo "- tool/migrate/audit_lemon_write_semantics.sh"
  echo
} > "$out_dir/parity_summary.md"

"$goyacc_cmd" -o sql.go -v y.output td_sql.y >/tmp/validate_goyacc.log 2>&1 || true
conflict_line="$(rg '^conflicts:' /tmp/validate_goyacc.log || true)"
sr_conflicts=0
rr_conflicts=0
if [[ -n "$conflict_line" ]]; then
  sr_conflicts="$(echo "$conflict_line" | sed -nE 's/.*: ([0-9]+) shift\/reduce(, ([0-9]+) reduce\/reduce)?/\1/p')"
  rr_conflicts="$(echo "$conflict_line" | sed -nE 's/.*: ([0-9]+) shift\/reduce, ([0-9]+) reduce\/reduce/\2/p')"
  if [[ -z "$rr_conflicts" ]]; then
    rr_conflicts=0
  fi
fi
cat > "$out_dir/conflicts.json" <<EOF
{
  "shift_reduce": $sr_conflicts,
  "reduce_reduce": $rr_conflicts,
  "source": "goyacc stderr/stdout",
  "line": "$(echo "$conflict_line" | sed 's/"/\\"/g')"
}
EOF
{
  echo "## goyacc output"
  echo '```'
  cat /tmp/validate_goyacc.log
  echo '```'
  echo
} >> "$out_dir/parity_summary.md"

{
  echo "## structured conflicts"
  echo "- reports/final/conflicts.json"
  echo
} >> "$out_dir/parity_summary.md"

{
  echo "## y.output conflicts"
  echo '```'
  rg -n "conflicts:" y.output || true
  echo '```'
  echo
} >> "$out_dir/parity_summary.md"

{
  echo "## precedence parity"
  echo '```'
  bash "$repo_root/tool/migrate/check_precedence_parity.sh" lemon/sql.y td_sql.y
  echo '```'
  echo
} >> "$out_dir/parity_summary.md"

write_audit_report="$(bash "$repo_root/tool/migrate/audit_lemon_write_semantics.sh")"
commented_only_count="$(rg -n '\| commented-only \|' "$write_audit_report" | wc -l)"
c_only_unreachable_count="$(rg -n '\| c-only-unreachable \|' "$write_audit_report" | wc -l)"
{
  echo "## lemon write semantics audit"
  echo
  echo "- report: \`reports/final/lemon_write_semantics.md\`"
  echo "- commented-only write constructors: $commented_only_count"
  echo "- c-only-unreachable write constructors: $c_only_unreachable_count"
  echo
} >> "$out_dir/parity_summary.md"

{
  echo "## test result"
  echo '```'
  GOCACHE=/tmp/go-build GOMODCACHE=/tmp/gomodcache go test ./... -count=1
  echo '```'
} >> "$out_dir/parity_summary.md"

{
  echo "# Open Gaps"
  echo
  echo "Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo
  echo "Data source:"
  echo "- reports/final/conflicts.json"
  echo
  if [[ "$sr_conflicts" -gt 0 || "$rr_conflicts" -gt 0 ]]; then
    echo "- Parser conflicts remain: shift/reduce=$sr_conflicts, reduce/reduce=$rr_conflicts."
  else
    echo "- Parser conflicts: none (shift/reduce=0, reduce/reduce=0)."
  fi
  if [[ "$commented_only_count" -gt 0 ]]; then
    echo "- Lemon write constructors with commented-only grammar branches: $commented_only_count (see reports/final/lemon_write_semantics.md)."
  fi
  if [[ "$c_only_unreachable_count" -gt 0 ]]; then
    echo "- Lemon write constructors currently not reachable from sql.y: $c_only_unreachable_count (see reports/final/lemon_write_semantics.md)."
  fi
  echo "- Lemon C runtime side-by-side execution is not wired in this repository; parity currently uses static rule extraction + Go behavior tests."
  echo "- AST field-by-field equivalence against Lemon runtime output is not yet automated because Lemon runtime harness is unavailable in-repo."
} > "$out_dir/open_gaps.md"

echo "final validation reports generated under: $out_dir"
