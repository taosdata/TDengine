#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
lemon_sql="$repo_root/lemon/sql.y"
go_sql="$repo_root/td_sql.y"
out_dir="$repo_root/reports/final"

mkdir -p "$out_dir"

if [[ ! -f "$lemon_sql" || ! -f "$go_sql" ]]; then
  echo "missing grammar files: $lemon_sql / $go_sql" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

lemon_rules="$tmp_dir/lemon_rules.tsv"
go_rules="$tmp_dir/go_rules.tsv"

# Lemon: count productions per lhs using ::= and (A) signatures.
awk '
{
  line=$0
  if (match(line, /^([a-zA-Z_][a-zA-Z0-9_]*)\([A-Za-z]\)[ \t]*::=/, m)) {
    lhs=m[1]
    c[lhs]++
  }
}
END {
  for (k in c) printf "%s\t%d\n", k, c[k]
}
' "$lemon_sql" | sort > "$lemon_rules"

# goyacc: count alternatives per rule block.
awk '
function countch(s, ch,    t) {
  t = s
  return gsub(ch, "", t)
}
function flush() {
  if (lhs != "") {
    c[lhs] += alts
  }
  lhs=""
  alts=0
  in_action_depth=0
  pending_alt=0
  seen_alt_body=0
}
{
  line=$0
  if (match(line, /^[a-zA-Z_][a-zA-Z0-9_]*[ \t]*:[ \t]*$/)) {
    flush()
    lhs=line
    sub(/[ \t]*:[ \t]*$/, "", lhs)
    pending_alt=1
    next
  }
  if (lhs == "") next

  if (in_action_depth > 0) {
    in_action_depth += countch(line, "\\{")
    in_action_depth -= countch(line, "\\}")
    next
  }

  if (line ~ /^[ \t]*\/\//) next
  if (line ~ /^[ \t]*$/) next
  if (line ~ /^[ \t]*%/) next

  if (line ~ /^[ \t]*\|/) {
    pending_alt=1
    seen_alt_body=0
    sub(/^[ \t]*\|[ \t]*/, "", line)
  }

  if (line ~ /^[ \t]*\/\*[ \t]*empty[ \t]*\*\/[ \t]*$/) {
    if (pending_alt == 1) {
      alts++
      pending_alt=0
    }
    seen_alt_body=1
    next
  }
  if (line ~ /^[ \t]*\/\*/) next
  if (line ~ /^[ \t]*\*/) next

  if (line ~ /^[ \t]*\{/) {
    in_action_depth = countch(line, "\\{") - countch(line, "\\}")
    if (in_action_depth < 0) in_action_depth = 0
    next
  }

  if (pending_alt == 1) {
    alts++
    pending_alt=0
  }
  seen_alt_body=1
}
END {
  flush()
  for (k in c) printf "%s\t%d\n", k, c[k]
}
' "$go_sql" | sort > "$go_rules"

matrix="$out_dir/statement_rule_matrix.md"
diff_md="$out_dir/statement_rule_diff.md"
fixq="$out_dir/statement_fix_queue.md"

{
  echo "# Statement Rule Matrix"
  echo
  echo "Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo
  echo "Scope: Lemon-defined rules only (Go-only helper/wrapper rules are excluded)"
  echo
  echo "| Rule | Lemon Branches | Go Branches | Status |"
  echo "|---|---:|---:|---|"
  join -t $'\t' -a1 -e 0 -o 0,1.2,2.2 "$lemon_rules" "$go_rules" \
    | awk -F '\t' '
      {
        status="match"
        if ($2 != $3) status="diff"
        printf "| %s | %d | %d | %s |\n", $1, $2, $3, status
      }
    ' | sort
} > "$matrix"

{
  echo "# Statement Rule Diff"
  echo
  echo "Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo
  echo "Scope: Lemon-defined rules only (Go-only helper/wrapper rules are excluded)"
  echo
  echo "## Missing In Go (present in Lemon)"
  echo
  missing="$(comm -23 <(cut -f1 "$lemon_rules") <(cut -f1 "$go_rules"))"
  if [[ -n "$missing" ]]; then
    printf "%s\n" "$missing" | sed 's/^/- /'
  else
    echo "- none"
  fi
  echo
  echo "## Rule Count Differences"
  echo
  diffs="$(join -t $'\t' -a1 -e 0 -o 0,1.2,2.2 "$lemon_rules" "$go_rules" \
    | awk -F '\t' '$2 != $3 { printf "- %s: lemon=%d go=%d\n", $1, $2, $3 }')"
  if [[ -n "$diffs" ]]; then
    printf "%s\n" "$diffs"
  else
    echo "- none"
  fi
} > "$diff_md"

{
  echo "# Statement Fix Queue (One Branch Per Iteration)"
  echo
  echo "Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo
  echo "Scope: Lemon-defined rules only (Go-only helper/wrapper rules are excluded)"
  echo
  echo "| ID | Rule | Lemon Branches | Go Branches | Status |"
  echo "|---|---|---:|---:|---|"
  join -t $'\t' -a1 -e 0 -o 0,1.2,2.2 "$lemon_rules" "$go_rules" \
    | awk -F '\t' '
      $2 != $3 {
        printf "| STMT-%03d | %s | %d | %d | pending |\n", ++i, $1, $2, $3
      }
    '
} > "$fixq"

echo "generated:"
echo "- $matrix"
echo "- $diff_md"
echo "- $fixq"
