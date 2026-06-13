#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
lemon_sql="$repo_root/lemon/sql.y"
go_sql="$repo_root/td_sql.y"
out_dir="$repo_root/reports/query"

mkdir -p "$out_dir"

if [[ ! -f "$lemon_sql" || ! -f "$go_sql" ]]; then
  echo "missing grammar files: $lemon_sql / $go_sql" >&2
  exit 1
fi

query_rules=(
  query_expression query_simple union_query_expression
  query_simple_or_subquery query_or_subquery query_specification
  set_quantifier_opt select_list select_item from_clause_opt
  table_reference_list table_reference table_primary alias_opt joined_table
  inner_joined outer_joined semi_joined anti_joined asof_joined win_joined
  join_on_clause_opt join_on_clause window_offset_clause window_offset_literal
  jlimit_clause_opt where_clause_opt group_by_clause_opt group_by_list
  having_clause_opt order_by_clause_opt sort_specification_list
  sort_specification ordering_specification_opt null_ordering_opt
  limit_clause_opt slimit_clause_opt subquery common_expression expr_or_subquery
  expression expression_list column_reference column_alias table_alias
  pseudo_column function_expression function_name star_func
  if_expression case_when_expression when_then_list case_when_else_opt
  boolean_value_expression boolean_primary predicate compare_op in_op
  search_condition partition_by_clause_opt partition_list partition_item
  range_opt every_opt interp_fill_opt tag_mode_opt
  twindow_clause_opt count_window_args interval_sliding_duration_literal
  sliding_opt extend_literal zeroth_literal state_window_opt true_for_opt
  fill_opt fill_value fill_mode fill_position_mode fill_position_mode_extension
  interp_fill_mode parenthesized_joined_table type_name trim_specification_type
)

rule_re="$(printf '%s|' "${query_rules[@]}")"
rule_re="${rule_re%|}"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

lemon_all="$tmp_dir/lemon_all.tsv"
go_all="$tmp_dir/go_all.tsv"

awk -v RULE_RE="^(""$rule_re"")$" '
BEGIN {
  in_prod=0
  lhs=""
  rhs=""
  action=""
}
function trim(s) {
  gsub(/^[ \t\r\n]+|[ \t\r\n]+$/, "", s)
  return s
}
function flush_prod() {
  if (lhs == "") return
  r = trim(rhs)
  a = trim(action)
  gsub(/[ \t]+/, " ", r)
  gsub(/[ \t]+/, " ", a)
  gsub(/\.$/, "", r)
  if (lhs ~ RULE_RE) {
    print lhs "\t" r "\t" a
  }
  lhs=""
  rhs=""
  action=""
  in_prod=0
}
{
  line=$0
  if (!in_prod) {
    if (match(line, /^([a-zA-Z_][a-zA-Z0-9_]*)\(A\)[ \t]*::=[ \t]*/)) {
      in_prod=1
      lhs=substr(line, RSTART, RLENGTH)
      sub(/\(A\)[ \t]*::=.*/, "", lhs)
      rhs=line
      sub(/^.*::=[ \t]*/, "", rhs)
      if (index(rhs, "{") > 0) {
        action=rhs
        sub(/^[^{]*\{/, "", action)
        sub(/\}.*/, "", action)
        sub(/\{.*/, "", rhs)
        flush_prod()
      }
      next
    }
    next
  }

  if (match(line, /^[ \t]*\/\//)) {
    next
  }

  if (index(line, "{") > 0) {
    action=line
    sub(/^[^{]*\{/, "", action)
    sub(/\}.*/, "", action)
    rhs=rhs " " line
    sub(/\{.*/, "", rhs)
    flush_prod()
    next
  }

  rhs=rhs " " line
  if (index(line, ".") > 0) {
    flush_prod()
  }
}
END {
  flush_prod()
}
' "$lemon_sql" > "$lemon_all"

awk -v RULE_RE="^(""$rule_re"")$" '
BEGIN {
  lhs=""
  alt=""
  seen=0
  in_action=0
}
function trim(s) {
  gsub(/^[ \t\r\n]+|[ \t\r\n]+$/, "", s)
  return s
}
function flush_alt() {
  a=trim(alt)
  if (lhs != "" && a != "") {
    gsub(/[ \t]+/, " ", a)
    print lhs "\t" a "\t" ""
  }
  alt=""
  seen=0
}
{
  line=$0
  if (in_action) {
    if (match(line, /^[ \t]*\}/)) {
      in_action=0
    }
    next
  }
  if (match(line, /^([a-zA-Z_][a-zA-Z0-9_]*)[ \t]*:[ \t]*$/)) {
    flush_alt()
    lhs=substr(line, RSTART, RLENGTH)
    sub(/[ \t]*:[ \t]*$/, "", lhs)
    next
  }
  if (lhs == "" || lhs !~ RULE_RE) next

  if (match(line, /^[ \t]*\|/)) {
    flush_alt()
    alt=line
    sub(/^[ \t]*\|[ \t]*/, "", alt)
    seen=1
    next
  }
  if (match(line, /^[ \t]*\/\*.*\*\/[ \t]*$/)) {
    if (line ~ /empty/) {
      alt="<empty>"
      seen=1
    }
    next
  }
  if (match(line, /^[ \t]*\/\//)) {
    next
  }
  if (match(line, /^[ \t]*\{/)) {
    in_action=1
    next
  }
  if (match(line, /^[ \t]*$/)) {
    flush_alt()
    next
  }
  if (!seen) {
    alt=line
    sub(/^[ \t]*/, "", alt)
    seen=1
  } else {
    more=line
    sub(/^[ \t]*/, "", more)
    alt=alt " " more
  }
}
END {
  flush_alt()
}
' "$go_sql" > "$go_all"

lemon_norm="$tmp_dir/lemon_norm.tsv"
go_norm="$tmp_dir/go_norm.tsv"

awk -F '\t' '
function norm(s) {
  gsub(/\([A-Za-z]\)/, "", s)
  gsub(/%prec[[:space:]]+[A-Za-z_][A-Za-z0-9_]*/, "", s)
  gsub(/\/\*[[:space:]]*empty[[:space:]]*\*\//, "<empty>", s)
  gsub(/\.[[:space:]]*\[[^]]+\]/, "<empty>", s)
  gsub(/(^|[[:space:]])\./, " <empty>", s)
  gsub(/<empty>[[:space:]]+<empty>/, "<empty>", s)
  gsub(/\{.*/, "", s)
  gsub(/\bhint_list\b/, "hint_opt", s)
  gsub(/\bwhen_then_expr\b/, "WHEN common_expression THEN common_expression", s)
  gsub(/\bin_predicate_value\b/, "NK_LP expression_list NK_RP", s)
  gsub(/WHEN common_expression THEN common_expression/, "when_then_expr", s)
  if (s ~ /^\/\//) s=""
  gsub(/[ \t]+/, " ", s)
  gsub(/^[ ]+|[ ]+$/, "", s)
  if (s == "") s="<empty>"
  return s
}
{
  lhs=$1
  if (lhs == "hint_list") lhs="hint_opt"
  print lhs "\t" norm($2) "\t" $3
}
' "$lemon_all" > "$lemon_norm"

awk -F '\t' '
function norm(s) {
  gsub(/\([A-Za-z]\)/, "", s)
  gsub(/%prec[[:space:]]+[A-Za-z_][A-Za-z0-9_]*/, "", s)
  gsub(/\/\*[[:space:]]*empty[[:space:]]*\*\//, "<empty>", s)
  gsub(/\{.*/, "", s)
  gsub(/\bhint_list\b/, "hint_opt", s)
  gsub(/\bwhen_then_expr\b/, "WHEN common_expression THEN common_expression", s)
  gsub(/\bin_predicate_value\b/, "NK_LP expression_list NK_RP", s)
  gsub(/WHEN common_expression THEN common_expression/, "when_then_expr", s)
  if (s ~ /^\/\//) s=""
  gsub(/[ \t]+/, " ", s)
  gsub(/^[ ]+|[ ]+$/, "", s)
  if (s == "") s="<empty>"
  return s
}
{ print $1 "\t" norm($2) "\t" $3 }
' "$go_all" > "$go_norm"

matrix="$out_dir/rule_matrix.md"
diff_md="$out_dir/rule_diff.md"
queue="$out_dir/fix_queue.md"

{
  echo "# Query Rule Matrix"
  echo
  echo "Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo
  echo "| Rule | Lemon Branches | Go Branches | Status |"
  echo "|---|---:|---:|---|"
  for r in "${query_rules[@]}"; do
    l_count="$(awk -F '\t' -v r="$r" '$1==r{c++} END{print c+0}' "$lemon_norm")"
    g_count="$(awk -F '\t' -v r="$r" '$1==r{c++} END{print c+0}' "$go_norm")"
    status="match"
    if [[ "$l_count" != "$g_count" ]]; then
      status="count-diff"
    fi
    echo "| $r | $l_count | $g_count | $status |"
  done
} > "$matrix"

{
  echo "# Query Rule Diff"
  echo
  echo "Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo
  echo "## Missing In Go (present in Lemon)"
  echo
  awk -F '\t' '{print $1 "\t" $2}' "$lemon_norm" | sort -u > "$tmp_dir/lemon_pairs.txt"
  awk -F '\t' '{print $1 "\t" $2}' "$go_norm" | sort -u > "$tmp_dir/go_pairs.txt"
  comm -23 "$tmp_dir/lemon_pairs.txt" "$tmp_dir/go_pairs.txt" | sed 's/^/- `/' | sed 's/$/`/'
  echo
  echo "## Extra In Go (not found in Lemon)"
  echo
  comm -13 "$tmp_dir/lemon_pairs.txt" "$tmp_dir/go_pairs.txt" | sed 's/^/- `/' | sed 's/$/`/'
  echo
  echo "## Rule Count Differences"
  echo
  for r in "${query_rules[@]}"; do
    l_count="$(awk -F '\t' -v r="$r" '$1==r{c++} END{print c+0}' "$lemon_norm")"
    g_count="$(awk -F '\t' -v r="$r" '$1==r{c++} END{print c+0}' "$go_norm")"
    if [[ "$l_count" != "$g_count" ]]; then
      echo "- \`$r\`: Lemon=$l_count, Go=$g_count"
    fi
  done
} > "$diff_md"

{
  echo "# Query Fix Queue (One Production Branch Per Iteration)"
  echo
  echo "Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo
  echo "| ID | Rule | Branch Signature | Source | Status | Test ID |"
  echo "|---|---|---|---|---|---|"
  i=1
  awk -F '\t' '{print $1 "\t" $2}' "$lemon_norm" | sort -u | while IFS=$'\t' read -r rule rhs; do
    if ! grep -Fqx "$rule"$'\t'"$rhs" "$tmp_dir/go_pairs.txt"; then
      id="$(printf "QRY-%03d" "$i")"
      sig="$(echo "$rhs" | sed 's/|/\\|/g')"
      echo "| $id | $rule | \`$sig\` | Lemon | todo | TEST-$id |"
      i=$((i+1))
    fi
  done
} > "$queue"

echo "generated:"
echo "- $matrix"
echo "- $diff_md"
echo "- $queue"
