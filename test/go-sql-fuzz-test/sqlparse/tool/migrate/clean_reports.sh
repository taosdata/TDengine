#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
reports_dir="$repo_root/reports"
archive_date="${1:-$(date -u +%F)}"
archive_dir="$reports_dir/archive/$archive_date"

mkdir -p "$archive_dir/query" "$archive_dir/baseline"

move_if_exists() {
  local src="$1"
  local dst="$2"
  if [[ -f "$src" ]]; then
    mkdir -p "$(dirname "$dst")"
    mv "$src" "$dst"
    echo "archived: ${src#$repo_root/} -> ${dst#$repo_root/}"
  fi
}

# Query iteration snapshots.
for n in 001 002 003 004 005 006; do
  move_if_exists \
    "$reports_dir/query/iteration-QRY-$n.md" \
    "$archive_dir/query/iteration-QRY-$n.md"
done

# Baseline snapshot reports.
move_if_exists "$reports_dir/baseline/precedence_diff.md" "$archive_dir/baseline/precedence_diff.md"
move_if_exists "$reports_dir/baseline/token_diff.md" "$archive_dir/baseline/token_diff.md"
move_if_exists "$reports_dir/baseline/rule_inventory.md" "$archive_dir/baseline/rule_inventory.md"

# Session and one-off reports.
move_if_exists "$reports_dir/session_handoff_2026-02-13.md" "$archive_dir/session_handoff_2026-02-13.md"
move_if_exists "$reports_dir/next_chat_execution_plan.md" "$archive_dir/next_chat_execution_plan.md"
move_if_exists "$reports_dir/test_sql_pass.md" "$archive_dir/test_sql_pass.md"
move_if_exists "$reports_dir/test_sql_fail.md" "$archive_dir/test_sql_fail.md"
move_if_exists "$reports_dir/test_consolidation_map.md" "$archive_dir/test_consolidation_map.md"

echo "archive complete: ${archive_dir#$repo_root/}"
