#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <path-to-lemon-sql.y>" >&2
  exit 1
fi

src="$1"
if [[ ! -f "$src" ]]; then
  echo "file not found: $src" >&2
  exit 1
fi

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

# 1) precedence declarations
grep -E '^%(left|right|nonassoc)[[:space:]]' "$src" \
  | sed -E 's/^%(left|right|nonassoc)[[:space:]]+//' \
  | tr ' .' '\n\n' \
  | rg '^[A-Z][A-Z0-9_]*$' > "$tmp" || true

# 2) grammar productions (rhs terminals)
grep -E '::=' "$src" \
  | sed -E 's/\{.*$//' \
  | sed -E 's/\([A-Za-z_][A-Za-z0-9_]*\)//g' \
  | tr -c 'A-Za-z0-9_\n' '\n' \
  | rg '^[A-Z][A-Z0-9_]{1,}$' >> "$tmp" || true

sort -u "$tmp"
