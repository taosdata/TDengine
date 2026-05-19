#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
source "${ROOT}/.gitlab/scripts/tsdb-docs-ci/common.sh"

# Build a null-safe array of changed markdown files (preserve spaces in filenames)
changed_files=()
while IFS= read -r file; do
  # skip empty lines
  [ -z "${file}" ] && continue
  [ -e "${file}" ] || continue
  changed_files+=("${file}")
done < <(changed_markdown_files)

if [ ${#changed_files[@]} -eq 0 ]; then
  echo "No changed markdown files under source/taos-community/docs"
  exit 0
fi

# Pass filenames as separate arguments, preserving spaces
markdownlint-cli2 \
  --config source/taos-community/docs/.markdownlint-cli2.jsonc \
  "${changed_files[@]}"
