#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
source "${ROOT}/.gitlab/scripts/tsdb-docs-ci/common.sh"

changed_files=()
while IFS= read -r file; do
  [ -z "${file}" ] && continue
  [ -e "${file}" ] || continue
  changed_files+=("${file}")
done < <(changed_markdown_files)

if [ ${#changed_files[@]} -eq 0 ]; then
  echo "No changed markdown files under source/taos-community/docs"
  exit 0
fi

autocorrect --lint "${changed_files[@]}"
