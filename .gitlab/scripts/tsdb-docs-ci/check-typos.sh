#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
source "${ROOT}/.gitlab/scripts/tsdb-docs-ci/common.sh"

changed_files=()
while IFS= read -r file; do
  [ -z "${file}" ] && continue
  [ -e "${file}" ] || continue
  changed_files+=("${file}")
done < <(changed_doc_files)

if [ ${#changed_files[@]} -eq 0 ]; then
  echo "No changed docs files under source/taos-community/docs"
  exit 0
fi

typos \
  "${changed_files[@]}" \
  --config source/taos-community/docs/typos.toml
