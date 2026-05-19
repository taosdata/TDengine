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

if [ ${#changed_files[@]} -gt 0 ]; then
  autocorrect --fix "${changed_files[@]}"
  markdownlint-cli2 \
    --fix \
    --config source/taos-community/docs/.markdownlint-cli2.jsonc \
    "${changed_files[@]}"
else
  echo "No changed markdown files under source/taos-community/docs"
fi

echo "docs autofix finished; review changes with git diff"
echo "typos remains check-only; add project terms to source/taos-community/docs/typos.toml."
