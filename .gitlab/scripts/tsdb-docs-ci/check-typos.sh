#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
source "${ROOT}/.gitlab/scripts/tsdb-docs-ci/common.sh"

changed_files=()
while IFS= read -r file; do
  [ -z "${file}" ] && continue
  [ -e "${file}" ] || continue
  # Skip binary asset types — typos scans bytes and triggers false positives
  # on PNG/JPG/etc. (assets live under docs/*/assets/).
  case "${file,,}" in
    *.png|*.jpg|*.jpeg|*.gif|*.svg|*.webp|*.ico|*.pdf|*.zip|*.tar|*.tgz|*.gz|*.xz|*.bz2|*.woff|*.woff2|*.ttf|*.eot|*.otf|*.mp4|*.webm|*.mov|*.mp3|*.wav)
      continue
      ;;
  esac
  changed_files+=("${file}")
done < <(changed_doc_files)

if [ ${#changed_files[@]} -eq 0 ]; then
  echo "No changed docs files under source/taos-community/docs"
  exit 0
fi

typos \
  "${changed_files[@]}" \
  --config source/taos-community/docs/typos.toml
