#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
source "${ROOT}/.gitlab/scripts/tsdb-docs-ci/common.sh"

mkdir -p "${DOCS_CI_WORKDIR}"

if ! git -C "${TSDB_DIR}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git clone "${TSDB_REPO_URL}" "${TSDB_DIR}"
  if [ -n "${CI_COMMIT_SHA:-}" ]; then
    git -C "${TSDB_DIR}" fetch origin "${CI_COMMIT_SHA}" || true
    git -C "${TSDB_DIR}" checkout -f "${CI_COMMIT_SHA}"
  fi
fi

ensure_repo "${ZH_DOC_REPO_URL}" "${ZH_DOC_DIR}"
ensure_repo "${EN_DOC_REPO_URL}" "${EN_DOC_DIR}"
