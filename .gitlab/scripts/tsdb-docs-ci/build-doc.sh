#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
source "${ROOT}/.gitlab/scripts/tsdb-docs-ci/common.sh"

# Bind-mounted repos have host-side ownership (gitlab-runner) while the
# container runs as root, which trips git 2.35+ "dubious ownership". Trust
# all paths so child git invocations (e.g. assemble.js) work.
git config --global --add safe.directory '*'

run_docs_build() {
  local doc_dir="$1"
  local tdengine_root="$2"
  local docs_src_root="$3"

  (
    cd "${doc_dir}"
    yarn install
    TDENGINE_ROOT="${tdengine_root}" DOCS_SRC_ROOT="${docs_src_root}" yarn ass local
    yarn build
  )
}

bash "${ROOT}/.gitlab/scripts/tsdb-docs-ci/prepare-workspace.sh"

# Symlinks for legacy {{#include}} paths in markdown files:
#   docs/examples/...          → TSDB_DIR/docs/examples (needs symlink inside docs/)
#   ../TDengine/source/...     → resolved via TDengine symlink at workspace level
#   /TDengine/source/...       → resolved via TDengine symlink at workspace level
ln -sfn "../source/taos-community/docs/examples" "${TSDB_DIR}/docs/examples"
ln -sfn tsdb "${DOCS_CI_WORKDIR}/TDengine"

eval "$(changed_doc_languages)"

if [ "${FORCE_BUILD_ALL:-}" = "1" ]; then
  zh=true
  en=true
fi

if [ "${zh}" = "true" ]; then
  run_docs_build "${ZH_DOC_DIR}" "${TSDB_DIR}" "${TSDB_DOCS_DIR}/zh"
fi

if [ "${en}" = "true" ]; then
  run_docs_build "${EN_DOC_DIR}" "${TSDB_DIR}" "${TSDB_DOCS_DIR}/en"
fi
