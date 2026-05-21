#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
source "${ROOT}/.gitlab/scripts/tsdb-docs-ci/common.sh"

: "${DOCS_CI_IMAGE:?DOCS_CI_IMAGE must be set}"

case "$*" in
  *"build-doc.sh"*)
    prepare_docs_repo_on_host "${ZH_DOC_REPO_URL}" "${ZH_DOC_DIR}" "${ZH_DOC_BRANCH}"
    prepare_docs_repo_on_host "${EN_DOC_REPO_URL}" "${EN_DOC_DIR}" "${EN_DOC_BRANCH}"
    ;;
esac

docker_args=(
  run
  --rm
  -v "${DOCS_CI_WORKDIR}:${DOCS_CI_WORKDIR}"
)

if [ "${ROOT}" != "${TSDB_DIR}" ]; then
  docker_args+=(-v "${ROOT}:${TSDB_DIR}")
fi

# Forward workspace and CI variables needed by scripts inside the container.
for _var in DOCS_CI_WORKDIR TSDB_DIR ZH_DOC_DIR EN_DOC_DIR CI_COMMIT_SHA CI_MERGE_REQUEST_DIFF_BASE_SHA CI_CONCURRENT_PROJECT_ID FORCE_BUILD_ALL; do
  if [ -n "${!_var+x}" ]; then
    docker_args+=(-e "${_var}=${!_var}")
  fi
done

docker_args+=(
  -w "${TSDB_DIR}"
  "${DOCS_CI_IMAGE}"
)

docker "${docker_args[@]}" "$@"
