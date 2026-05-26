#!/usr/bin/env bash
# =============================================================================
# tsdb-docs-cd / build-remote.sh — runs INSIDE the docker container
#
# Builds one language of the docs site in "remote" mode (assemble.js fetches
# every configured version branch). Build output lands at ${DOC_DIR}/build/
# which is bind-mounted and visible to the host after this exits.
# =============================================================================
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
source "${ROOT}/.gitlab/scripts/tsdb-docs-ci/common.sh"

LANG_ARG=""
while [ $# -gt 0 ]; do
  case "$1" in
    --lang) LANG_ARG="${2:-}"; shift 2 ;;
    --lang=*) LANG_ARG="${1#*=}"; shift ;;
    *) echo "build-remote.sh: unknown argument: $1" >&2; exit 2 ;;
  esac
done

case "${LANG_ARG}" in
  zh) DOC_DIR="${ZH_DOC_DIR}" ;;
  en) DOC_DIR="${EN_DOC_DIR}" ;;
  *) echo "build-remote.sh: --lang must be zh or en" >&2; exit 2 ;;
esac

git config --global --add safe.directory '*'

if [ ! -d "${DOC_DIR}/.git" ]; then
  echo "build-remote.sh: ${DOC_DIR} not a git checkout; host-side prep missing?" >&2
  exit 1
fi

# assemble.js shells out to `git fetch` on tsdb. Wire CI_JOB_TOKEN for auth.
if [ -n "${CI_JOB_TOKEN:-}" ]; then
  git config --global \
    url."https://gitlab-ci-token:${CI_JOB_TOKEN}@git.tdengine.net/".insteadOf \
    "https://git.tdengine.net/"
fi

(
  cd "${DOC_DIR}"
  yarn install
  yarn ass
  yarn build
)

BUILD_OUTPUT_DIR="${DOC_DIR}/build"
if [ ! -d "${BUILD_OUTPUT_DIR}" ]; then
  echo "build-remote.sh: expected build output at ${BUILD_OUTPUT_DIR}, not found" >&2
  exit 1
fi

echo "build-remote.sh: build succeeded → ${BUILD_OUTPUT_DIR}"
