#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)

usage() {
  cat <<'EOF'
Usage: .gitlab/scripts/tsdb-docs-ci/local-validate.sh [options]

Prepare local docs CI dependencies, then run docs lint and build in Docker.

Options:
  --workdir DIR          Workspace containing docs repos (default: parent of current tsdb checkout)
  --tsdb-dir DIR         Existing tsdb repo to validate (default: current checkout)
  --base-ref REF         Base ref for markdownlint changed-file detection (default: origin/main)
  --image-tar FILE       Load docs-ci image from an offline docker save tar/tar.gz
  --build-image          Build docs-ci image locally from .gitlab/scripts/tsdb-docs-ci/Dockerfile
  --pull-image IMAGE     Pull an existing docs-ci image and retag it locally
  --image-name IMAGE     Local image tag used for validation (default: docs-ci:local)
  --tsdb-branch BRANCH   Branch used for tsdb; specified tsdb dirs are left as-is unless this is set
  --docs-branch BRANCH   Use BRANCH for both docs repos (default: zh=master, en=main)
  --zh-docs-branch BR    Override zh docs branch only (default: master)
  --en-docs-branch BR    Override en docs branch only (default: main)
  --fix                  Run autocorrect and markdownlint fixes before validation
  --preview              After validation succeeds, build both zh & en sites and serve them locally
  --zh-port N            Host port for the zh preview site (default: 3000)
  --en-port N            Host port for the en preview site (default: 3001)
  -h, --help             Show this help
EOF
}

DOCS_CI_WORKDIR="${DOCS_CI_WORKDIR:-$(cd "${ROOT}/.." && pwd)}"
TSDB_DIR="${TSDB_DIR:-}"
if [ -n "${TSDB_DIR}" ]; then
  TSDB_DIR_SPECIFIED=true
else
  TSDB_DIR_SPECIFIED=false
fi
BASE_REF="origin/main"
IMAGE_TAR=""
BUILD_IMAGE=false
PULL_IMAGE=""
DOCS_CI_IMAGE="${DOCS_CI_IMAGE:-docs-ci:local}"
TSDB_BRANCH="${TSDB_BRANCH:-}"
ZH_DOC_BRANCH="${ZH_DOC_BRANCH:-master}"
EN_DOC_BRANCH="${EN_DOC_BRANCH:-main}"
FIX=false
PREVIEW=false
ZH_PORT=3000
EN_PORT=3001

while [ "$#" -gt 0 ]; do
  case "$1" in
    --workdir)
      DOCS_CI_WORKDIR="$2"
      shift 2
      ;;
    --tsdb-dir)
      TSDB_DIR="$2"
      TSDB_DIR_SPECIFIED=true
      shift 2
      ;;
    --base-ref)
      BASE_REF="$2"
      shift 2
      ;;
    --image-tar)
      IMAGE_TAR="$2"
      shift 2
      ;;
    --build-image)
      BUILD_IMAGE=true
      shift
      ;;
    --pull-image)
      PULL_IMAGE="$2"
      shift 2
      ;;
    --image-name)
      DOCS_CI_IMAGE="$2"
      shift 2
      ;;
    --tsdb-branch)
      TSDB_BRANCH="$2"
      shift 2
      ;;
    --docs-branch)
      ZH_DOC_BRANCH="$2"
      EN_DOC_BRANCH="$2"
      shift 2
      ;;
    --zh-docs-branch)
      ZH_DOC_BRANCH="$2"
      shift 2
      ;;
    --en-docs-branch)
      EN_DOC_BRANCH="$2"
      shift 2
      ;;
    --fix)
      FIX=true
      shift
      ;;
    --preview)
      PREVIEW=true
      shift
      ;;
    --zh-port)
      ZH_PORT="$2"
      shift 2
      ;;
    --en-port)
      EN_PORT="$2"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

export DOCS_CI_WORKDIR
export TSDB_DIR="${TSDB_DIR:-${ROOT}}"
export ZH_DOC_DIR="${ZH_DOC_DIR:-${DOCS_CI_WORKDIR}/docs.taosdata.com}"
export EN_DOC_DIR="${EN_DOC_DIR:-${DOCS_CI_WORKDIR}/docs.tdengine.com}"
export DOCS_CI_IMAGE
export ZH_DOC_BRANCH
export EN_DOC_BRANCH

TSDB_REPO_URL="${TSDB_REPO_URL:-https://git.tdengine.net/rd-public/tsdb.git}"
ZH_DOC_REPO_URL="${ZH_DOC_REPO_URL:-https://github.com/taosdata/docs.taosdata.com.git}"
EN_DOC_REPO_URL="${EN_DOC_REPO_URL:-https://github.com/taosdata/docs.tdengine.com.git}"

ensure_repo_branch() {
  local repo_url="$1"
  local repo_dir="$2"
  local branch="$3"

  if [ ! -d "${repo_dir}/.git" ]; then
    git clone "${repo_url}" "${repo_dir}"
  fi

  git -C "${repo_dir}" fetch origin "${branch}"
  git -C "${repo_dir}" checkout -B "${branch}" "origin/${branch}"
}

ensure_tsdb_repo() {
  if ! git -C "${TSDB_DIR}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git clone "${TSDB_REPO_URL}" "${TSDB_DIR}"
    git -C "${TSDB_DIR}" fetch origin "${TSDB_BRANCH:-feat/tsdb-docs-gitlab-ci}"
    git -C "${TSDB_DIR}" checkout -B "${TSDB_BRANCH:-feat/tsdb-docs-gitlab-ci}" "origin/${TSDB_BRANCH:-feat/tsdb-docs-gitlab-ci}"
    return
  fi

  if [ -z "${TSDB_BRANCH}" ]; then
    return
  fi

  git -C "${TSDB_DIR}" fetch origin "${TSDB_BRANCH:-feat/tsdb-docs-gitlab-ci}"
  git -C "${TSDB_DIR}" checkout -B "${TSDB_BRANCH:-feat/tsdb-docs-gitlab-ci}" "origin/${TSDB_BRANCH:-feat/tsdb-docs-gitlab-ci}"
}

prepare_image() {
  if [ -n "${IMAGE_TAR}" ]; then
    docker load -i "${IMAGE_TAR}"
    docker tag docs-ci:latest "${DOCS_CI_IMAGE}"
    return
  fi

  if [ -n "${PULL_IMAGE}" ]; then
    docker pull "${PULL_IMAGE}"
    docker tag "${PULL_IMAGE}" "${DOCS_CI_IMAGE}"
    return
  fi

  if [ "${BUILD_IMAGE}" = true ] || ! docker image inspect "${DOCS_CI_IMAGE}" >/dev/null 2>&1; then
    docker build -t "${DOCS_CI_IMAGE}" -f "${ROOT}/.gitlab/scripts/tsdb-docs-ci/Dockerfile" "${ROOT}/.gitlab/scripts/tsdb-docs-ci"
  fi
}

run_in_docs_image() {
  DOCS_CI_IMAGE="${DOCS_CI_IMAGE}" \
  DOCS_CI_WORKDIR="${DOCS_CI_WORKDIR}" \
  TSDB_DIR="${TSDB_DIR}" \
  ZH_DOC_DIR="${ZH_DOC_DIR}" \
  EN_DOC_DIR="${EN_DOC_DIR}" \
  ZH_DOC_BRANCH="${ZH_DOC_BRANCH}" \
  EN_DOC_BRANCH="${EN_DOC_BRANCH}" \
  CI_MERGE_REQUEST_DIFF_BASE_SHA="${CI_MERGE_REQUEST_DIFF_BASE_SHA:-}" \
  CI_COMMIT_SHA="${CI_COMMIT_SHA:-}" \
  FORCE_BUILD_ALL="${FORCE_BUILD_ALL:-}" \
  bash "${ROOT}/.gitlab/scripts/tsdb-docs-ci/run-in-docker.sh" "$@"
}

mkdir -p "${DOCS_CI_WORKDIR}"

ensure_tsdb_repo
ensure_repo_branch "${ZH_DOC_REPO_URL}" "${ZH_DOC_DIR}" "${ZH_DOC_BRANCH}"
ensure_repo_branch "${EN_DOC_REPO_URL}" "${EN_DOC_DIR}" "${EN_DOC_BRANCH}"

prepare_image

if [[ "${BASE_REF}" == origin/* ]]; then
  git -C "${TSDB_DIR}" fetch origin "${BASE_REF#origin/}" || true
else
  git -C "${TSDB_DIR}" fetch origin "${BASE_REF}" || true
fi
export CI_MERGE_REQUEST_DIFF_BASE_SHA
CI_MERGE_REQUEST_DIFF_BASE_SHA=$(git -C "${TSDB_DIR}" merge-base "${BASE_REF}" HEAD)
export CI_COMMIT_SHA
CI_COMMIT_SHA=$(git -C "${TSDB_DIR}" rev-parse HEAD)

if [ "${FIX}" = true ]; then
  run_in_docs_image bash .gitlab/scripts/tsdb-docs-ci/autofix.sh
fi

run_in_docs_image bash .gitlab/scripts/tsdb-docs-ci/check-typos.sh
run_in_docs_image bash .gitlab/scripts/tsdb-docs-ci/check-autocorrect.sh
run_in_docs_image bash .gitlab/scripts/tsdb-docs-ci/check-markdownlint.sh
run_in_docs_image bash .gitlab/scripts/tsdb-docs-ci/build-doc.sh

echo "docs local validation passed"

if [ "${PREVIEW}" = true ]; then
  echo "==========================================================="
  echo "building both zh & en sites for preview ..."
  echo "==========================================================="
  FORCE_BUILD_ALL=1 run_in_docs_image bash .gitlab/scripts/tsdb-docs-ci/build-doc.sh

  serve_one() {
    local doc_dir="$1"
    local host_port="$2"
    docker run -d --rm \
      -p "${host_port}:3000" \
      -v "${doc_dir}:/site" \
      -w /site \
      "${DOCS_CI_IMAGE}" \
      bash -lc "yarn serve --host 0.0.0.0 --port 3000 --no-open"
  }

  ZH_CID=$(serve_one "${ZH_DOC_DIR}" "${ZH_PORT}")
  EN_CID=$(serve_one "${EN_DOC_DIR}" "${EN_PORT}")

  cleanup_preview() {
    docker stop "${ZH_CID}" "${EN_CID}" >/dev/null 2>&1 || true
  }
  trap cleanup_preview EXIT INT TERM

  cat <<EOF
==========================================================
docs preview ready (Ctrl+C to stop both servers)
  中文站点: http://localhost:${ZH_PORT}
  English : http://localhost:${EN_PORT}
==========================================================
EOF

  docker wait "${ZH_CID}" "${EN_CID}" >/dev/null
fi
