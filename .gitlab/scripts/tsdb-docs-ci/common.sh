#!/usr/bin/env bash
set -euo pipefail

DOCS_CI_WORKDIR_BASE="${DOCS_CI_WORKDIR_BASE:-/root/gitlab_doc_ci_work}"
# Per-runner-slot workspace to avoid concurrent pipelines racing on the
# shared docs.{taosdata,tdengine}.com checkouts and assembled docs/ tree.
DOCS_CI_WORKDIR="${DOCS_CI_WORKDIR:-${DOCS_CI_WORKDIR_BASE}/slot-${CI_CONCURRENT_PROJECT_ID:-default}}"
TSDB_REPO_URL="${TSDB_REPO_URL:-https://git.tdengine.net/rd-public/tsdb.git}"
ZH_DOC_REPO_URL="${ZH_DOC_REPO_URL:-https://github.com/taosdata/docs.taosdata.com.git}"
EN_DOC_REPO_URL="${EN_DOC_REPO_URL:-https://github.com/taosdata/docs.tdengine.com.git}"
ZH_DOC_BRANCH="${ZH_DOC_BRANCH:-master}"
EN_DOC_BRANCH="${EN_DOC_BRANCH:-main}"

TSDB_DIR="${TSDB_DIR:-${DOCS_CI_WORKDIR}/tsdb}"
ZH_DOC_DIR="${ZH_DOC_DIR:-${DOCS_CI_WORKDIR}/docs.taosdata.com}"
EN_DOC_DIR="${EN_DOC_DIR:-${DOCS_CI_WORKDIR}/docs.tdengine.com}"
TSDB_DOCS_DIR="${TSDB_DIR}/source/taos-community/docs"

ensure_repo() {
  local repo_url="$1"
  local repo_dir="$2"

  if [ ! -d "${repo_dir}/.git" ]; then
    git clone "${repo_url}" "${repo_dir}"
  fi
}

prepare_docs_repo_on_host() {
  local repo_url="$1"
  local repo_dir="$2"
  local branch="$3"

  mkdir -p "${DOCS_CI_WORKDIR}"
  ensure_repo "${repo_url}" "${repo_dir}"
  git -C "${repo_dir}" remote set-url origin "${repo_url}"
  git -C "${repo_dir}" fetch origin "${branch}" --prune
  git -C "${repo_dir}" reset --hard FETCH_HEAD
  git -C "${repo_dir}" clean -fd
  git -C "${repo_dir}" checkout -B "${branch}" FETCH_HEAD
}

changed_doc_files() {
  # Guard against unset CI variables under set -u. If the MR base SHA or commit
  # SHA is not available, we cannot determine which docs files changed. In this
  # case, emit a sentinel path under the docs subtree so callers take the
  # conservative path of building both zh and en. Also emit a clear diagnostic
  # explaining the fallback.
  if [ -z "${CI_MERGE_REQUEST_DIFF_BASE_SHA:-}" ] || [ -z "${CI_COMMIT_SHA:-}" ]; then
    >&2 printf 'CI_MERGE_REQUEST_DIFF_BASE_SHA or CI_COMMIT_SHA not set; unable to determine changed docs files, falling back to building both zh and en (conservative)\n'
    # Print a sentinel path that matches the docs subtree so changed_doc_languages
    # will conservatively enable both builds.
    printf 'source/taos-community/docs/ci-vars-unset\n'
    return 0
  fi

  git diff --name-only "${CI_MERGE_REQUEST_DIFF_BASE_SHA}" "${CI_COMMIT_SHA}" -- source/taos-community/docs
}

changed_markdown_files() {
  changed_doc_files | grep -E '\.(md|mdx)$' || true
}

changed_doc_languages() {
  local zh_changed=false
  local en_changed=false
  local file

  while IFS= read -r file; do
    case "${file}" in
      source/taos-community/docs/zh/*) zh_changed=true ;;
      source/taos-community/docs/en/*) en_changed=true ;;
      source/taos-community/docs/examples/*)
        zh_changed=true
        en_changed=true
        ;;
      # If a changed file is under the docs subtree but not in a known
      # language-specific directory, conservatively trigger both builds.
      source/taos-community/docs/*)
        zh_changed=true
        en_changed=true
        ;;
    esac
  done < <(changed_doc_files)

  printf 'zh=%s\nen=%s\n' "${zh_changed}" "${en_changed}"
}
