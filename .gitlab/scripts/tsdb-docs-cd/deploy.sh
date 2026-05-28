#!/usr/bin/env bash
# =============================================================================
# tsdb-docs-cd / deploy.sh — HOST-SIDE entry point for docs CD
#
# Orchestrates: docker build → host rsync → Feishu notification.
# Called directly by the GitLab job script (NOT via run-in-docker.sh).
#
# Usage:
#   deploy.sh --lang zh|en
#
# Env / CI variables consumed:
#   DEPLOY_DRY_RUN=1                      skip the actual rsync, just log
#   DOCS_CD_STATE_DIR                     persistent state dir (default /root/gitlab_doc_cd_state)
#   DOCS_FEISHU_WEBHOOK_URL               fetched from Infisical via OIDC at runtime
#   CI_JOB_JWT_V2                         GitLab OIDC token (auto-injected)
# =============================================================================
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
source "${ROOT}/.gitlab/scripts/tsdb-docs-ci/common.sh"
source "${ROOT}/.gitlab/scripts/tsdb-docs-cd/branch-notify.sh"

# -----------------------------------------------------------------------------
# Deploy infra (baked-in; not secrets).
# -----------------------------------------------------------------------------
# Production
: "${ZH_DEPLOY_TARGET:=root@101.200.125.16:/data/cndocs}"
: "${EN_DEPLOY_TARGET:=ubuntu@20.124.239.6:/data/endocs}"
: "${ZH_DEPLOY_URL_PREFIX:=https://docs.taosdata.com}"
: "${EN_DEPLOY_URL_PREFIX:=https://docs.tdengine.com}"
# Staging (192.168.1.131, SSH already configured from 0.30)
: "${ZH_STAGING_TARGET:=root@192.168.1.131:/data/chr/cndocs}"
: "${EN_STAGING_TARGET:=root@192.168.1.131:/data/chr/endocs}"
: "${ZH_STAGING_URL_PREFIX:=http://192.168.1.131:8031}"
: "${EN_STAGING_URL_PREFIX:=http://192.168.1.131:8032}"

LANG_ARG=""
DEPLOY_ENV="${DEPLOY_ENV:-staging}"
while [ $# -gt 0 ]; do
  case "$1" in
    --lang) LANG_ARG="${2:-}"; shift 2 ;;
    --lang=*) LANG_ARG="${1#*=}"; shift ;;
    --env) DEPLOY_ENV="${2:-}"; shift 2 ;;
    --env=*) DEPLOY_ENV="${1#*=}"; shift ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "deploy.sh: unknown argument: $1" >&2; exit 2 ;;
  esac
done

case "${LANG_ARG}" in
  zh)
    DOC_DIR="${ZH_DOC_DIR}"
    DOC_BRANCH="${ZH_DOC_BRANCH}"
    DOC_REPO_URL="${ZH_DOC_REPO_URL}"
    if [ "${DEPLOY_ENV}" = "production" ]; then
      DEPLOY_TARGET="${ZH_DEPLOY_TARGET}"
      URL_PREFIX="${ZH_DEPLOY_URL_PREFIX}"
    else
      DEPLOY_TARGET="${ZH_STAGING_TARGET}"
      URL_PREFIX="${ZH_STAGING_URL_PREFIX}"
    fi
    ;;
  en)
    DOC_DIR="${EN_DOC_DIR}"
    DOC_BRANCH="${EN_DOC_BRANCH}"
    DOC_REPO_URL="${EN_DOC_REPO_URL}"
    if [ "${DEPLOY_ENV}" = "production" ]; then
      DEPLOY_TARGET="${EN_DEPLOY_TARGET}"
      URL_PREFIX="${EN_DEPLOY_URL_PREFIX}"
    else
      DEPLOY_TARGET="${EN_STAGING_TARGET}"
      URL_PREFIX="${EN_STAGING_URL_PREFIX}"
    fi
    ;;
  *)
    echo "deploy.sh: --lang must be zh or en (got: '${LANG_ARG}')" >&2
    exit 2
    ;;
esac

echo "deploy.sh: env=${DEPLOY_ENV} lang=${LANG_ARG} target=${DEPLOY_TARGET}"

DOCS_CD_STATE_DIR="${DOCS_CD_STATE_DIR:-/root/gitlab_doc_cd_state}"

# -----------------------------------------------------------------------------
# Fetch secrets from Infisical via OIDC (zero stored credentials).
# -----------------------------------------------------------------------------
INFISICAL_URL="${INFISICAL_URL:-https://infisical.tdengine.net}"
INFISICAL_SECRET_PATH="${INFISICAL_SECRET_PATH:-/docs-ci-cd}"
INFISICAL_ENV="${INFISICAL_ENV:-prod}"
INFISICAL_IDENTITY_ID="${INFISICAL_IDENTITY_ID:-2660b85d-c815-4841-9715-3cf717f30aa2}"
INFISICAL_PROJECT_ID="${INFISICAL_PROJECT_ID:-5175f9f6-2f05-41ab-a2c2-e4de427e2dcd}"

fetch_infisical_secret() {
  local secret_name="$1"
  if ! command -v jq >/dev/null 2>&1; then
    echo "fetch_infisical_secret: jq not found on host; install with: apt-get install -y jq" >&2
    return 1
  fi
  local login_response
  login_response=$(curl -sS --connect-timeout 10 -X POST "${INFISICAL_URL}/api/v1/auth/oidc-auth/login" \
    -H 'Content-Type: application/json' \
    -d "{\"identityId\":\"${INFISICAL_IDENTITY_ID}\",\"jwt\":\"${CI_JOB_JWT_V2}\"}")
  local token
  token=$(printf '%s' "${login_response}" | jq -r '.accessToken // empty')
  if [ -z "${token}" ]; then
    echo "fetch_infisical_secret: OIDC login failed. Response: ${login_response}" >&2
    return 1
  fi
  local secret_response
  secret_response=$(curl -sS --connect-timeout 10 \
    "${INFISICAL_URL}/api/v3/secrets/raw/${secret_name}?environment=${INFISICAL_ENV}&secretPath=${INFISICAL_SECRET_PATH}&workspaceId=${INFISICAL_PROJECT_ID}" \
    -H "Authorization: Bearer ${token}")
  local value
  value=$(printf '%s' "${secret_response}" | jq -r '.secret.secretValue // empty')
  if [ -z "${value}" ]; then
    echo "fetch_infisical_secret: secret '${secret_name}' not found or empty. Response: ${secret_response}" >&2
    return 1
  fi
  printf '%s' "${value}"
}

if [ -n "${CI_JOB_JWT_V2:-}" ] && [ -z "${DOCS_FEISHU_WEBHOOK_URL:-}" ]; then
  echo "deploy.sh: CI_JOB_JWT_V2 present (${#CI_JOB_JWT_V2} chars), fetching webhook URL from Infisical..."
  DOCS_FEISHU_WEBHOOK_URL=$(fetch_infisical_secret "DOCS_FEISHU_WEBHOOK_URL") || true
  export DOCS_FEISHU_WEBHOOK_URL
  if [ -n "${DOCS_FEISHU_WEBHOOK_URL}" ]; then
    echo "deploy.sh: webhook URL fetched successfully"
  else
    echo "deploy.sh: webhook URL fetch returned empty" >&2
  fi
elif [ -z "${CI_JOB_JWT_V2:-}" ]; then
  echo "deploy.sh: CI_JOB_JWT_V2 not available; cannot fetch secrets from Infisical" >&2
fi

# -----------------------------------------------------------------------------
# Feishu notification helper.
# -----------------------------------------------------------------------------
notify_feishu() {
  local text="$1"
  if [ -z "${DOCS_FEISHU_WEBHOOK_URL:-}" ]; then
    echo "notify_feishu: DOCS_FEISHU_WEBHOOK_URL not set; skipping. Message:"
    echo "${text}"
    return 0
  fi
  local payload
  if command -v jq >/dev/null 2>&1; then
    payload=$(jq -nc --arg t "${text}" '{msg_type:"text",content:{text:$t}}')
  else
    local escaped
    escaped=$(printf '%s' "${text}" | sed 's/\\/\\\\/g; s/"/\\"/g' | awk 'BEGIN{ORS="\\n"}{print}')
    payload="{\"msg_type\":\"text\",\"content\":{\"text\":\"${escaped%\\n}\"}}"
  fi
  curl -sS -X POST -H 'Content-Type: application/json' \
    -d "${payload}" "${DOCS_FEISHU_WEBHOOK_URL}" >/dev/null || \
    echo "notify_feishu: webhook POST failed" >&2
}

on_error() {
  local rc=$?
  notify_feishu "❌ docs-cd ${LANG_ARG} FAILED
job: ${CI_JOB_URL:-<no-url>}
commit: ${CI_COMMIT_SHORT_SHA:-?}
exit: ${rc}"
  exit "${rc}"
}
trap on_error ERR

# =============================================================================
# Phase 1: Host-side repo prep + Docker build
# =============================================================================
: "${DOCS_CI_IMAGE:?set project/group CI variable DOCS_CI_IMAGE}"

prepare_docs_repo_on_host "${DOC_REPO_URL}" "${DOC_DIR}" "${DOC_BRANCH}"

export DEPLOY_LANG="${LANG_ARG}"
bash "${ROOT}/.gitlab/scripts/tsdb-docs-ci/run-in-docker.sh" \
  bash .gitlab/scripts/tsdb-docs-cd/build-remote.sh --lang "${LANG_ARG}"

BUILD_OUTPUT_DIR="${DOC_DIR}/build"
if [ ! -d "${BUILD_OUTPUT_DIR}" ]; then
  echo "deploy.sh: build output not found at ${BUILD_OUTPUT_DIR}" >&2
  exit 1
fi

# =============================================================================
# Phase 2: Content diff (host-side, persistent state dir)
# =============================================================================
mkdir -p "${DOCS_CD_STATE_DIR}"
STATE_FILE="${DOCS_CD_STATE_DIR}/${LANG_ARG}.sha256"
NEW_STATE_FILE="${DOCS_CD_STATE_DIR}/${LANG_ARG}.sha256.new"
BRANCH_STATE_FILE="${DOCS_CD_STATE_DIR}/${LANG_ARG}.branches"
NEW_BRANCH_STATE_FILE="${DOCS_CD_STATE_DIR}/${LANG_ARG}.branches.new"
ASSEMBLE_CONFIG="${DOC_DIR}/assemble_config.json"

if [ ! -f "${ASSEMBLE_CONFIG}" ]; then
  echo "deploy.sh: assemble config not found at ${ASSEMBLE_CONFIG}" >&2
  exit 1
fi

(
  cd "${BUILD_OUTPUT_DIR}"
  find . -type f -name '*.html' -print0 | sort -z | xargs -0 sha256sum
) > "${NEW_STATE_FILE}"

collect_current_branch_state "${TSDB_REPO_URL}" "${ASSEMBLE_CONFIG}" "${NEW_BRANCH_STATE_FILE}"

CHANGED_URLS=()
CHANGED_TARGETS=()
FIRST_RUN=0
if [ -f "${STATE_FILE}" ]; then
  while IFS= read -r line; do
    rel="${line#*  ./}"
    CHANGED_URLS+=("${rel}")
  done < <(comm -13 <(sort "${STATE_FILE}") <(sort "${NEW_STATE_FILE}"))
else
  FIRST_RUN=1
fi

if [ -f "${BRANCH_STATE_FILE}" ]; then
  while IFS= read -r target; do
    [ -n "${target}" ] && CHANGED_TARGETS+=("${target}")
  done < <(classify_changed_targets "${BRANCH_STATE_FILE}" "${NEW_BRANCH_STATE_FILE}")
fi

# =============================================================================
# Phase 3: rsync (host-side, has SSH access)
# =============================================================================
if [ "${DEPLOY_DRY_RUN:-0}" = "1" ]; then
  echo "deploy.sh: DRY RUN — would rsync ${BUILD_OUTPUT_DIR}/ → ${DEPLOY_TARGET}"
  rm -f "${NEW_STATE_FILE}" "${NEW_BRANCH_STATE_FILE}"
  exit 0
fi

rsync -av --delete \
  -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
  "${BUILD_OUTPUT_DIR}/" "${DEPLOY_TARGET}"

# rsync succeeded — promote the snapshot.
mv "${NEW_STATE_FILE}" "${STATE_FILE}"
mv "${NEW_BRANCH_STATE_FILE}" "${BRANCH_STATE_FILE}"

# =============================================================================
# Phase 4: Notification
# =============================================================================
if [ "${FIRST_RUN}" = "1" ]; then
  notify_feishu "✅ docs-cd ${LANG_ARG} baseline initialized
target: ${DEPLOY_TARGET}
job: ${CI_JOB_URL:-<no-url>}
(no URL diff this run — recording snapshot for next time)"
elif [ "${#CHANGED_TARGETS[@]}" -eq 0 ]; then
  echo "deploy.sh: no tracked tsdb branch changes; skipping success notification."
else
  notify_feishu "$(build_success_notification "${LANG_ARG}" "${CI_JOB_URL:-<no-url>}" "${URL_PREFIX}" "${CHANGED_TARGETS[@]}")"
fi
