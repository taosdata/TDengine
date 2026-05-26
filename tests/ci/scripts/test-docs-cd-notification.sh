#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)

DEPLOY_SH="${ROOT}/.gitlab/scripts/tsdb-docs-cd/deploy.sh"
grep -F 'source "${ROOT}/.gitlab/scripts/tsdb-docs-cd/branch-notify.sh"' "${DEPLOY_SH}"
grep -F 'collect_current_branch_state' "${DEPLOY_SH}"
grep -F 'classify_changed_targets' "${DEPLOY_SH}"
grep -F 'build_success_notification' "${DEPLOY_SH}"
grep -F 'job: ${CI_JOB_URL:-<no-url>}' "${DEPLOY_SH}"
grep -F 'deploy.sh: no tracked tsdb branch changes; skipping success notification.' "${DEPLOY_SH}"
grep -F 'notify_feishu "❌ docs-cd ${LANG_ARG} FAILED' "${DEPLOY_SH}"
grep -F 'baseline initialized' "${DEPLOY_SH}"

if grep -F 'affected:' "${DEPLOY_SH}"; then
  echo "success notification must not include affected bucket details" >&2
  exit 1
fi

if grep -F 'changed:' "${DEPLOY_SH}"; then
  echo "success notification must not include changed URL details" >&2
  exit 1
fi

if grep -F 'URLs changed' "${DEPLOY_SH}"; then
  echo "success notification must not include URL counts" >&2
  exit 1
fi

if grep -F 'NOTIFY_MAX_URLS' "${DEPLOY_SH}"; then
  echo "deploy.sh must not keep obsolete NOTIFY_MAX_URLS references" >&2
  exit 1
fi

source "${ROOT}/.gitlab/scripts/tsdb-docs-cd/branch-notify.sh"

state_file=$(mktemp)
new_state_file=$(mktemp)
assemble_config=$(mktemp)
failed_state_file=$(mktemp)
integration_scratch=$(mktemp -d)
trap 'rm -rf "${integration_scratch}"; rm -f "${state_file}" "${new_state_file}" "${assemble_config}" "${failed_state_file}"' EXIT

cat > "${state_file}" <<'EOF'
main	1111111111111111111111111111111111111111
3.0	2222222222222222222222222222222222222222
3.3.6	3333333333333333333333333333333333333333
EOF

# 1. Only main changes => latest
cat > "${new_state_file}" <<'EOF'
main	aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
3.0	2222222222222222222222222222222222222222
3.3.6	3333333333333333333333333333333333333333
EOF
changed_targets=$(classify_changed_targets "${state_file}" "${new_state_file}")
test "${changed_targets}" = "latest"

# 2. Only 3.3.6 changes => latest
cat > "${new_state_file}" <<'EOF'
main	1111111111111111111111111111111111111111
3.0	2222222222222222222222222222222222222222
3.3.6	bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
EOF
changed_targets=$(classify_changed_targets "${state_file}" "${new_state_file}")
test "${changed_targets}" = "latest"

# 3. Only 3.0 changes => next
cat > "${new_state_file}" <<'EOF'
main	1111111111111111111111111111111111111111
3.0	bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
3.3.6	3333333333333333333333333333333333333333
EOF
changed_targets=$(classify_changed_targets "${state_file}" "${new_state_file}")
test "${changed_targets}" = "next"

# 4. 3.0 and a non-main, non-3.0 branch change => latest\nnext
cat > "${new_state_file}" <<'EOF'
main	1111111111111111111111111111111111111111
3.0	bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
3.3.6	bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
EOF
changed_targets=$(classify_changed_targets "${state_file}" "${new_state_file}")
test "${changed_targets}" = $'latest\nnext'

cat > "${assemble_config}" <<'EOF'
{"assembleVersions":[{"branch":"3.0"},{"branch":"main"}]}
EOF

git() {
  if [ "$1" = "ls-remote" ] && [ "$3" = "refs/heads/3.0" ]; then
    printf '%s\t%s\n' "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "refs/heads/3.0"
    return 0
  fi
  if [ "$1" = "ls-remote" ] && [ "$3" = "refs/heads/main" ]; then
    return 0
  fi
  command git "$@"
}

if collect_current_branch_state "origin" "${assemble_config}" "${failed_state_file}"; then
  echo "collect_current_branch_state must fail when a branch cannot be resolved" >&2
  exit 1
fi

if [ -s "${failed_state_file}" ]; then
  echo "collect_current_branch_state must not leave partial state on failure" >&2
  exit 1
fi

unset -f git

message=$(build_success_notification en "https://git.tdengine.net/rd-public/tsdb/-/jobs/35788" "https://docs.tdengine.com" latest next)
expected=$'✅ docs-cd deployed\njob: https://git.tdengine.net/rd-public/tsdb/-/jobs/35788\nClick to visit:\nlatest: https://docs.tdengine.com\nnext: https://docs.tdengine.com/next'
test "${message}" = "${expected}"

message=$(build_success_notification zh "https://git.tdengine.net/rd-public/tsdb/-/jobs/35787" "https://docs.taosdata.com" next)
expected=$'✅ docs-cd deployed\njob: https://git.tdengine.net/rd-public/tsdb/-/jobs/35787\n点击查看:\nnext: https://docs.taosdata.com/next'
test "${message}" = "${expected}"

mkdir -p "${integration_scratch}/bin" "${integration_scratch}/state" "${integration_scratch}/docs.tdengine.com/.git" "${integration_scratch}/docs.tdengine.com/build"
mkdir -p "${integration_scratch}/work"

cat > "${integration_scratch}/docs.tdengine.com/assemble_config.json" <<'EOF'
{"assembleVersions":[{"branch":"3.0"},{"branch":"main"}]}
EOF

cat > "${integration_scratch}/docs.tdengine.com/build/index.html" <<'EOF'
<html>stable</html>
EOF

cat > "${integration_scratch}/bin/bash" <<'EOF'
#!/bin/bash
set -euo pipefail
if [ "$#" -gt 0 ] && [ "$1" = "${RUN_IN_DOCKER_PATH}" ]; then
  exit 0
fi
exec /bin/bash "$@"
EOF

cat > "${integration_scratch}/bin/git" <<'EOF'
#!/bin/bash
set -euo pipefail
if [ "$1" = "ls-remote" ]; then
  if [ "$2" != "${EXPECTED_TSDB_REPO_URL}" ]; then
    exit 0
  fi
  branch="${3#refs/heads/}"
  awk -F '\t' -v branch="${branch}" '$1 == branch { printf "%s\trefs/heads/%s\n", $2, branch }' "${GIT_LS_REMOTE_FILE}"
  exit 0
fi
if [ "$1" = "-C" ] || [ "$1" = "clone" ]; then
  exit 0
fi
command git "$@"
EOF

cat > "${integration_scratch}/bin/rsync" <<'EOF'
#!/bin/bash
set -euo pipefail
exit 0
EOF

cat > "${integration_scratch}/bin/sha256sum" <<'EOF'
#!/bin/bash
set -euo pipefail
for file in "$@"; do
  shasum -a 256 "$file"
done
EOF

chmod +x "${integration_scratch}/bin/bash" "${integration_scratch}/bin/git" "${integration_scratch}/bin/rsync" "${integration_scratch}/bin/sha256sum"

run_deploy() {
  local capture_file="$1"
  shift
  PATH="${integration_scratch}/bin:${PATH}" \
  DOCS_CI_IMAGE="example.invalid/docs-ci:latest" \
  DOCS_CI_WORKDIR="${integration_scratch}/work" \
  DOCS_CD_STATE_DIR="${integration_scratch}/state" \
  EN_DOC_DIR="${integration_scratch}/docs.tdengine.com" \
  EN_DOC_REPO_URL="docs-origin" \
  EN_DOC_BRANCH="main" \
  TSDB_REPO_URL="tsdb-origin" \
  EXPECTED_TSDB_REPO_URL="tsdb-origin" \
  RUN_IN_DOCKER_PATH="${ROOT}/.gitlab/scripts/tsdb-docs-ci/run-in-docker.sh" \
  CI_JOB_URL="$1" \
  GIT_LS_REMOTE_FILE="$2" \
  bash "${DEPLOY_SH}" --lang en --env production > "${capture_file}" 2>&1
}

cat > "${integration_scratch}/branches-run1.tsv" <<'EOF'
3.0	2222222222222222222222222222222222222222
main	1111111111111111111111111111111111111111
EOF

run_deploy "${integration_scratch}/run1.out" "https://job.example/1" "${integration_scratch}/branches-run1.tsv"
grep -F 'baseline initialized' "${integration_scratch}/run1.out"

cat > "${integration_scratch}/branches-run2.tsv" <<'EOF'
3.0	2222222222222222222222222222222222222222
main	aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
EOF

run_deploy "${integration_scratch}/run2.out" "https://job.example/2" "${integration_scratch}/branches-run2.tsv"
grep -F '✅ docs-cd deployed' "${integration_scratch}/run2.out"
grep -F 'job: https://job.example/2' "${integration_scratch}/run2.out"
grep -F 'Click to visit:' "${integration_scratch}/run2.out"
grep -F 'latest: https://docs.tdengine.com' "${integration_scratch}/run2.out"
if grep -F 'next:' "${integration_scratch}/run2.out"; then
  echo "latest-only branch change must not include next link" >&2
  exit 1
fi
if grep -F 'skipping success notification' "${integration_scratch}/run2.out"; then
  echo "branch changes must notify even when HTML output is unchanged" >&2
  exit 1
fi

cat > "${integration_scratch}/docs.tdengine.com/build/index.html" <<'EOF'
<html>html-drift-only</html>
EOF

run_deploy "${integration_scratch}/run3.out" "https://job.example/3" "${integration_scratch}/branches-run2.tsv"
grep -F 'deploy.sh: no tracked tsdb branch changes; skipping success notification.' "${integration_scratch}/run3.out"
if grep -F '✅ docs-cd deployed' "${integration_scratch}/run3.out"; then
  echo "HTML drift alone must not trigger success notification" >&2
  exit 1
fi
