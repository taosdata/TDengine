#!/usr/bin/env bash
# =============================================================================
# sparse-checkout.sh — 在测试 worker 上获取测试代码
# =============================================================================
# 目标目录结构（供 run_container.sh 使用）：
#   $WORKDIR/TDinternal/community/   → 软链接到 source/taos-community
#   run_container.sh 将其挂载为容器内 /mnt/source/taos-community
#
# 实现方式：
#   1. 优先：CI_PROJECT_DIR 已有 source/taos-community/ 且含 cases.task
#      → $WORKDIR/TDinternal/community 软链接到 CI_PROJECT_DIR/source/taos-community
#   2. Fallback：稀疏检出目标分支的 source/taos-community/ 子目录
# =============================================================================
set -euo pipefail

WORKDIR="${WORKDIR:?WORKDIR is required}"
SRC_SUBDIR="source/taos-community"   # tsdb 仓库中社区版代码的路径
TDINTERNAL_DIR="${WORKDIR}/TDinternal"   # 兼容 run_container.sh -e 的目录结构
TDENGINE_DIR="${TDINTERNAL_DIR}/community"  # run_container.sh -e 时期望的路径

echo "[sparse-checkout] WORKDIR=${WORKDIR}"
echo "[sparse-checkout] CI_PROJECT_DIR=${CI_PROJECT_DIR}"
mkdir -p "${WORKDIR}"

# --------------------------------------------------
# 方式 1：CI_PROJECT_DIR 已有 source/taos-community/tests
# --------------------------------------------------
CASES_TASK_IN_PROJ="${CI_PROJECT_DIR}/${SRC_SUBDIR}/tests/parallel_test/cases.task"
if [[ -f "${CASES_TASK_IN_PROJ}" ]]; then
    CASE_COUNT=$(grep -c -v '^#' "${CASES_TASK_IN_PROJ}" || true)
    echo "[sparse-checkout] Found cases.task (${CASE_COUNT} lines) in CI_PROJECT_DIR"
    echo "[sparse-checkout] Symlinking: ${TDENGINE_DIR} -> ${CI_PROJECT_DIR}/${SRC_SUBDIR}"
    rm -rf "${TDINTERNAL_DIR}"
    mkdir -p "${TDINTERNAL_DIR}"
    ln -sf "${CI_PROJECT_DIR}/${SRC_SUBDIR}" "${TDENGINE_DIR}"
    exit 0
fi

# --------------------------------------------------
# 方式 2：稀疏检出目标分支的 source/taos-community/
# --------------------------------------------------
if [[ -n "${CI_MERGE_REQUEST_TARGET_BRANCH_NAME:-}" ]]; then
    REF="${CI_MERGE_REQUEST_TARGET_BRANCH_NAME}"
    echo "[sparse-checkout] MR pipeline: using target branch '${REF}'"
else
    REF="${CI_COMMIT_BRANCH:-main}"
    echo "[sparse-checkout] Push/manual pipeline: using branch '${REF}'"
fi

echo "[sparse-checkout] Sparse-checkout '${SRC_SUBDIR}' from ${CI_REPOSITORY_URL} (ref=${REF})"

CLONE_DIR="${WORKDIR}/tsdb-src"
rm -rf "${CLONE_DIR}"
mkdir -p "${CLONE_DIR}"
cd "${CLONE_DIR}"

git init -q
git remote add origin "${CI_REPOSITORY_URL}"
git config core.sparseCheckout true
mkdir -p .git/info

# 只拉取测试所需子目录
cat > .git/info/sparse-checkout <<SPARSE
${SRC_SUBDIR}/tests/
${SRC_SUBDIR}/include/
SPARSE

git fetch --depth=1 origin "refs/heads/${REF}" 2>&1 | tail -5
git checkout FETCH_HEAD -q

# 验证
CASES_PATH="${CLONE_DIR}/${SRC_SUBDIR}/tests/parallel_test/cases.task"
if [[ ! -f "${CASES_PATH}" ]]; then
    echo "[sparse-checkout] ERROR: cases.task not found at ${CASES_PATH}"
    echo "[sparse-checkout] Contents:"
    ls "${CLONE_DIR}/${SRC_SUBDIR}/" 2>/dev/null || echo "(empty dir)"
    exit 1
fi

CASE_COUNT=$(grep -c -v '^#' "${CASES_PATH}" || true)
echo "[sparse-checkout] OK: ${CASE_COUNT} cases"

# TDinternal/community 软链接 → source/taos-community
rm -rf "${TDINTERNAL_DIR}"
mkdir -p "${TDINTERNAL_DIR}"
ln -sf "${CLONE_DIR}/${SRC_SUBDIR}" "${TDENGINE_DIR}"
echo "[sparse-checkout] Symlink: ${TDENGINE_DIR} -> ${CLONE_DIR}/${SRC_SUBDIR}"

# TDengine/ 软链接：run-test-batch.sh 等脚本用 ${WORKDIR}/TDengine 访问测试文件
ln -sf "${TDENGINE_DIR}" "${WORKDIR}/TDengine"
echo "[sparse-checkout] Symlink: ${WORKDIR}/TDengine -> ${TDENGINE_DIR}"

echo "[sparse-checkout] Test dirs: $(ls "${TDENGINE_DIR}/tests/" | tr '\n' ' ')"
