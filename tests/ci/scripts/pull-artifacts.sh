#!/usr/bin/env bash
# =============================================================================
# pull-artifacts.sh — 从 Nexus 下载构建产物到测试 worker
# =============================================================================
# 下载两份 tar.gz 并解压为 run_container.sh 所需的目录结构：
#   $WORKDIR/debugNoSan/build/bin/   — 非 ASAN 可执行文件
#   $WORKDIR/debugNoSan/build/lib/   — 非 ASAN 动态链接库
#   $WORKDIR/debugSan/build/bin/     — ASAN 可执行文件（debugSan 二进制内置 ASAN）
#   $WORKDIR/debugSan/build/lib/     — ASAN 动态链接库
#
# Nexus 目录结构：
#   ${BASE_PATH}/noasan/linux-x64-noasan.tar.gz
#   ${BASE_PATH}/asan/linux-x64-asan.tar.gz
#
# tar 内部结构：debug/build/bin/*, debug/build/lib/*, debug/build/share/*
# 解压后重命名：debug/ → debugNoSan/ 或 debugSan/
#
# 可选环境变量：
#   OVERRIDE_NEXUS_PATH — 直接覆盖基础路径，跳过自动计算
# =============================================================================
set -euo pipefail

WORKDIR="${WORKDIR:?WORKDIR is required}"
NEXUS_URL="${NEXUS_URL:-https://nexus.tdengine.net}"
NEXUS_REPO="${NEXUS_REPO:-tdtest}"

# --------------------------------------------------
# 计算 Nexus 基础路径（OVERRIDE_NEXUS_PATH 优先）
# --------------------------------------------------
if [[ -n "${OVERRIDE_NEXUS_PATH:-}" ]]; then
    BASE_PATH="${OVERRIDE_NEXUS_PATH}"
    echo "[pull-artifacts] Using OVERRIDE_NEXUS_PATH: ${BASE_PATH}"
elif [[ "${CI_PIPELINE_SOURCE}" == "merge_request_event" ]]; then
    BASE_PATH="tsdb/ci/mr${CI_MERGE_REQUEST_IID}/linux/x64"
elif [[ "${CI_PIPELINE_SOURCE}" == "schedule" ]]; then
    BASE_PATH="tsdb/daily/$(date +%Y%m%d)/${CI_COMMIT_BRANCH}/linux/x64"
elif [[ "${CI_PIPELINE_SOURCE}" == "web" ]]; then
    BASE_PATH="tsdb/release/${RELEASE_VERSION:-manual}/linux/x64"
else
    BASE_PATH="tsdb/ci/branch-${CI_COMMIT_BRANCH}/linux/x64"
fi

NOSAN_URL="${NEXUS_URL}/repository/${NEXUS_REPO}/${BASE_PATH}/noasan/linux-x64-noasan.tar.gz"
SAN_URL="${NEXUS_URL}/repository/${NEXUS_REPO}/${BASE_PATH}/asan/linux-x64-asan.tar.gz"

echo "[pull-artifacts] Base path: ${BASE_PATH}"
echo "[pull-artifacts] NoAsan:    ${NOSAN_URL}"
echo "[pull-artifacts] Asan:      ${SAN_URL}"
mkdir -p "${WORKDIR}"

# --------------------------------------------------
# 通用下载函数（最多重试 3 次）
# --------------------------------------------------
download_artifact() {
    local url=$1 dest=$2 label=$3
    for attempt in 1 2 3; do
        echo "[pull-artifacts] [${label}] Download attempt ${attempt}/3..."
        if curl -fsSL \
            --retry 2 --retry-delay 5 \
            --connect-timeout 30 --max-time 300 \
            -u "${NEXUS_USERNAME}:${NEXUS_PASSWORD}" \
            "${url}" \
            -o "${dest}"; then
            echo "[pull-artifacts] [${label}] OK ($(du -sh "${dest}" | cut -f1))"
            return 0
        fi
        if [[ ${attempt} -eq 3 ]]; then
            echo "[pull-artifacts] [${label}] ERROR: Failed after 3 attempts"
            return 1
        fi
        echo "[pull-artifacts] [${label}] Retry in 10s..."
        sleep 10
    done
}

# --------------------------------------------------
# 下载并解压 debugNoSan
# --------------------------------------------------
NOSAN_TAR="${WORKDIR}/linux-x64-noasan.tar.gz"
echo ""
echo "=== [NoAsan] Downloading ==="
download_artifact "${NOSAN_URL}" "${NOSAN_TAR}" "noasan"

echo "[pull-artifacts] [noasan] Extracting..."
(cd "${WORKDIR}" && tar xzf linux-x64-noasan.tar.gz)
rm -f "${NOSAN_TAR}"
# tar 内部是 debug/ 结构，重命名为 debugNoSan/
if [[ -d "${WORKDIR}/debug" && ! -d "${WORKDIR}/debugNoSan" ]]; then
    mv "${WORKDIR}/debug" "${WORKDIR}/debugNoSan"
fi

# --------------------------------------------------
# 下载并解压 debugSan
# --------------------------------------------------
SAN_TAR="${WORKDIR}/linux-x64-asan.tar.gz"
echo ""
echo "=== [Asan] Downloading ==="
download_artifact "${SAN_URL}" "${SAN_TAR}" "asan"

echo "[pull-artifacts] [asan] Extracting..."
# san tar 内部也是 debug/ 结构，先解压到临时目录再移动到 debugSan/
TMP_SAN="${WORKDIR}/_san_extract"
mkdir -p "${TMP_SAN}"
(cd "${TMP_SAN}" && tar xzf "${SAN_TAR}")
rm -f "${SAN_TAR}"
if [[ -d "${TMP_SAN}/debug" ]]; then
    [[ -d "${WORKDIR}/debugSan" ]] && rm -rf "${WORKDIR}/debugSan"
    mv "${TMP_SAN}/debug" "${WORKDIR}/debugSan"
fi
rm -rf "${TMP_SAN}"

# --------------------------------------------------
# 验证两套产物
# --------------------------------------------------
echo ""
echo "=== Verification ==="

check_dir() {
    local label=$1 bin_dir=$2 lib_dir=$3
    local ok=0
    [[ -d "${bin_dir}" ]] || { echo "[pull-artifacts] ERROR: ${bin_dir} not found"; ok=1; }
    [[ -d "${lib_dir}" ]] || { echo "[pull-artifacts] ERROR: ${lib_dir} not found"; ok=1; }
    [[ ${ok} -ne 0 ]] && return ${ok}
    echo "[pull-artifacts] [${label}] Binaries: $(ls "${bin_dir}" | wc -l) files"
    echo "[pull-artifacts] [${label}] Libraries: $(ls "${lib_dir}" | wc -l) files"
    echo "[pull-artifacts] [${label}] Key binaries:"
    # 运行时二进制
    for f in taosd taos taosadapter taoskeeper taosBenchmark; do
        if [[ -f "${bin_dir}/${f}" ]]; then
            echo "  [OK] ${f}: $(du -sh "${bin_dir}/${f}" | cut -f1)"
        else
            echo "  [MISSING] ${f}"
        fi
    done
    # ctest 测试二进制（对应 legacy transfer_debug_dirs 中显式列出的产物）
    echo "[pull-artifacts] [${label}] ctest binaries:"
    _missing=0
    for f in replay_test sml_test get_db_name_test varbinary_test write_raw_block_test; do
        if [[ -f "${bin_dir}/${f}" ]]; then
            echo "  [OK] ${f}"
        else
            echo "  [MISSING] ${f}"
            _missing=$((_missing+1))
        fi
    done
    [[ ${_missing} -gt 0 ]] && echo "  WARNING: ${_missing} ctest binary(ies) missing"
    return 0
}

check_dir "noasan" "${WORKDIR}/debugNoSan/build/bin" "${WORKDIR}/debugNoSan/build/lib"
check_dir "asan"   "${WORKDIR}/debugSan/build/bin"   "${WORKDIR}/debugSan/build/lib"

echo ""
echo "[pull-artifacts] Done."
