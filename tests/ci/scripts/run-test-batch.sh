#!/usr/bin/env bash
# =============================================================================
# run-test-batch.sh — modulo 分配用例到当前节点，节点内并发执行，生成 JUnit XML
# =============================================================================
# 环境变量：
#   WORKDIR            — 工作目录（已含 TDengine/ 软链接和 debugNoSan/）
#   CI_NODE_INDEX      — GitLab 1-based 节点编号 (1..N)
#   CI_NODE_TOTAL      — 并行总节点数
#   CI_PROJECT_DIR     — GitLab runner checkout 目录（artifacts 输出根）
#   SANITIZER          — y|n，默认 n
#   TEST_CONCURRENCY   — 节点内同时运行的 docker 容器数，默认 4
# =============================================================================
set -uo pipefail

WORKDIR="${WORKDIR:?WORKDIR is required}"
TEST_CONCURRENCY="${TEST_CONCURRENCY:-4}"

# GitLab CI_NODE_INDEX 是 1-based
NODE_INDEX="${CI_NODE_INDEX:-1}"
NODE_TOTAL="${CI_NODE_TOTAL:-1}"
NODE_INDEX_0=$(( NODE_INDEX - 1 ))   # 0-based，用于取模

RESULTS_DIR="${CI_PROJECT_DIR}/results"
LOGS_DIR="${RESULTS_DIR}/logs"
TDENGINE_DIR="${WORKDIR}/TDengine"
CASES_TASK="${TDENGINE_DIR}/tests/parallel_test/cases.task"
RUN_CONTAINER="${TDENGINE_DIR}/tests/parallel_test/run_container.sh"
BATCH_FILE="${WORKDIR}/batch-${NODE_INDEX_0}.tsv"
# 并发执行时每个 slot 的状态文件目录
SLOT_DIR="${WORKDIR}/slots"

mkdir -p "${RESULTS_DIR}" "${LOGS_DIR}" "${SLOT_DIR}"

# --------------------------------------------------
# core_pattern 修复（apport 会导致 run_container.sh 以 exit=123 退出）
# --------------------------------------------------
ORIG_CORE_PATTERN=$(cat /proc/sys/kernel/core_pattern 2>/dev/null || true)
if echo "${ORIG_CORE_PATTERN}" | grep -q '^|'; then
    echo "[run-test-batch] core_pattern contains pipe (apport), overriding to /tmp/core.%e.%p"
    echo "/tmp/core.%e.%p" > /proc/sys/kernel/core_pattern 2>/dev/null || \
        echo "[run-test-batch] WARNING: cannot write core_pattern (non-fatal)"
fi
trap '[ -n "${ORIG_CORE_PATTERN}" ] && echo "${ORIG_CORE_PATTERN}" > /proc/sys/kernel/core_pattern 2>/dev/null || true' EXIT

echo "========================================"
echo " Test Batch Runner"
echo " Node:        ${NODE_INDEX} / ${NODE_TOTAL}  (0-based: ${NODE_INDEX_0})"
echo " Concurrency: ${TEST_CONCURRENCY}"
echo " WORKDIR:     ${WORKDIR}"
echo " CI_PROJECT:  ${CI_PROJECT_DIR}"
echo "========================================"

# --------------------------------------------------
# 前置检查
# --------------------------------------------------
err=0
[[ -f "${CASES_TASK}" ]]       || { echo "ERROR: cases.task not found: ${CASES_TASK}"; err=1; }
[[ -f "${RUN_CONTAINER}" ]]    || { echo "ERROR: run_container.sh not found: ${RUN_CONTAINER}"; err=1; }
[[ -d "${WORKDIR}/debugNoSan/build/bin" ]] || { echo "ERROR: artifacts missing: ${WORKDIR}/debugNoSan"; err=1; }
[[ -d "${WORKDIR}/debugSan/build/bin" ]]   || echo "WARNING: debugSan artifacts missing: ${WORKDIR}/debugSan (san=y cases will fail)"
[[ ${err} -eq 0 ]] || exit 1
chmod +x "${RUN_CONTAINER}"

# --------------------------------------------------
# 解析 cases.task，按 modulo 分配本节点用例
# --------------------------------------------------
> "${BATCH_FILE}"
total_global=0

while IFS= read -r line; do
    [[ "${line}" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line// }" ]] && continue
    san=$(echo "${line}"  | cut -d, -f3 | tr -d ' ')
    path=$(echo "${line}" | cut -d, -f4 | tr -d ' ')
    cmd=$(echo "${line}"  | cut -d, -f5-)
    [[ -z "${path}" || -z "${cmd}" ]] && continue
    # modulo 分配；san 标志随 case 一起写入 BATCH_FILE（第3列），run_case_in_slot 按此选择 debugSan/debugNoSan
    if (( total_global % NODE_TOTAL == NODE_INDEX_0 )); then
        printf '%s\t%s\t%s\n' "${path}" "${cmd}" "${san:-n}" >> "${BATCH_FILE}"
    fi
    total_global=$(( total_global + 1 ))
done < "${CASES_TASK}"

BATCH_COUNT=$(wc -l < "${BATCH_FILE}")
echo ""
echo "Cases assigned to this node: ${BATCH_COUNT} (of ${total_global} total)"
echo "  Modulo: index=${NODE_INDEX_0}, total=${NODE_TOTAL}"
echo ""

if [[ ${BATCH_COUNT} -eq 0 ]]; then
    echo "No cases assigned. Exiting."
    echo '<?xml version="1.0" encoding="UTF-8"?><testsuite name="empty" tests="0" failures="0" errors="0" time="0"/>' \
        > "${RESULTS_DIR}/junit-${NODE_INDEX}.xml"
    exit 0
fi

# --------------------------------------------------
# 并发执行
# --------------------------------------------------
# 每个 case 运行在独立 slot（thread_no），避免 sim/coredump 目录冲突。
# thread_no = NODE_INDEX_0 * TEST_CONCURRENCY + slot_id
#   slot_id ∈ [0, TEST_CONCURRENCY-1]
#
# 状态文件: ${SLOT_DIR}/slot-{slot_id}.{pid,rc,elapsed,label,log}

# 初始化 slot 状态数组（0 = free）
declare -a SLOT_PIDS
for (( s=0; s<TEST_CONCURRENCY; s++ )); do
    SLOT_PIDS[$s]=0
done

pass_count=0
fail_count=0
junit_cases=""
overall_start=$(date +%s)

# 每个 case 的序号（用于唯一标识 log 文件）
case_seq=0
# 所有已启动但未收割的 case 数量
in_flight=0
# harvest_one 的输出：通过全局变量传递，避免命令替换子 shell 导致 SLOT_PIDS 修改丢失
FINISHED_SLOT=-1

# --------------------------------------------------
# Pipeline cancel 处理：SIGTERM 时停止本 job 的所有容器
# 容器命名规则：tdci-{CI_JOB_ID}-t{thread_no}
# 宿主机日志路径：WORKDIR/tmp/thread_volume/{thread_no}/taoslog/  (实时挂载)
#             和：WORKDIR/tmp/thread_volume/{thread_no}/sim/var_taoslog/
# --------------------------------------------------
# 容器名含 MR 号方便跨 PR 识别：tdci-mr37-710-t0
# CI_MERGE_REQUEST_IID 仅 MR pipeline 有值；push/web pipeline 用 branch 名兜底
_MR_PART="${CI_MERGE_REQUEST_IID:+mr${CI_MERGE_REQUEST_IID}}"
_MR_PART="${_MR_PART:-branch}"
JOB_CONTAINER_PREFIX="tdci-${_MR_PART}-${CI_JOB_ID:-local}"
cancel_handler() {
    echo ""
    echo "[run-test-batch] *** Job cancelled — stopping all containers for ${JOB_CONTAINER_PREFIX} ***"
    docker ps --filter "name=${JOB_CONTAINER_PREFIX}" --format "{{.Names}}" 2>/dev/null \
        | xargs -r docker stop --time 15 2>/dev/null || true
    sleep 2
    docker ps --filter "name=${JOB_CONTAINER_PREFIX}" --format "{{.Names}}" 2>/dev/null \
        | xargs -r docker kill 2>/dev/null || true
    echo "[run-test-batch] Containers stopped. Host-side logs:"
    echo "  ${WORKDIR}/tmp/thread_volume/*/taoslog/         (taosd 实时日志)"
    echo "  ${WORKDIR}/tmp/thread_volume/*/sim/var_taoslog/ (run_case.sh 结束时拷贝)"
    exit 130
}
trap cancel_handler SIGTERM SIGINT

# 实际读取 batch 文件，启动 & 收割
run_case_in_slot() {
    local slot=$1 path=$2 cmd=$3 san=$4 seq=$5
    local thread_no=$(( NODE_INDEX_0 * TEST_CONCURRENCY + slot ))
    local container_name="${JOB_CONTAINER_PREFIX}-t${thread_no}"
    local log_file="${SLOT_DIR}/case-${seq}.log"
    local start_ms=$(date +%s%3N)

    bash "${RUN_CONTAINER}" \
        -w "${WORKDIR}" \
        -d "${path}" \
        -c "${cmd}" \
        -s "${san}" \
        -t "${thread_no}" \
        -n "${container_name}" \
        > "${log_file}" 2>&1
    local rc=$?
    local elapsed_ms=$(( $(date +%s%3N) - start_ms ))

    echo "${rc}"          > "${SLOT_DIR}/slot-${slot}.rc"
    echo "${elapsed_ms}"  > "${SLOT_DIR}/slot-${slot}.elapsed"
    echo "${path}::${cmd}" > "${SLOT_DIR}/slot-${slot}.label"
    echo "${log_file}"    > "${SLOT_DIR}/slot-${slot}.log"
    exit ${rc}
}
export -f run_case_in_slot

find_free_slot() {
    for (( s=0; s<TEST_CONCURRENCY; s++ )); do
        if [[ ${SLOT_PIDS[$s]} -eq 0 ]]; then
            echo $s; return
        fi
    done
    echo -1
}

harvest_one() {
    # 等待任意一个后台子进程结束，结果写入全局变量 FINISHED_SLOT
    # 注意：必须直接调用（不能用 $(harvest_one)），否则子 shell 无法修改父进程的 SLOT_PIDS
    while true; do
        for (( s=0; s<TEST_CONCURRENCY; s++ )); do
            if [[ ${SLOT_PIDS[$s]} -ne 0 ]]; then
                if ! kill -0 ${SLOT_PIDS[$s]} 2>/dev/null; then
                    wait ${SLOT_PIDS[$s]} 2>/dev/null || true
                    SLOT_PIDS[$s]=0
                    FINISHED_SLOT=$s
                    return
                fi
            fi
        done
        sleep 0.2
    done
}

process_finished_slot() {
    local slot=$1
    local rc=0 elapsed_ms=0 label="" log_file=""
    [[ -f "${SLOT_DIR}/slot-${slot}.rc" ]]      && rc=$(cat "${SLOT_DIR}/slot-${slot}.rc")
    [[ -f "${SLOT_DIR}/slot-${slot}.elapsed" ]]  && elapsed_ms=$(cat "${SLOT_DIR}/slot-${slot}.elapsed")
    [[ -f "${SLOT_DIR}/slot-${slot}.label" ]]    && label=$(cat "${SLOT_DIR}/slot-${slot}.label")
    [[ -f "${SLOT_DIR}/slot-${slot}.log" ]]      && log_file=$(cat "${SLOT_DIR}/slot-${slot}.log")

    local elapsed_s="$(( elapsed_ms / 1000 )).$(printf '%03d' $(( elapsed_ms % 1000 )))"
    local path="${label%%::*}"
    local safe_name=$(echo "${label}" | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g')
    local safe_path=$(echo "${path}" | sed 's/&/\&amp;/g')

    if [[ ${rc} -eq 0 ]]; then
        echo "  => PASS (${elapsed_s}s)  [${label}]"
        junit_cases+="    <testcase name=\"${safe_name}\" classname=\"${safe_path}\" time=\"${elapsed_s}\"/>"$'\n'
        pass_count=$(( pass_count + 1 ))
        rm -f "${log_file}" "${SLOT_DIR}/slot-${slot}."* 2>/dev/null
    else
        echo "  => FAIL (exit=${rc}, ${elapsed_s}s)  [${label}]"
        # 保存失败日志
        local safe_fname=$(echo "${path}_${slot}" | tr '/ ' '_')
        local fail_log="${LOGS_DIR}/fail-node${NODE_INDEX}-${safe_fname}.log"
        cp "${log_file}" "${fail_log}" 2>/dev/null || true
        # Artifact URL（GitLab artifacts 只能在 job 完成后通过 UI 访问，此处写相对路径）
        local artifact_rel="results/logs/$(basename "${fail_log}")"
        local log_tail=$(tail -50 "${fail_log}" 2>/dev/null \
            | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g' | head -c 4096)
        junit_cases+="    <testcase name=\"${safe_name}\" classname=\"${safe_path}\" time=\"${elapsed_s}\">"$'\n'
        junit_cases+="      <failure message=\"exit code ${rc}\">"$'\n'
        junit_cases+="Log artifact: ${artifact_rel}"$'\n'
        junit_cases+="${log_tail}"$'\n'
        junit_cases+="      </failure>"$'\n'
        junit_cases+="    </testcase>"$'\n'
        fail_count=$(( fail_count + 1 ))
        rm -f "${log_file}" "${SLOT_DIR}/slot-${slot}."* 2>/dev/null
    fi
}

# 读取所有 case 并以 slot 池方式并发启动
case_seq=0
while IFS=$'\t' read -r path cmd san || [[ -n "${path}" ]]; do
    [[ -z "${path}" ]] && continue
    san="${san:-n}"  # 默认非 ASAN

    echo "------------------------------------------------------------"
    echo "  [seq=${case_seq}] [san=${san}] ${path}::${cmd}"

    # 找空闲 slot；若满则等待一个完成
    slot=$(find_free_slot)
    if [[ $slot -eq -1 ]]; then
        harvest_one   # 直接调用，修改全局 SLOT_PIDS 和 FINISHED_SLOT
        process_finished_slot "${FINISHED_SLOT}"
        in_flight=$(( in_flight - 1 ))
        slot=$(find_free_slot)
    fi

    # 启动 case 到后台
    ( run_case_in_slot "${slot}" "${path}" "${cmd}" "${san}" "${case_seq}" ) &
    SLOT_PIDS[$slot]=$!
    in_flight=$(( in_flight + 1 ))
    case_seq=$(( case_seq + 1 ))

done < "${BATCH_FILE}"

# 等待所有 in-flight case 完成
while (( in_flight > 0 )); do
    harvest_one   # 直接调用，修改全局 SLOT_PIDS 和 FINISHED_SLOT
    process_finished_slot "${FINISHED_SLOT}"
    in_flight=$(( in_flight - 1 ))
done

overall_end=$(date +%s)
overall_elapsed=$(( overall_end - overall_start ))
total_cases=$(( pass_count + fail_count ))

# --------------------------------------------------
# JUnit XML 输出
# --------------------------------------------------
JUNIT_FILE="${RESULTS_DIR}/junit-${NODE_INDEX}.xml"
cat > "${JUNIT_FILE}" <<JUNIT
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="test-linux-node${NODE_INDEX}-of-${NODE_TOTAL}"
           tests="${total_cases}"
           failures="${fail_count}"
           errors="0"
           skipped="0"
           time="${overall_elapsed}">
${junit_cases}</testsuite>
JUNIT

echo ""
echo "========================================"
echo " Summary — Node ${NODE_INDEX}/${NODE_TOTAL}  (0-based: ${NODE_INDEX_0})"
echo "   Total:  ${total_cases}"
echo "   Pass:   ${pass_count}"
echo "   Fail:   ${fail_count}"
echo "   Time:   ${overall_elapsed}s"
echo "========================================"

[[ ${fail_count} -gt 0 ]] && { echo "FAILED: ${fail_count} case(s)"; exit 1; }
echo "All tests passed."
