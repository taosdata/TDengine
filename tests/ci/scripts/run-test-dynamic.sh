#!/usr/bin/env bash
# =============================================================================
# run-test-dynamic.sh — 从协调器动态拉取用例，节点内并发执行，生成 JUnit XML
# =============================================================================
# 用于替代 run-test-batch.sh（静态 modulo 分配）
#
# 环境变量：
#   WORKDIR              — 工作目录（已含 TDinternal/community/ 软链接和 debugNoSan/）
#   CI_NODE_INDEX        — 节点编号（仅用于 JUnit 文件命名，1-based）
#   CI_PROJECT_DIR       — GitLab runner checkout 目录（artifacts 输出根）
#   SANITIZER            — y|n，默认 n
#   TEST_CONCURRENCY     — 节点内同时运行的 docker 容器数，默认 4
#   COORDINATOR_URL      — 协调器地址，默认 http://192.168.2.207:<port>
#   CI_MERGE_REQUEST_IID — MR 号（容器命名用）
#   COORDINATOR_PORT     — 手动指定端口（优先于 pipeline ID 计算）
#   POLL_INTERVAL        — 空闲时轮询间隔秒数，默认 3
#   MAX_IDLE_SECONDS     — 无新 case 的最长等待秒数，默认 900
# =============================================================================
set -uo pipefail

WORKDIR="${WORKDIR:?WORKDIR is required}"
SANITIZER="${SANITIZER:-n}"

# ── 本机并发 pipeline 计数器（flock 原子操作）────────────────────────────────
# 所有在同一台 worker 上运行的 CI pipeline job 共享此文件，
# 用于感知当前机器上有多少条 pipeline 在同时跑测试。
# TEST_CONCURRENCY = floor(nproc * 0.8 / active_jobs)，限定在 [2, 16]。
# 这样 10 条并行 PR 和 1 条 PR 都能得到合理的并发数，互不踩踏。
_CI_LOCK_DIR="/data1/tdengine-ci"
mkdir -p "${_CI_LOCK_DIR}" 2>/dev/null || true
_CI_PID_DIR="${_CI_LOCK_DIR}/.ci_pids"
_CI_LOCK_FILE="${_CI_LOCK_DIR}/.ci_active_jobs.lock"
mkdir -p "${_CI_PID_DIR}" 2>/dev/null || true

# 原子注册：写入本进程 PID 文件，统计存活 job 数
# 注：bash 子 shell ( ) 中 $$ 展开为父 shell 的 PID（POSIX 规定），
# 因此即使在 flock 子 shell 里调用，$$ 也是 run-test-dynamic.sh 本体的 PID。
_ci_register_job() {
    local count
    # 先写自己的 PID 文件（在 flock 外，确保 kill -0 能看到）
    echo $$ > "${_CI_PID_DIR}/pid_$$"
    (
        flock -x 9
        count=0
        for _pf in "${_CI_PID_DIR}"/pid_*; do
            [[ -f "${_pf}" ]] || continue
            _pid=$(basename "${_pf}" | sed 's/pid_//')
            if kill -0 "${_pid}" 2>/dev/null; then
                count=$(( count + 1 ))
            else
                rm -f "${_pf}"   # 清理已死进程的 PID 文件
            fi
        done
        [[ ${count} -lt 1 ]] && count=1
        echo "${count}"
    ) 9>>"${_CI_LOCK_FILE}"
}

# 退出时删除 PID 文件
_ci_unregister_job() {
    rm -f "${_CI_PID_DIR}/pid_$$" 2>/dev/null || true
}

# TEST_CONCURRENCY 计算（仅在未显式指定时）
if [[ -z "${TEST_CONCURRENCY:-}" ]]; then
    _ncpus=$(nproc 2>/dev/null || echo 8)
    _active=$(_ci_register_job)   # 注册并拿到当前活跃数
    # 保留 20% 给 OS / 协调器 / 其他进程，剩余平摊给各 pipeline
    _usable=$(( _ncpus * 8 / 10 ))
    _base=$(( _usable / _active ))
    # 上限按核数弹性设定（CI_SCHED_AGGR=0/1/2 对应大机器 60%/70%/80%，小机器固定 16）
    case "${CI_SCHED_AGGR:-1}" in
        0) _max_conc_pct=60 ;;
        2) _max_conc_pct=80 ;;
        *) _max_conc_pct=70 ;;   # aggr=1（默认）
    esac
    _max_conc=$(( _ncpus > 16 ? _ncpus * _max_conc_pct / 100 : 16 ))
    TEST_CONCURRENCY=$(( _base < 2 ? 2 : (_base > _max_conc ? _max_conc : _base) ))
    echo "[run-test-dynamic] auto TEST_CONCURRENCY=${TEST_CONCURRENCY}" \
         "(nproc=${_ncpus}, active_pipelines=${_active}, usable=${_usable}, aggr=${CI_SCHED_AGGR:-1})"
else
    # 显式指定时仍注册计数，以便其他 job 感知
    _ci_register_job > /dev/null
fi
NODE_INDEX="${CI_NODE_INDEX:-1}"
POLL_INTERVAL="${POLL_INTERVAL:-3}"
MAX_IDLE="${MAX_IDLE_SECONDS:-900}"
# 单个用例最长执行时间（秒），超时后强制 kill 容器并标记失败
CASE_TIMEOUT="${CASE_TIMEOUT:-1200}"
# 当前 worker 的能力标签（逗号分隔），协调器依此路由 large-mem 用例
# 在 Large-Mem runner 的用例集中设置: WORKER_CAPS=large-mem
WORKER_CAPS="${WORKER_CAPS:-}"
# ── 协调器 URL ────────────────────────────────────────────────────────────────
# 端口算法与 coordinator.py 保持一致: CI_PIPELINE_ID % 10000 + 20000
# CI 中由 step 3 显式导出 COORDINATOR_URL，此处仅停用于本地调试
_DEFAULT_PORT="${COORDINATOR_PORT:-$(( (${CI_PIPELINE_ID:-0}) % 10000 + 20000 ))}"
COORDINATOR_URL="${COORDINATOR_URL:-http://192.168.2.207:${_DEFAULT_PORT}}"

# ── 目录 ──────────────────────────────────────────────────────────────────────
RESULTS_DIR="${CI_PROJECT_DIR}/results"
LOGS_DIR="${RESULTS_DIR}/logs"
TDENGINE_DIR="${WORKDIR}/TDinternal/community"
RUN_CONTAINER="${TDENGINE_DIR}/test/ci/run_container.sh"
SLOT_DIR="${WORKDIR}/slots-n${NODE_INDEX}"

# ── 失败用例本地保留目录（不被 after_script 清理，用于 HTTP 浏览）──────────
FAIL_LOGS_BASE="/data1/tdengine-ci/fail-logs"
FAIL_RETAIN_DIR="${FAIL_LOGS_BASE}/job-${CI_JOB_ID:-local}"
FAIL_HTTP_PORT=8899
mkdir -p "${FAIL_RETAIN_DIR}" 2>/dev/null || true
# 清理 7 天前的旧失败日志
find "${FAIL_LOGS_BASE}" -mindepth 1 -maxdepth 1 -name 'job-*' -type d -mtime +7 -exec rm -rf {} + 2>/dev/null || true

mkdir -p "${RESULTS_DIR}" "${LOGS_DIR}" "${SLOT_DIR}"

# ── Worker 自识别 IP ──────────────────────────────────────────────────────────
MY_HOSTNAME=$(hostname)
# 优先取 192.168. 网段 IP（与 Prometheus node_exporter 监听地址一致）
# 若没有则回落到第一个 IP
MY_IP=$(hostname -I 2>/dev/null | tr ' ' '\n' | grep '^192\.168\.' | head -1)
[ -z "$MY_IP" ] && MY_IP=$(hostname -I 2>/dev/null | awk '{print $1}')

# ── Builder 节点检查：排除 builder 节点作为 test worker ────────────────────
# Builder 节点（u1-47、u2-104 等）仅用于编译，不应作为 test worker 运行
# 检查 CI_RUNNER_TAGS 中是否有 tsdb-builder-* 标签，如果有则说明这是 builder 节点
if [[ "${CI_RUNNER_TAGS}" == *"tsdb-builder"* ]]; then
  echo "[run-test-dynamic] ERROR: Builder node ${MY_HOSTNAME} should not run as test worker"
  echo "[run-test-dynamic] This node is reserved for compilation (prepare/build/check/upload/coordinator)"
  echo "[run-test-dynamic] Exiting..."
  exit 0  # 用 exit 0 而不是 exit 1，避免 CI job 标记为失败
fi

# ── 容器命名（含 MR 号） ───────────────────────────────────────────────────────
_MR_PART="${CI_MERGE_REQUEST_IID:+mr${CI_MERGE_REQUEST_IID}}"
_MR_PART="${_MR_PART:-branch}"
JOB_CONTAINER_PREFIX="tdci-${_MR_PART}-${CI_JOB_ID:-local}"

# ── core_pattern 修复（apport → 导致 exit=123） ───────────────────────────────
ORIG_CORE_PATTERN=$(cat /proc/sys/kernel/core_pattern 2>/dev/null || true)
if echo "${ORIG_CORE_PATTERN}" | grep -q '^|'; then
    echo "[run-test-dynamic] core_pattern contains pipe (apport), overriding to /tmp/core.%e.%p"
    echo "/tmp/core.%e.%p" > /proc/sys/kernel/core_pattern 2>/dev/null || \
        echo "[run-test-dynamic] WARNING: cannot write core_pattern (non-fatal)"
fi

echo "========================================"
echo " Test Dynamic Runner"
echo " Node:        ${NODE_INDEX}"
echo " Worker:      ${MY_HOSTNAME}  (${MY_IP})"
echo " Coordinator: ${COORDINATOR_URL}"
echo " Sanitizer:   ${SANITIZER}"
echo " Concurrency: ${TEST_CONCURRENCY}"
echo " WorkerCaps:  ${WORKER_CAPS:-none}"
echo " WORKDIR:     ${WORKDIR}"
echo "========================================"

# ── 前置检查 ──────────────────────────────────────────────────────────────────
err=0
[[ -f "${RUN_CONTAINER}" ]]    || { echo "ERROR: run_container.sh not found: ${RUN_CONTAINER}"; err=1; }
[[ -d "${WORKDIR}/debugNoSan/build/bin" ]] || { echo "ERROR: artifacts missing: ${WORKDIR}/debugNoSan"; err=1; }
[[ -d "${WORKDIR}/debugSan/build/bin" ]]   || echo "WARN: debugSan artifacts missing: ${WORKDIR}/debugSan (san=y cases will fail)"
[[ ${err} -eq 0 ]] || exit 1
chmod +x "${RUN_CONTAINER}"

# ── HTTP 文件服务（用于浏览失败用例日志）──────────────────────────────────────
# 启动一个轻量 HTTP 服务，使 coordinator 可以打印可点击链接
# 仅在本节点有失败用例时才有意义，但预先启动以简化逻辑
if ! curl -sf --max-time 1 "http://127.0.0.1:${FAIL_HTTP_PORT}/" >/dev/null 2>&1; then
    python3 -m http.server "${FAIL_HTTP_PORT}" \
        --directory "${FAIL_LOGS_BASE}" \
        --bind 0.0.0.0 >/dev/null 2>&1 &
    _HTTP_PID=$!
    echo "[run-test-dynamic] HTTP file server started on :${FAIL_HTTP_PORT} (PID=${_HTTP_PID})"
    echo "[run-test-dynamic] Browse: http://${MY_IP}:${FAIL_HTTP_PORT}/"
else
    echo "[run-test-dynamic] HTTP file server already running on :${FAIL_HTTP_PORT}"
    _HTTP_PID=""
fi

# ── curl 辅助函数 ─────────────────────────────────────────────────────────────
# 带重试的 GET
coord_get() {
    local url="$1"
    local i
    for i in 1 2 3; do
        local out
        out=$(curl -sf --max-time 10 "${url}" 2>/dev/null) && { echo "${out}"; return 0; }
        sleep 2
    done
    return 1
}

# POST JSON（失败返回非 0 但不退出）
coord_post() {
    local url="$1"
    local body="$2"
    local i
    for i in 1 2 3; do
        curl -sf --max-time 10 -X POST "${url}" \
            -H "Content-Type: application/json" \
            -d "${body}" >/dev/null 2>&1 && return 0
        sleep 2
    done
    echo "[run-test-dynamic] WARN: POST ${url} failed after 3 retries"
    return 1
}

# ── Pipeline cancel 处理 ──────────────────────────────────────────────────────
cancel_handler() {
    echo ""
    echo "[run-test-dynamic] *** Job cancelled — stopping containers for ${JOB_CONTAINER_PREFIX} ***"
    _ci_unregister_job
    # 先按命名前缀停（当前 job 的容器）
    docker ps --filter "name=${JOB_CONTAINER_PREFIX}" --format "{{.Names}}" 2>/dev/null \
        | xargs -r docker stop --time 15 2>/dev/null || true
    sleep 2
    docker ps --filter "name=${JOB_CONTAINER_PREFIX}" --format "{{.Names}}" 2>/dev/null \
        | xargs -r docker kill 2>/dev/null || true
    # 恢复 core_pattern
    [ -n "${ORIG_CORE_PATTERN}" ] && echo "${ORIG_CORE_PATTERN}" > /proc/sys/kernel/core_pattern 2>/dev/null || true
    exit 130
}
trap cancel_handler SIGTERM SIGINT

# 退出时：按当前 job 前缀清理本 job 的容器，并递减本机计数器
_on_exit() {
    # 停止心跳后台进程
    [[ ${_heartbeat_pid:-0} -gt 0 ]] && kill "${_heartbeat_pid}" 2>/dev/null || true
    _ci_unregister_job
    docker ps --filter "name=${JOB_CONTAINER_PREFIX}" --format "{{.Names}}" 2>/dev/null \
        | xargs -r docker stop --time 10 2>/dev/null || true
    docker ps -a --filter "name=${JOB_CONTAINER_PREFIX}" --format "{{.Names}}" 2>/dev/null \
        | xargs -r docker rm -f 2>/dev/null || true
    [ -n "${ORIG_CORE_PATTERN}" ] && echo "${ORIG_CORE_PATTERN}" > /proc/sys/kernel/core_pattern 2>/dev/null || true
}
trap _on_exit EXIT

# ── slot 池并发结构（与 run-test-batch.sh 相同）─────────────────────────────
declare -a SLOT_PIDS
for (( s=0; s<TEST_CONCURRENCY; s++ )); do
    SLOT_PIDS[$s]=0
done

pass_count=0
fail_count=0
junit_cases=""
failed_labels=()
failed_urls=()
failed_dirs=()
failed_logs=()
_coord_gone=0
in_flight=0
FINISHED_SLOT=-1
overall_start=$(date +%s)
last_case_time=${overall_start}
case_seq=0

# ── 在后台子进程中执行单个 case ───────────────────────────────────────────────
run_case_in_slot() {
    local slot=$1 path=$2 cmd=$3 seq=$4 runner=${5:-legacy} san=${6:-n}
    local thread_no=$(( (NODE_INDEX - 1) * TEST_CONCURRENCY + slot ))
    local container_name="${JOB_CONTAINER_PREFIX}-t${thread_no}"
    local log_file="${SLOT_DIR}/case-${seq}.log"
    local start_ms=$(date +%s%3N)

    # 选择 run_container.sh 脚本
    local _rc_script
    if [[ "${runner}" == "newfw" ]]; then
        _rc_script="${TDENGINE_DIR}/test/ci/run_container.sh"
    else
        _rc_script="${RUN_CONTAINER}"
    fi

    # 确保 ci/ 目录存在于 thread_volume（run_container.sh 仅复制 exec_dir 子目录，
    # 但容器入口点 ci/run_case.sh 挂载在 thread_volume 层被遮盖，需预先补上）
    local _tvol="${WORKDIR}/tmp/thread_volume/${thread_no}"
    mkdir -p "${_tvol}"
    [[ ! -d "${_tvol}/ci" ]] && cp -rf "${TDENGINE_DIR}/test/ci" "${_tvol}/ci" 2>/dev/null || true

    timeout --kill-after=15 "${CASE_TIMEOUT}" \
        bash "${_rc_script}" \
            -w "${WORKDIR}" \
            -e \
            -s "${san}" \
            -d "${path}" \
            -c "${cmd}" \
            -t "${thread_no}" \
            -n "${container_name}" \
            > "${log_file}" 2>&1
    local rc=$?
    # timeout 返回 124(SIGTERM) 或 137(SIGKILL+15s)，强制停容器
    if [[ ${rc} -eq 124 || ${rc} -eq 137 ]]; then
        echo "[run-test-dynamic] CASE TIMEOUT (${CASE_TIMEOUT}s): killing container ${container_name}" >> "${log_file}"
        docker stop --time 10 "${container_name}" >/dev/null 2>&1 || true
        docker rm -f "${container_name}" >/dev/null 2>&1 || true
        rc=124
    fi
    local elapsed_ms=$(( $(date +%s%3N) - start_ms ))

    echo "${rc}"            > "${SLOT_DIR}/slot-${slot}.rc"
    echo "${elapsed_ms}"    > "${SLOT_DIR}/slot-${slot}.elapsed"
    echo "${path}::${cmd}"  > "${SLOT_DIR}/slot-${slot}.label"
    echo "${log_file}"      > "${SLOT_DIR}/slot-${slot}.log"
    # 存储协调器需要的 case_idx
    echo "${slot_idx_map[$slot]:-unknown}" > "${SLOT_DIR}/slot-${slot}.idx"
    # 记录 slot→slug 和 tnum→slug 映射，供 after_script 路由日志
    # 同时记录 san 标志供 process_finished_slot 写 case.txt
    echo "${san}" > "${SLOT_DIR}/slot-${slot}.san"
    local _slug_raw; _slug_raw=$(echo "${cmd}" | grep -oP 'cases/\S+' | tail -1)
    if [[ -z "${_slug_raw}" ]]; then
        # fallback: 取 cmd 中最后一个含 / 的 token（如 82-UnitTest/test.sh）
        _slug_raw=$(echo "${cmd}" | tr ' ' '\n' | grep '/' | grep -v '^-' | grep -v '^http' | tail -1)
    fi
    if [[ -n "${_slug_raw}" ]]; then
        local _slug_val="n${NODE_INDEX}-$(echo "${_slug_raw}" \
            | sed 's|^cases/||; s|/|__|g; s|\.py||; s|\.sh||; s/[\[\*\?]/_/g; s/[^A-Za-z0-9_.-]/_/g; s/__*/_/g; s/_$//; s/^_//')"
        echo "${_slug_val}" > "${SLOT_DIR}/slot-${slot}.slug"
        # tnum 级映射：after_script 不需要知道 TEST_CONCURRENCY
        echo "${_slug_val}" > "${SLOT_DIR}/tnum-${thread_no}.slug"
    fi
    exit ${rc}
}
export -f run_case_in_slot

# ── slot 辅助函数 ─────────────────────────────────────────────────────────────
find_free_slot() {
    for (( s=0; s<TEST_CONCURRENCY; s++ )); do
        [[ ${SLOT_PIDS[$s]} -eq 0 ]] && { echo $s; return; }
    done
    echo -1
}

count_free_slots() {
    local free=0
    for (( s=0; s<TEST_CONCURRENCY; s++ )); do
        [[ ${SLOT_PIDS[$s]} -eq 0 ]] && free=$(( free + 1 ))
    done
    echo $free
}

# ── 动态扩容：当同机并发 pipeline 数减少时提升 TEST_CONCURRENCY ────────────────
# 使用与启动时相同的计算公式，每 60s 检查一次（flock 有开销，不宜过频）。
# find_free_slot / count_free_slots / harvest_one 均动态引用 TEST_CONCURRENCY，
# 只需扩展 SLOT_PIDS 并更新变量即可立即生效，无需其他改动。
_last_slot_expand_check=0
_maybe_expand_slots() {
    local _now; _now=$(date +%s)
    [[ $(( _now - _last_slot_expand_check )) -lt 60 ]] && return
    _last_slot_expand_check=${_now}

    local _cur_active; _cur_active=$(_ci_register_job)
    local _nc; _nc=$(nproc 2>/dev/null || echo 8)
    local _usable=$(( _nc * 8 / 10 ))
    local _new_base=$(( _usable / (_cur_active < 1 ? 1 : _cur_active) ))
    case "${CI_SCHED_AGGR:-1}" in
        0) _ep=60 ;; 2) _ep=80 ;; *) _ep=70 ;;
    esac
    local _new_max=$(( _nc > 16 ? _nc * _ep / 100 : 16 ))
    local _new_conc=$(( _new_base < 2 ? 2 : (_new_base > _new_max ? _new_max : _new_base) ))

    if [[ ${_new_conc} -gt ${TEST_CONCURRENCY} ]]; then
        echo "[run-test-dynamic] expand TEST_CONCURRENCY: ${TEST_CONCURRENCY} → ${_new_conc}" \
             "(active_pipelines=${_cur_active}, nproc=${_nc})"
        for (( s=${TEST_CONCURRENCY}; s<_new_conc; s++ )); do
            SLOT_PIDS[$s]=0
        done
        TEST_CONCURRENCY=${_new_conc}
    fi
}

harvest_one() {
    # 等待任意一个后台子进程结束，结果写入全局 FINISHED_SLOT
    # 必须直接调用（不用命令替换子 shell），否则 SLOT_PIDS 修改丢失
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

# case_idx 与 slot 的映射（用于上报协调器）
declare -A slot_idx_map

process_finished_slot() {
    local slot=$1
    local rc=0 elapsed_ms=0 label="" log_file="" case_idx="" fail_log=""
    [[ -f "${SLOT_DIR}/slot-${slot}.rc" ]]      && rc=$(cat "${SLOT_DIR}/slot-${slot}.rc")
    [[ -f "${SLOT_DIR}/slot-${slot}.elapsed" ]]  && elapsed_ms=$(cat "${SLOT_DIR}/slot-${slot}.elapsed")
    [[ -f "${SLOT_DIR}/slot-${slot}.label" ]]    && label=$(cat "${SLOT_DIR}/slot-${slot}.label")
    [[ -f "${SLOT_DIR}/slot-${slot}.log" ]]      && log_file=$(cat "${SLOT_DIR}/slot-${slot}.log")
    [[ -f "${SLOT_DIR}/slot-${slot}.idx" ]]      && case_idx=$(cat "${SLOT_DIR}/slot-${slot}.idx")

    local elapsed_s="$(( elapsed_ms / 1000 )).$(printf '%03d' $(( elapsed_ms % 1000 )))"
    local path="${label%%::*}"
    local cmd="${label#*::}"
    local safe_name=$(echo "${label}" | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g')
    local safe_path=$(echo "${path}" | sed 's/&/\&amp;/g')

    # 上报协调器（coordinator 已退出时跳过）
    if [[ -n "${case_idx}" && ${_coord_gone:-0} -eq 0 ]]; then
        local log_b64="" done_log_b64=""
        local _report_slug=""
        [[ -f "${SLOT_DIR}/slot-${slot}.slug" ]] && _report_slug="job-${CI_JOB_ID:-local}/$(cat "${SLOT_DIR}/slot-${slot}.slug")"
        if [[ ${rc} -ne 0 ]]; then
            # fail_log 是已复制到 LOGS_DIR 的 artifact（log_file 在 fail 分支末已 rm -f）
            local _log_src="${fail_log:-}"
            [[ -z "${_log_src}" || ! -f "${_log_src}" ]] && _log_src="${log_file}"
            # /api/fail：发送最多 512KB 日志供 coordinator 实时展示（允许大 body，失败仅丢失实时通知）
            [[ -f "${_log_src}" ]] && log_b64=$(tail -c 524288 "${_log_src}" | base64 -w 0 2>/dev/null || true)
            # /api/done：只发最后 4KB（coordinator 汇总只用 [-4096:]），保证小 body 可靠送达
            [[ -f "${_log_src}" ]] && done_log_b64=$(tail -c 4096 "${_log_src}" | base64 -w 0 2>/dev/null || true)
            # 立即推送失败通知到协调器（实时可见）
            local _fail_notify
            printf -v _fail_notify \
                '{"worker":"%s","worker_ip":"%s","rc":%d,"elapsed_ms":%d,"path":"%s","cmd":"%s","slug":"%s","log_b64":"%s"}' \
                "${MY_HOSTNAME}" "${MY_IP}" "${rc}" "${elapsed_ms}" \
                "$(echo "${path}" | sed 's/"/\\"/g')" \
                "$(echo "${cmd}"  | sed 's/"/\\"/g')" \
                "${_report_slug}" \
                "${log_b64}"
            coord_post "${COORDINATOR_URL}/api/fail" "${_fail_notify}" || true
        fi
        local post_body
        printf -v post_body \
            '{"worker":"%s","worker_ip":"%s","idx":%s,"rc":%d,"elapsed_ms":%d,"path":"%s","cmd":"%s","slug":"%s","log_b64":"%s"}' \
            "${MY_HOSTNAME}" "${MY_IP}" "${case_idx}" "${rc}" "${elapsed_ms}" \
            "$(echo "${path}" | sed 's/"/\\"/g')" \
            "$(echo "${cmd}"  | sed 's/"/\\"/g')" \
            "${_report_slug}" \
            "${done_log_b64}"
        coord_post "${COORDINATOR_URL}/api/done" "${post_body}" || true
    fi

    if [[ ${rc} -eq 0 ]]; then
        echo "  => PASS (${elapsed_s}s)  [${label}]"
        junit_cases+="    <testcase name=\"${safe_name}\" classname=\"${safe_path}\" time=\"${elapsed_s}\"/>"$'\n'
        pass_count=$(( pass_count + 1 ))
        rm -f "${log_file}" "${SLOT_DIR}/slot-${slot}."* 2>/dev/null
    else
        echo "  => FAIL (exit=${rc}, ${elapsed_s}s)  [${label}]"

        # ── 用例专属 artifact 目录 ────────────────────────────────────────────
        # 优先读 run_case_in_slot 写好的 .slug 文件；提取不到时 fallback。
        local _case_slug
        if [[ -f "${SLOT_DIR}/slot-${slot}.slug" ]]; then
            _case_slug=$(cat "${SLOT_DIR}/slot-${slot}.slug")
        else
            local _raw_target
            _raw_target=$(echo "${cmd}" | grep -oP 'cases/\S+' | tail -1)
            if [[ -z "${_raw_target}" ]]; then
                # fallback: 取 cmd 中最后一个含 / 的 token（如 82-UnitTest/test.sh）
                _raw_target=$(echo "${cmd}" | tr ' ' '\n' | grep '/' | grep -v '^-' | grep -v '^http' | tail -1)
            fi
            if [[ -n "${_raw_target}" ]]; then
                _case_slug="n${NODE_INDEX}-$(echo "${_raw_target}" \
                    | sed 's|^cases/||; s|/|__|g; s|\.py||; s|\.sh||; s/[\[\*\?]/_/g; s/[^A-Za-z0-9_.-]/_/g; s/__*/_/g; s/_$//; s/^_//')"
            else
                _case_slug="n${NODE_INDEX}-s${slot}-$(echo "${cmd}" | md5sum | cut -c1-8)"
            fi
        fi
        local CASE_LOGS_DIR="${LOGS_DIR}/${_case_slug}"
        mkdir -p "${CASE_LOGS_DIR}"

        local fail_log="${CASE_LOGS_DIR}/run.log.txt"
        cp "${log_file}" "${fail_log}" 2>/dev/null || true
        local artifact_rel="results/logs/${_case_slug}/run.log.txt"

        # ── 实时打印失败日志（两个并列 section：摘要 + 完整日志）────────────
        local _ts; _ts=$(date +%s)
        local _sec_id="fail_n${NODE_INDEX}_s${slot}_${_ts}"
        local _sec_title="FAIL [exit=${rc}] ${label}"

        # [1] 外层摘要 section（过滤 pip 噪音，只展示核心失败输出，默认展开）
        local _cur_url="http://${MY_IP}:${FAIL_HTTP_PORT}/job-${CI_JOB_ID:-local}/${_case_slug}/"
        local _cur_dir="root@${MY_IP}:${FAIL_LOGS_BASE}/job-${CI_JOB_ID:-local}/${_case_slug}/"
        echo -e "\e[0Ksection_start:${_ts}:${_sec_id}[collapsed=true]\r\e[0K\e[31;1m${_sec_title}\e[0m"
        echo "────────────────────────────────────────────────────────────────"
        echo "Case logs:   ${_cur_url}run.log.txt"
        echo "Runner logs: ${_cur_url}"
        echo "Fail dir:    ${_cur_dir}"
        echo "摘要信息:"
        if [[ -f "${fail_log}" ]]; then
            grep -v -E \
                '^\s*(Collecting |Downloading |━+|Requirement already|Successfully (installed|uninstalled)|Attempting uninstall|Found existing installation|Uninstalling |WARNING: Running pip|\[notice\]|-----|Looking in indexes)' \
                "${fail_log}" \
                | grep -v '^$' \
                | tail -80
        else
            echo "(no log)"
        fi
        echo "────────────────────────────────────────────────────────────────"
        echo -e "\e[0Ksection_end:${_ts}:${_sec_id}\r\e[0K"

        # ── 收集关联日志文件到用例专属目录 ───────────────────────────────────
        # 按 slot 推算 thread_no（与 run_container.sh 一致）
        local _tnum=$(( (NODE_INDEX - 1) * TEST_CONCURRENCY + slot ))
        local _tvol="${WORKDIR}/tmp/thread_volume/${_tnum}"

        # sim/（保留容器内原始路径结构）
        # 宿主机 thread_volume/{tnum}/sim/ 挂载到容器 /mnt/tsdb/sim/
        #   psim/log/     ← 客户端日志 (taoslog0.0, taosSlowLog)
        #   psim/cfg/     ← 客户端配置
        #   dnode*/log/   ← taosd 服务端日志 (taosdlog.0, udfdlog.0)
        #   dnode*/cfg/   ← taosd 配置
        #   asan/         ← ASan 日志（如有）
        #   var_taoslog/  ← run_case.sh 从 /var/log/taos/ 复制（补充）
        # 跳过：tsim/（空）、dnode*/data/（可能几十 GB）
        local _sim_src="${_tvol}/sim"
        if [[ -d "${_sim_src}" ]]; then
            local _sim_dest="${CASE_LOGS_DIR}/sim"
            find "${_sim_src}" -mindepth 1 -maxdepth 1 -type d | while read -r _d; do
                _dname=$(basename "${_d}")
                if [[ "${_dname}" == "psim" ]]; then
                    # 客户端日志和配置
                    for _sub in log cfg; do
                        if [[ -d "${_d}/${_sub}" && "$(ls -A "${_d}/${_sub}" 2>/dev/null)" ]]; then
                            mkdir -p "${_sim_dest}/psim/${_sub}"
                            cp -rf "${_d}/${_sub}/." "${_sim_dest}/psim/${_sub}/" 2>/dev/null || true
                        fi
                    done
                elif [[ "${_dname}" == "dnode"* ]]; then
                    # taosd 日志和配置（跳过 data/）
                    for _sub in log cfg; do
                        if [[ -d "${_d}/${_sub}" && "$(ls -A "${_d}/${_sub}" 2>/dev/null)" ]]; then
                            mkdir -p "${_sim_dest}/${_dname}/${_sub}"
                            cp -rf "${_d}/${_sub}/." "${_sim_dest}/${_dname}/${_sub}/" 2>/dev/null || true
                        fi
                    done
                elif [[ "${_dname}" == "asan" || "${_dname}" == "var_taoslog" ]]; then
                    # 有文件才收集
                    if [[ "$(find "${_d}" -type f 2>/dev/null | head -1)" ]]; then
                        mkdir -p "${_sim_dest}/${_dname}"
                        cp -rf "${_d}/." "${_sim_dest}/${_dname}/" 2>/dev/null || true
                        # ASAN 日志（dnode1.asan、psim.info 等）无扩展名，HTTP/GitLab
                        # 前端无法内联预览，为每个文件创建 .txt 软链方便直接查看
                        if [[ "${_dname}" == "asan" ]]; then
                            find "${_sim_dest}/asan" -maxdepth 1 -type f ! -name '*.txt' 2>/dev/null \
                                | while IFS= read -r _af; do
                                    ln -sf "$(basename "${_af}")" "${_af}.txt" 2>/dev/null || true
                                done
                        fi
                    fi
                fi
                # 跳过 tsim/ 等无用目录
            done
            [[ -d "${_sim_dest}" ]] && echo "[sim] Collected → results/logs/${_case_slug}/sim/"
        fi

        # coredump/（只收实际 core 文件，跳过 TDengine 数据目录）
        # core_pattern 为 /corefile/core_%e-%p，匹配 core_* 和 core.*
        # TDengine 还会在同目录下创建 tdengine_slow_log/ tdengine_stream_data/ 等
        local _coredump_files_for_gdb=""    # 供后续 GDB 分析
        local _core_src="${_tvol}/coredump"
        if [[ -d "${_core_src}" ]]; then
            local _core_files
            _core_files=$(find "${_core_src}" -maxdepth 2 -type f \( -name 'core_*' -o -name 'core.*' -o -name 'core' \) 2>/dev/null)
            if [[ -n "${_core_files}" ]]; then
                _coredump_files_for_gdb="${_core_files}"    # 保存原始路径列表
                echo "[coredump] Core files found:"
                echo "${_core_files}" | xargs -r ls -lh 2>/dev/null || true
                local _core_size_kb
                _core_size_kb=$(echo "${_core_files}" | xargs -r du -sk 2>/dev/null | awk '{s+=$1}END{print s+0}')
                # 始终把 core 文件保留到 FAIL_RETAIN_DIR（不被 after_script 清理）
                # 小文件（<300MB）同时 cp 到 CI artifact；大文件只做 hardlink（零额外空间）
                local _core_dest="${CASE_LOGS_DIR}/coredump"
                mkdir -p "${_core_dest}"
                local _retain_core_dest="${FAIL_RETAIN_DIR}/${_case_slug}/coredump"
                mkdir -p "${_retain_core_dest}"
                if [[ ${_core_size_kb} -lt 307200 ]]; then
                    echo "${_core_files}" | while IFS= read -r _cf; do
                        cp "${_cf}" "${_core_dest}/" 2>/dev/null || true
                    done
                    echo "[coredump] Collected (${_core_size_kb}KB) → results/logs/${_case_slug}/coredump/"
                else
                    echo "[coredump] LARGE coredump (${_core_size_kb}KB > 300MB), NOT uploaded to artifacts."
                    # hardlink 到 FAIL_RETAIN_DIR：同一文件系统下零额外空间，
                    # after_script 删原文件后 hardlink 仍持有 inode，数据不丢
                    echo "${_core_files}" | while IFS= read -r _cf; do
                        ln "${_cf}" "${_retain_core_dest}/" 2>/dev/null \
                            || cp "${_cf}" "${_retain_core_dest}/" 2>/dev/null || true
                    done
                    echo "[coredump] Hardlinked to ${_retain_core_dest}/"
                    echo "[coredump] Browse: http://${MY_IP}:${FAIL_HTTP_PORT}/job-${CI_JOB_ID:-local}/${_case_slug}/coredump/"
                fi
            fi
        fi

        # ── 本地持久保留（不被 after_script 清理）──────────────────────────
        # 复制日志到 FAIL_RETAIN_DIR，并通过符号链接关联共享二进制
        local _retain_dir="${FAIL_RETAIN_DIR}/${_case_slug}"
        mkdir -p "${_retain_dir}"
        # 复制 run.log.txt 和已收集的 sim/ coredump/
        cp -r "${CASE_LOGS_DIR}/." "${_retain_dir}/" 2>/dev/null || true
        # case.txt：供 rerun.sh --case 使用的元数据
        local _san_flag="n"
        [[ -f "${SLOT_DIR}/slot-${slot}.san" ]] && _san_flag=$(cat "${SLOT_DIR}/slot-${slot}.san")
        local _dbg_for_case; [[ "${_san_flag}" == "y" ]] && _dbg_for_case="${WORKDIR}/debugSan" || _dbg_for_case="${WORKDIR}/debugNoSan"
        cat > "${_retain_dir}/case.txt" <<CASETXT
COMMUNITY_DIR=${CI_PROJECT_DIR}/source/taos-community
DEBUG_DIR=${_dbg_for_case}
SANITIZER=${_san_flag}
CMD=${cmd}
CASETXT
        # 若存在 jdbc-out.log（83-DocTest/jdbc.sh 日志），一并收集
        local _jdbc_log="${TDENGINE_DIR}/docs/examples/JDBC/JDBCDemo/jdbc-out.log"
        if [[ -f "${_jdbc_log}" ]]; then
            cp "${_jdbc_log}" "${_retain_dir}/jdbc-out.log" 2>/dev/null || true
        fi
        # taosd/taos 二进制放入共享目录，每个用例做 symlink
        local _debug_bin="${WORKDIR}/debugNoSan/build/bin"
        if echo "${cmd}" | grep -qE '\bsan\b|sanitizer'; then
            [[ -d "${WORKDIR}/debugSan/build/bin" ]] && _debug_bin="${WORKDIR}/debugSan/build/bin"
        fi
        local _debug_lib; _debug_lib="$(dirname "${_debug_bin}")/lib"
        local _shared_bin="${FAIL_RETAIN_DIR}/_shared_bin"
        local _shared_lib="${FAIL_RETAIN_DIR}/_shared_lib"
        mkdir -p "${_shared_bin}" "${_shared_lib}"
        for _bin_name in taosd taos; do
            if [[ -f "${_debug_bin}/${_bin_name}" ]]; then
                # 只在共享目录中不存在或文件已变化时才复制
                if [[ ! -f "${_shared_bin}/${_bin_name}" ]] || \
                   ! cmp -s "${_debug_bin}/${_bin_name}" "${_shared_bin}/${_bin_name}"; then
                    cp "${_debug_bin}/${_bin_name}" "${_shared_bin}/${_bin_name}" 2>/dev/null || true
                fi
                # 用例目录中创建 build/bin/ 并符号链接到共享副本
                mkdir -p "${_retain_dir}/build/bin"
                ln -sf "${_shared_bin}/${_bin_name}" "${_retain_dir}/build/bin/${_bin_name}" 2>/dev/null || true
            fi
        done
        # libtaosnative.so 放入共享目录，每个用例做 symlink（供 GDB solib-search-path 使用）
        local _lib_name="libtaosnative.so"
        if [[ -f "${_debug_lib}/${_lib_name}" ]]; then
            if [[ ! -f "${_shared_lib}/${_lib_name}" ]] || \
               ! cmp -s "${_debug_lib}/${_lib_name}" "${_shared_lib}/${_lib_name}"; then
                cp "${_debug_lib}/${_lib_name}" "${_shared_lib}/${_lib_name}" 2>/dev/null || true
            fi
            mkdir -p "${_retain_dir}/build/lib"
            ln -sf "${_shared_lib}/${_lib_name}" "${_retain_dir}/build/lib/${_lib_name}" 2>/dev/null || true
        fi
        echo "[retain] Saved to ${_retain_dir}/"
        echo "[retain] Browse: http://${MY_IP}:${FAIL_HTTP_PORT}/job-${CI_JOB_ID:-local}/${_case_slug}/"

        # ── coredump GDB 摘要（retain 已就绪，_debug_bin / _shared_bin 均已定义）──
        # 识别崩溃进程 → 查找对应 binary → 在容器内运行 gdb --batch 'thread apply all bt'
        # 结果打印到 job 日志 + 保存到 ${_retain_dir}/coredump/gdb-bt-*.txt
        if [[ -n "${_coredump_files_for_gdb}" ]]; then
            local _gdb_dir="${_retain_dir}/coredump"
            mkdir -p "${_gdb_dir}"
            local _gdb_ts; _gdb_ts=$(date +%s)
            local _gdb_sec_id="gdb_n${NODE_INDEX}_${_gdb_ts}"
            echo -e "\e[0Ksection_start:${_gdb_ts}:${_gdb_sec_id}[collapsed=true]\r\e[0K\U0001F50D Coredump GDB \u2014 ${_case_slug}"
            while IFS= read -r _cf; do
                [[ -f "${_cf}" ]] || continue
                local _cf_name; _cf_name=$(basename "${_cf}")
                echo "────────────────────────────────────────────────────────────────"
                echo "Core : ${_cf_name}  ($(du -sh "${_cf}" 2>/dev/null | cut -f1))"
                # 识别 binary:
                #   优先: file 命令解析 execfn（实际可执行文件路径，最准确）
                #   其次: 从文件名 core_%e-%p 提取进程名（%e 可能是线程名如 dnode-mgmt）
                local _file_out; _file_out=$(file "${_cf}" 2>/dev/null | head -1)
                local _bin_hint
                _bin_hint=$(echo "${_file_out}" | grep -oP "execfn: '\K[^']+" | head -1)
                _bin_hint=$(basename "${_bin_hint:-}")
                if [[ -z "${_bin_hint}" ]]; then
                    # core_%e-%p → 去掉 core_ 前缀和 -<digits> 后缀
                    _bin_hint=$(echo "${_cf_name}" | sed 's/^core_//; s/-[0-9]*$//')
                fi
                echo "Binary: '${_bin_hint:-unknown}'"
                echo "file  : ${_file_out}"
                # 查找可执行文件（debug_bin → shared_bin）
                local _exe=""
                for _bdir in "${_debug_bin}" "${_shared_bin}"; do
                    [[ -n "${_bin_hint}" && -f "${_bdir}/${_bin_hint}" ]] && { _exe="${_bdir}/${_bin_hint}"; break; }
                done
                # fallback: taosd（线程名 dnode-mgmt 等本质上是 taosd 进程）
                if [[ -z "${_exe}" ]]; then
                    for _bdir in "${_debug_bin}" "${_shared_bin}"; do
                        if [[ -f "${_bdir}/taosd" ]]; then
                            _exe="${_bdir}/taosd"
                            echo "(binary '${_bin_hint}' not found; falling back to taosd)"
                            break
                        fi
                    done
                fi
                local _gdb_out="${_gdb_dir}/gdb-bt-${_cf_name}.txt"
                if [[ -n "${_exe}" ]]; then
                    local _exe_real; _exe_real=$(readlink -f "${_exe}" 2>/dev/null || echo "${_exe}")
                    # lib 目录与 bin 目录同级：debugNoSan/build/lib 或 debugSan/build/lib
                    local _lib_dir; _lib_dir=$(dirname "$(dirname "${_exe_real}")")/lib
                    echo "Exe   : ${_exe_real}"
                    echo "Lib   : ${_lib_dir}"
                    echo "[GDB] running thread apply all bt (timeout 90s) ..."
                    local _lib_vol=""
                    [[ -d "${_lib_dir}" ]] && _lib_vol="-v ${_lib_dir}:/_lib:ro"
                    timeout 90 docker run --rm \
                        -v "${_exe_real}:/_exe:ro" \
                        -v "${_cf}:/_core:ro" \
                        ${_lib_vol} \
                        "${BUILDER_IMAGE}" bash -c '
                            which gdb >/dev/null 2>&1 || { echo "(gdb not available in image)"; exit 0; }
                            gdb --batch -q \
                                -ex "set pagination off" \
                                -ex "set print thread-events off" \
                                -ex "set solib-search-path /_lib" \
                                -ex "thread apply all bt" \
                                /_exe /_core 2>&1 | head -500
                        ' 2>/dev/null | tee "${_gdb_out}" \
                        || echo "[coredump] GDB timed out or failed"
                else
                    echo "(binary '${_bin_hint:-unknown}' not found in debug/shared dirs — manual GDB needed)"
                    echo "  _debug_bin : ${_debug_bin}"
                    echo "  _shared_bin: ${_shared_bin}"
                    printf '%s\n' "(binary not found, manual analysis needed)" > "${_gdb_out}"
                fi
            done <<< "${_coredump_files_for_gdb}"
            echo "────────────────────────────────────────────────────────────────"
            echo -e "\e[0Ksection_end:${_gdb_ts}:${_gdb_sec_id}\r\e[0K"
        fi

        # ── JUnit XML（head -c 8192 保留更多上下文）─────────────────────────
        # 先过滤 pip 噪音、ANSI 转义序列和 XML 1.0 非法控制字符，避免解析失败
        local log_tail
        log_tail=$(tail -100 "${fail_log}" 2>/dev/null \
            | grep -v -E '^\s*(Collecting |Downloading |━+|Requirement already|Successfully (installed|uninstalled)|Attempting uninstall|Found existing installation|Uninstalling |WARNING: Running pip|\[notice\]|-----|Looking in indexes)' \
            | grep -v '^$' \
            | sed 's/\x1b\[[0-9;]*[mKHJABCDsurh]//g; s/\r//g' \
            | LC_ALL=C tr -cd '\11\12\40-\176' \
            | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g' \
            | head -c 8192)
        junit_cases+="    <testcase name=\"${safe_name}\" classname=\"${safe_path}\" time=\"${elapsed_s}\">"$'\n'
        junit_cases+="      <failure message=\"exit code ${rc}\">"$'\n'
        junit_cases+="Log artifact: ${artifact_rel}"$'\n'
        junit_cases+="${log_tail}"$'\n'
        junit_cases+="      </failure>"$'\n'
        junit_cases+="    </testcase>"$'\n'
        failed_labels+=("  [exit=${rc}] (${elapsed_s}s)  ${label}")
        failed_urls+=("http://${MY_IP}:${FAIL_HTTP_PORT}/job-${CI_JOB_ID:-local}/${_case_slug}/")
        failed_dirs+=("root@${MY_IP}:${FAIL_LOGS_BASE}/job-${CI_JOB_ID:-local}/${_case_slug}/")
        failed_logs+=("${fail_log}")
        fail_count=$(( fail_count + 1 ))
        rm -f "${log_file}" "${SLOT_DIR}/slot-${slot}."* 2>/dev/null
    fi
    unset "slot_idx_map[$slot]"
}

# ── 心跳后台循环：每 30s 向 coordinator 发送心跳 ─────────────────────────────
# 独立于 case 执行，即使所有 slot 都忙也会持续发送，
# 避免 coordinator 误判 worker 死亡而收割正在运行的 case。
_heartbeat_pid=0
_start_heartbeat() {
    (
        while true; do
            sleep 30
            curl -sf --max-time 5 \
                "${COORDINATOR_URL}/api/heartbeat?worker=${MY_HOSTNAME}&job_id=${CI_JOB_ID:-local}&node=${NODE_INDEX}" \
                >/dev/null 2>&1 || true
        done
    ) &
    _heartbeat_pid=$!
    echo "[run-test-dynamic] heartbeat loop started (pid=${_heartbeat_pid}, interval=30s)"
}
_start_heartbeat

# ── 主循环：从协调器拉取用例 ──────────────────────────────────────────────────
echo ""
echo "[run-test-dynamic] Starting main loop, polling ${COORDINATOR_URL}/api/next"
echo ""

_all_done=0
_wait_indicated=0
_coord_fail_streak=0   # 连续联系失败次数

while [[ ${_all_done} -eq 0 ]]; do
    # 当同机并发 pipeline 减少时动态扩容 slot 池（每60s检查一次）
    _maybe_expand_slots

    # 计算空闲 slot 数
    free_slots=$(count_free_slots)

    # 若有 slot 等待但被通知需要等待，暂时只申请 1 个
    if [[ ${_wait_indicated} -gt 0 ]]; then
        req_slots=1
        _wait_indicated=$(( _wait_indicated - 1 ))
    else
        req_slots=${free_slots}
    fi

    # 只有有空闲 slot 时才请求新 case
    if [[ ${free_slots} -gt 0 && ${req_slots} -gt 0 ]]; then
        resp=$(coord_get \
            "${COORDINATOR_URL}/api/next?worker=${MY_HOSTNAME}&ip=${MY_IP}&slots=${req_slots}&caps=${WORKER_CAPS}" \
        ) || {
            _coord_fail_streak=$(( _coord_fail_streak + 1 ))
            echo "[run-test-dynamic] WARN: coordinator unreachable (streak=${_coord_fail_streak}), retry in ${POLL_INTERVAL}s"
            # 两种情况下主动退出：
            # 快速退出策略：
            # 1. 连续失败 >= 5 次（15s）且无 in_flight → coordinator 已退出，立即退出
            # 2. 连续失败 >= 5 次（15s）且有 in_flight → 进入 drain 模式，耗尽当前任务后退出
            # 3. 兜底：连续失败 >= 120 次时强制退出（临时网络抖动保护）
            if [[ ${_coord_fail_streak} -ge 5 ]]; then
                if [[ ${in_flight} -eq 0 ]]; then
                    echo "[run-test-dynamic] Coordinator gone and no local work. Exiting."
                else
                    echo "[run-test-dynamic] Coordinator gone with ${in_flight} in-flight case(s). Draining."
                fi
                _coord_gone=1
                _all_done=1
                break
            fi
            if [[ ${_coord_fail_streak} -ge 120 ]]; then
                echo "[run-test-dynamic] Coordinator unreachable for too long. Exiting."
                _coord_gone=1
                _all_done=1
                break
            fi
            sleep "${POLL_INTERVAL}"
            continue
        }
        _coord_fail_streak=0   # 成功则清零

        # ── 解析关键字段（inline grep，不依赖 python3 解析成败）─────────────
        _resp_alldone=0
        _resp_ql=1        # 默认为有任务（保守）
        _resp_assigned=1
        echo "${resp}" | grep -q '"all_done": *true\|"all_done":true' && _resp_alldone=1
        _resp_ql=$(echo "${resp}" | python3 -c \
            "import sys,json; print(json.load(sys.stdin).get('queue_left',1))" 2>/dev/null) \
            || _resp_ql=$(echo "${resp}" | grep -oP '"queue_left"\s*:\s*\K[0-9]+' || echo 1)
        _resp_assigned=$(echo "${resp}" | python3 -c \
            "import sys,json; print(len(json.load(sys.stdin).get('cases',[])))" 2>/dev/null) \
            || _resp_assigned=0

        if [[ ${_resp_alldone} -eq 1 && ${_resp_assigned} -eq 0 ]]; then
            _all_done=1; break
        fi

        # queue_left=0 且本次没分到用例：记录 streak，后续快速退出
        if [[ "${_resp_ql}" -eq 0 && "${_resp_assigned}" -eq 0 ]]; then
            _empty_streak=$(( ${_empty_streak:-0} + 1 ))
        else
            _empty_streak=0
        fi

        # 解析 wait_ms
        wait_ms=$(echo "${resp}" | python3 -c \
            "import sys,json; print(json.load(sys.stdin).get('wait_ms',0))" 2>/dev/null || echo 0)
        if [[ "${wait_ms}" -gt 0 ]]; then
            echo "[run-test-dynamic] coordinator says wait ${wait_ms}ms (worker load high)"
            _wait_indicated=3   # 接下来 3 轮降低请求量
            sleep $(( wait_ms / 1000 + 1 ))
            continue
        fi

        # 解析 cases 数组，逐一启动
        cases_json=$(echo "${resp}" | python3 -c \
            "import sys,json; [print(c['idx'], c['path'], c['cmd'], c.get('runner','legacy'), c.get('san','n'), sep='\t') for c in json.load(sys.stdin).get('cases',[])]" \
            2>/dev/null || true)

        if [[ -n "${cases_json}" ]]; then
            last_case_time=$(date +%s)
            while IFS=$'\t' read -r c_idx c_path c_cmd c_runner c_san; do
                [[ -z "${c_path}" ]] && continue
                echo "------------------------------------------------------------"
                echo "  [idx=${c_idx}] [${c_runner:-legacy}] [san=${c_san:-n}] ${c_path}::${c_cmd}"

                # 找空闲 slot（若无则先收割一个）
                slot=$(find_free_slot)
                if [[ $slot -eq -1 ]]; then
                    harvest_one
                    process_finished_slot "${FINISHED_SLOT}"
                    in_flight=$(( in_flight - 1 ))
                    slot=$(find_free_slot)
                fi

                # 启动 case
                slot_idx_map[$slot]="${c_idx}"
                ( run_case_in_slot "${slot}" "${c_path}" "${c_cmd}" "${case_seq}" "${c_runner:-legacy}" "${c_san:-n}" ) &
                SLOT_PIDS[$slot]=$!
                in_flight=$(( in_flight + 1 ))
                case_seq=$(( case_seq + 1 ))

            done <<< "${cases_json}"
        fi
    fi

    # 若所有 slot 都忙，或没有新 case，则收割完成的
    if [[ ${free_slots} -eq 0 ]]; then
        harvest_one
        process_finished_slot "${FINISHED_SLOT}"
        in_flight=$(( in_flight - 1 ))
    elif [[ ${in_flight} -gt 0 ]]; then
        # 有 case 在跑但 slot 未满。
        # 若全局队列也空（queue_left=0）且已连续多次没拿到任务，不必继续轮询，
        # 直接进入 drain 模式（等本地容器跑完即可）。
        if [[ "${_resp_ql:-1}" -eq 0 && ${_empty_streak:-0} -ge 3 ]]; then
            echo "[run-test-dynamic] queue_left=0, no new cases, draining ${in_flight} local in-flight case(s)..."
            _all_done=1
            break
        fi
        sleep "${POLL_INTERVAL}"
    else
        # 本地无任务且全局队列空：确认 3 次后立即退出
        if [[ "${_resp_ql:-1}" -eq 0 && ${_empty_streak:-0} -ge 3 ]]; then
            echo "[run-test-dynamic] queue_left=0 and no local work (streak=${_empty_streak}), exiting"
            _all_done=1
            break
        fi
        # 超时保护（协调器长时间不分发任何用例）
        idle_sec=$(( $(date +%s) - last_case_time ))
        if [[ ${idle_sec} -gt ${MAX_IDLE} ]]; then
            echo "[run-test-dynamic] TIMEOUT: no new cases for ${idle_sec}s, exiting"
            break
        fi
        sleep "${POLL_INTERVAL}"
    fi
done

# ── 等待所有 in-flight 完成 ───────────────────────────────────────────────────
echo ""
echo "[run-test-dynamic] Queue done, draining ${in_flight} in-flight case(s)..."
while (( in_flight > 0 )); do
    harvest_one
    process_finished_slot "${FINISHED_SLOT}"
    in_flight=$(( in_flight - 1 ))
done

overall_end=$(date +%s)
overall_elapsed=$(( overall_end - overall_start ))
total_cases=$(( pass_count + fail_count ))

# ── JUnit XML ─────────────────────────────────────────────────────────────────
JUNIT_FILE="${RESULTS_DIR}/junit-${NODE_INDEX}.xml"
cat > "${JUNIT_FILE}" <<JUNIT
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="test-linux-node${NODE_INDEX}-dynamic"
           tests="${total_cases}"
           failures="${fail_count}"
           errors="0"
           skipped="0"
           time="${overall_elapsed}">
${junit_cases}</testsuite>
JUNIT

echo ""
echo "========================================"
echo " Summary — Node ${NODE_INDEX}"
echo "   Total:  ${total_cases}"
echo "   Pass:   ${pass_count}"
echo "   Fail:   ${fail_count}"
echo "   Time:   ${overall_elapsed}s"
echo " Coordinator: ${COORDINATOR_URL}"
echo "========================================"

if [[ ${fail_count} -gt 0 ]]; then
    echo ""
    echo "↓ 展开下方折叠条目可查看失败日志及复现方法"
    echo ""
    # 逐个展开失败详情（collapsed section，点击展开）
    for _fi in "${!failed_labels[@]}"; do
        _fl="${failed_labels[$_fi]}"
        _furl="${failed_urls[$_fi]:-}"
        _fdir="${failed_dirs[$_fi]:-}"
        _flog="${failed_logs[$_fi]:-}"
        _sec_id="summary_fail_n${NODE_INDEX}_${_fi}"
        _sec_title="SUMMARY ${_fl}"
        _ts=$(date +%s)
        echo -e "\e[0Ksection_start:${_ts}:${_sec_id}[collapsed=true]\r\e[0K\e[31;1m${_sec_title}\e[0m"
        echo "────────────────────────────────────────────────────────────────"
        [[ -n "${_furl}" ]] && echo "Case logs:   ${_furl}run.log.txt"
        [[ -n "${_furl}" ]] && echo "Runner logs: ${_furl}"
        [[ -n "${_fdir}" ]] && echo "Fail dir:    ${_fdir}"
        echo "摘要信息:"
        if [[ -n "${_flog}" && -f "${_flog}" ]]; then
            grep -v -E \
                '^\s*(Collecting |Downloading |\u2501+|Requirement already|Successfully (installed|uninstalled)|Attempting uninstall|Found existing installation|Uninstalling |WARNING: Running pip|\[notice\]|-----|Looking in indexes)' \
                "${_flog}" \
                | grep -v '^$' \
                | tail -80
        else
            echo "(log not available)"
        fi
        echo "────────────────────────────────────────────────────────────────"
        echo -e "\e[0Ksection_end:${_ts}:${_sec_id}\r\e[0K"
    done
    exit 1
fi
echo "All tests passed."
