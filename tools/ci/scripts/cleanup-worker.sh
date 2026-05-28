#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# CI Worker 节点磁盘清理脚本
# 部署位置: 所有 worker 节点 (u3-141 ~ u3-147 等)
# 建议 cron: */30 * * * *  (每 30 分钟执行一次)
#
# 清理策略 (两档保留期):
#   磁盘用量 < 90%: CLEANUP_KEEP_DAYS 天（默认 5，覆盖周末场景）
#   磁盘用量 ≥ 90%: CLEANUP_KEEP_DAYS_URGENT 天（默认 3，紧急压缩）
#
#   fail-logs/job-<N>/: 超过保留天数 → 删除
#   job-<N>/ (顶层工作目录): 无活跃进程 AND 超过保留天数 → 删除
#                   进阶: 若 job 所属 MR 已 merged/closed → 立即删除（不受保留天数限制）
#
# 环境变量 (可在 /root/.ci-cleanup.env 中配置):
#   WORKDIR                   默认 /data1/tdengine-ci
#   DISK_MOUNT                要监控的挂载点, 默认同 WORKDIR 所在分区
#   CLEANUP_KEEP_DAYS         正常保留天数 (默认 5)
#   CLEANUP_KEEP_DAYS_URGENT  磁盘 ≥ 90% 时的保留天数 (默认 3)
#   DRY_RUN                   设为 1 则只打印不实际删除
#   GITLAB_TOKEN              GitLab token（CI 里自动用 CI_JOB_TOKEN 注入）
#   GITLAB_URL                默认 https://git.tdengine.net
#   PROJECT_PATH              默认 rd-public/tsdb
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── 配置 ──────────────────────────────────────────────────────────────────────
# 先保存调用方（CI inline 赋值）传入的 token，防止 env 文件里的旧值（如 REPLACE_ME）覆盖它
_caller_token="${GITLAB_TOKEN:-}"
[[ -f /root/.ci-cleanup.env ]] && source /root/.ci-cleanup.env
[[ -n "${_caller_token}" ]] && GITLAB_TOKEN="${_caller_token}"

WORKDIR="${WORKDIR:-/data1/tdengine-ci}"
# 监控 WORKDIR 所在分区（防止不同机器 /data vs /data1 导致 df 返回 0）
DISK_MOUNT="${DISK_MOUNT:-${WORKDIR}}"

CLEANUP_KEEP_DAYS="${CLEANUP_KEEP_DAYS:-5}"
# 磁盘用量 ≥ 90% 时的紧急保留天数
CLEANUP_KEEP_DAYS_URGENT="${CLEANUP_KEEP_DAYS_URGENT:-3}"

DRY_RUN="${DRY_RUN:-0}"
LOG_FILE="${LOG_FILE:-/var/log/ci-cleanup-worker.log}"
HOSTNAME_SHORT="${HOSTNAME_SHORT:-$(hostname -s)}"

# GitLab API（用于查询 job 所属 MR 状态，可选；不配置时退化为纯时间清理）
GITLAB_TOKEN_HEADER="${GITLAB_TOKEN_HEADER:-PRIVATE-TOKEN}"
GITLAB_TOKEN="${GITLAB_TOKEN:-}"
GITLAB_URL="${GITLAB_URL:-https://git.tdengine.net}"
PROJECT_PATH="${PROJECT_PATH:-rd-public/tsdb}"
PROJECT_ENC="${PROJECT_ENC:-rd-public%2Ftsdb}"

# ── 工具函数 ──────────────────────────────────────────────────────────────────
log() {
    local msg="[$(date '+%F %T')] [${HOSTNAME_SHORT}] $*"
    if [[ "${LOG_FILE}" == "/dev/stdout" || "${LOG_FILE}" == "/dev/stderr" || "${LOG_FILE}" == "-" ]]; then
        echo "${msg}"
    else
        echo "${msg}" | tee -a "${LOG_FILE}"
    fi
}

do_rm() {
    local target="$1"
    if [[ "${DRY_RUN}" == "1" ]]; then
        log "[DRY-RUN] rm -rf ${target}"
    else
        local sz; sz=$(du -sh "${target}" 2>/dev/null | cut -f1 || echo "?")
        log "Removing ${target} (${sz})"
        rm -rf "${target}"
    fi
}

# 获取磁盘使用率 (整数, %)
disk_usage_pct() {
    df "${DISK_MOUNT}" --output=pcent 2>/dev/null | tail -1 | tr -d ' %' || echo "0"
}

# 判断某个 job 目录是否有活跃进程在使用
# 用 pgrep -f 避免 'ps aux | grep' 经典自匹配 bug（grep 进程本身含路径字符串会误判）
job_is_active() {
    local jobdir="$1"   # e.g. job-31163
    pgrep -f "${WORKDIR}/${jobdir}" > /dev/null 2>&1
}

# 通过 GitLab API 查询 job 所属 MR 状态
# 返回: "merged" | "closed" | "open" | "unknown"
#   unknown: token 未配置 / API 失败 / job 不属于 MR pipeline
job_mr_state() {
    local job_id="$1"
    [[ -z "${GITLAB_TOKEN}" ]] && echo "unknown" && return

    local raw http_code body

    # Step 1: GET /jobs/:id → 获取 pipeline_id
    raw=$(curl -s --max-time 8 \
        -w "\n%{http_code}" \
        --header "${GITLAB_TOKEN_HEADER}: ${GITLAB_TOKEN}" \
        "${GITLAB_URL}/api/v4/projects/${PROJECT_ENC}/jobs/${job_id}" 2>/dev/null)
    http_code=$(printf '%s' "${raw}" | tail -1)
    body=$(printf '%s' "${raw}" | head -n -1)
    [[ "${http_code}" != "200" ]] && echo "unknown" && return

    local pipeline_id
    pipeline_id=$(printf '%s' "${body}" | python3 -c \
        "import json,sys; d=json.load(sys.stdin); print(d.get('pipeline',{}).get('id',''))" \
        2>/dev/null)
    [[ -z "${pipeline_id}" ]] && echo "unknown" && return

    # Step 2: GET /pipelines/:id → 获取 merge_request_iid（非 MR pipeline 为 null/空）
    raw=$(curl -s --max-time 8 \
        -w "\n%{http_code}" \
        --header "${GITLAB_TOKEN_HEADER}: ${GITLAB_TOKEN}" \
        "${GITLAB_URL}/api/v4/projects/${PROJECT_ENC}/pipelines/${pipeline_id}" 2>/dev/null)
    http_code=$(printf '%s' "${raw}" | tail -1)
    body=$(printf '%s' "${raw}" | head -n -1)
    [[ "${http_code}" != "200" ]] && echo "unknown" && return

    local mr_iid
    mr_iid=$(printf '%s' "${body}" | python3 -c \
        "import json,sys; d=json.load(sys.stdin); v=d.get('merge_request_iid'); print(v if v else '')" \
        2>/dev/null)

    if [[ -z "${mr_iid}" ]]; then
        # 子流水线（trigger 触发）的 merge_request_iid 为 null；
        # 回退：用 pipeline.ref（分支名）搜索关联 MR（最近更新的一条）
        local ref ref_enc mr_raw mr_http mr_body
        ref=$(printf '%s' "${body}" | python3 -c \
            "import json,sys; print(json.load(sys.stdin).get('ref',''))" 2>/dev/null)
        if [[ -n "${ref}" ]]; then
            ref_enc=$(python3 -c \
                "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1],safe=''))" \
                "${ref}" 2>/dev/null)
            mr_raw=$(curl -s --max-time 8 \
                -w "\n%{http_code}" \
                --header "${GITLAB_TOKEN_HEADER}: ${GITLAB_TOKEN}" \
                "${GITLAB_URL}/api/v4/projects/${PROJECT_ENC}/merge_requests?source_branch=${ref_enc}&order_by=updated_at&sort=desc&per_page=1" \
                2>/dev/null)
            mr_http=$(printf '%s' "${mr_raw}" | tail -1)
            mr_body=$(printf '%s' "${mr_raw}" | head -n -1)
            if [[ "${mr_http}" == "200" ]]; then
                mr_iid=$(printf '%s' "${mr_body}" | python3 -c \
                    "import json,sys; mrs=json.load(sys.stdin); print(mrs[0].get('iid','') if mrs else '')" \
                    2>/dev/null)
            fi
        fi
        [[ -z "${mr_iid}" ]] && echo "unknown" && return  # push/schedule pipeline 或 API 不可用
    fi

    # Step 3: GET /merge_requests/:iid → 获取 MR 状态
    raw=$(curl -s --max-time 8 \
        -w "\n%{http_code}" \
        --header "${GITLAB_TOKEN_HEADER}: ${GITLAB_TOKEN}" \
        "${GITLAB_URL}/api/v4/projects/${PROJECT_ENC}/merge_requests/${mr_iid}" 2>/dev/null)
    http_code=$(printf '%s' "${raw}" | tail -1)
    body=$(printf '%s' "${raw}" | head -n -1)
    [[ "${http_code}" != "200" ]] && echo "unknown" && return

    printf '%s' "${body}" | python3 -c \
        "import json,sys; d=json.load(sys.stdin); print(d.get('state','unknown'))" \
        2>/dev/null || echo "unknown"
}

# 删除指定目录下 mtime 超过 N 分钟 的子目录
cleanup_old_subdirs() {
    local parent="$1"
    local keep_minutes="$2"
    local label="$3"
    [[ -d "${parent}" ]] || return 0
    local count=0
    while IFS= read -r -d '' dir; do
        count=$(( count + 1 ))
        do_rm "${dir}"
    done < <(find "${parent}" -mindepth 1 -maxdepth 1 -type d \
                -mmin "+${keep_minutes}" -print0 2>/dev/null)
    log "${label}: removed ${count} old directories (keep < ${keep_minutes}min)"
}

# ── WORKDIR 合法性校验（防止误操作系统目录）────────────────────────────────
if [[ -z "${WORKDIR}" ]]; then
    echo "[ERROR] WORKDIR is empty, refusing to run" >&2; exit 1
fi
if [[ "${WORKDIR}" == "/" || "${WORKDIR}" == "/root" || "${WORKDIR}" == "/home" \
    || "${WORKDIR}" == "/etc" || "${WORKDIR}" == "/var" || "${WORKDIR}" == "/usr" \
    || "${WORKDIR}" == "/tmp" ]]; then
    echo "[ERROR] WORKDIR='${WORKDIR}' looks like a system directory, refusing to run" >&2; exit 1
fi
if [[ ! -d "${WORKDIR}" ]]; then
    echo "[ERROR] WORKDIR='${WORKDIR}' does not exist" >&2; exit 1
fi

# ── 加互斥锁 ──────────────────────────────────────────────────────────────────
LOCK_FILE="/tmp/ci-cleanup-worker.lock"
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
    log "Another cleanup is already running, skipping."
    exit 0
fi

# ── 主逻辑 ────────────────────────────────────────────────────────────────────
usage=$(disk_usage_pct)
# 清理前可用容量（GB），用于最后计算实际释放量（整数百分比在大磁盘上精度不足）
_avail_before=$(df "${DISK_MOUNT}" --output=avail -BG 2>/dev/null | tail -1 | tr -d 'G ' || echo "")
if (( usage >= 90 )); then
    CLEANUP_KEEP_DAYS=${CLEANUP_KEEP_DAYS_URGENT}
    _mode="urgent(disk>=90%)"
else
    _mode="normal"
fi
keep_min=$(( CLEANUP_KEEP_DAYS * 24 * 60 ))
log "=== Worker cleanup started | disk=${usage}% | keep=${CLEANUP_KEEP_DAYS}d [${_mode}] | DRY_RUN=${DRY_RUN} ==="

# ── 1. 清理 fail-logs/job-<N>/ 目录 ──────────────────────────────────────────
fail_logs_dir="${WORKDIR}/fail-logs"
if [[ -d "${fail_logs_dir}" ]]; then
    cleanup_old_subdirs "${fail_logs_dir}" "${keep_min}" "fail-logs"
else
    log "No fail-logs directory found, skipping"
fi

# ── 2. 清理顶层 job-<N>/ 工作目录 ────────────────────────────────────────────
log "--- Scanning top-level job-<N> directories ---"
job_removed=0
for jobpath in "${WORKDIR}"/job-*/; do
    [[ -d "${jobpath}" ]] || continue
    jobdir=$(basename "${jobpath}")

    # 跳过有活跃进程的 job 目录
    if job_is_active "${jobdir}"; then
        log "SKIP ${jobdir}: active process"
        continue
    fi

    # 若 job 所属 MR 已 merged/closed，立即删除（不受保留天数限制）
    local_job_id="${jobdir#job-}"
    _mr_st=$(job_mr_state "${local_job_id}")
    if [[ "${_mr_st}" == "merged" || "${_mr_st}" == "closed" ]]; then
        log "REMOVE ${jobdir}: MR state=${_mr_st}"
        do_rm "${jobpath}"
        job_removed=$(( job_removed + 1 ))
        continue
    fi

    # 按时间保留策略决定是否删除
    now=$(date +%s)
    mtime=$(stat -c %Y "${jobpath}" 2>/dev/null || echo "${now}")
    age_min=$(( (now - mtime) / 60 ))

    if (( age_min > keep_min )); then
        do_rm "${jobpath}"
        job_removed=$(( job_removed + 1 ))
    else
        log "KEEP ${jobdir} (age=${age_min}min <= ${keep_min}min; mr=${_mr_st})"
    fi
done
log "Removed ${job_removed} old job directories"

# ── 3. 清理完成后打印磁盘状态 ────────────────────────────────────────────────
usage_after=$(disk_usage_pct)
_freed_str=""
_avail_after=$(df "${DISK_MOUNT}" --output=avail -BG 2>/dev/null | tail -1 | tr -d 'G ' || echo "")
if [[ "${_avail_before}" =~ ^[0-9]+$ && "${_avail_after}" =~ ^[0-9]+$ ]]; then
    _freed_str=" | freed=$(( _avail_after - _avail_before ))GB"
fi
log "=== Worker cleanup done | disk: ${usage}% → ${usage_after}%${_freed_str} ==="
