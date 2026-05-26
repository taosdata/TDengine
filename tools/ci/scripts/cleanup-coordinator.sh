#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# CI Coordinator 磁盘清理脚本
# 部署位置: 192.168.2.104  (coordinator / builder 节点)
# 建议 cron: 0 */2 * * *  (每 2 小时执行一次)
#
# 清理策略 (两档保留期):
#   磁盘用量 < 90%: CLEANUP_KEEP_DAYS 天（默认 5，覆盖周末场景）
#   磁盘用量 ≥ 90%: CLEANUP_KEEP_DAYS_URGENT 天（默认 3，紧急压缩）
#
#   mr<N>/ 目录:
#     - merged / closed → 始终删除整个目录（无论磁盘）
#     - open / unknown, 正在运行 (进程占用 or mtime < ACTIVE_GRACE_MIN) → 跳过
#     - open / unknown, idle ≥ 保留天数 → 删整个目录
#     - open / unknown, idle < 保留天数 → 保留
#   coordinator-state/pipeline-<N>/ 目录:
#     - 超过保留天数 → 删除
#   daily-*/web-*/push-*/ 目录:
#     - 超过保留策略 → 删除
#
# 环境变量 / 配置文件:
#   GITLAB_TOKEN              GitLab token (CI 里用 CI_JOB_TOKEN)
#   GITLAB_URL                默认 https://git.tdengine.net
#   DRY_RUN                   设为 1 则只打印不实际删除
#   CLEANUP_KEEP_DAYS         正常保留天数（默认 5）
#   CLEANUP_KEEP_DAYS_URGENT  磁盘 ≥ 90% 时的保留天数（默认 3）
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── 配置 ──────────────────────────────────────────────────────────────────────
WORKDIR="${WORKDIR:-/data1/tdengine-ci}"
GITLAB_URL="${GITLAB_URL:-https://git.tdengine.net}"
PROJECT_PATH="${PROJECT_PATH:-rd-public/tsdb}"
PROJECT_ENC="rd-public%2Ftsdb"

# 正在运行的 job 宽限期 (分钟): mtime 距今 < 此值 → 认为正在运行
ACTIVE_GRACE_MIN="${ACTIVE_GRACE_MIN:-30}"
# 统一保留天数：mr / coordinator-state / daily-* / web-* / push-* 目录（5天覆盖周末）
CLEANUP_KEEP_DAYS="${CLEANUP_KEEP_DAYS:-5}"
# 磁盘用量 ≥ 90% 时的紧急保留天数
CLEANUP_KEEP_DAYS_URGENT="${CLEANUP_KEEP_DAYS_URGENT:-3}"
STATE_KEEP_DAYS="${STATE_KEEP_DAYS:-${CLEANUP_KEEP_DAYS}}"
# daily-<branch>-YYYYMMDD 每个分支保留最新几个
DAILY_KEEP_COUNT="${DAILY_KEEP_COUNT:-3}"
# web-*/push-*/旧式 daily-* 保留天数
DAILY_KEEP_DAYS="${DAILY_KEEP_DAYS:-${CLEANUP_KEEP_DAYS}}"

DRY_RUN="${DRY_RUN:-0}"
LOG_FILE="${LOG_FILE:-/var/log/ci-cleanup-coordinator.log}"
# Token 认证头：CI job 里传 "JOB-TOKEN"（使用 CI_JOB_TOKEN），手动运行传 "PRIVATE-TOKEN"（使用 PAT）
GITLAB_TOKEN_HEADER="${GITLAB_TOKEN_HEADER:-PRIVATE-TOKEN}"

# ── 加载 token 配置 ───────────────────────────────────────────────────────────
# env 文件仅用于手动运行时提供默认值；CI 通过内联赋值传入的 token 优先级更高，
# 必须在 source 之前保存，防止 env 文件里的旧值（如 REPLACE_ME）覆盖它
_caller_token="${GITLAB_TOKEN:-}"
[[ -f /root/.ci-cleanup.env ]] && source /root/.ci-cleanup.env
[[ -n "${_caller_token}" ]] && GITLAB_TOKEN="${_caller_token}"
GITLAB_TOKEN="${GITLAB_TOKEN:-}"

# ── 工具函数 ──────────────────────────────────────────────────────────────────
log() {
    local msg="[$(date '+%F %T')] $*"
    if [[ "${LOG_FILE}" == "/dev/stdout" || "${LOG_FILE}" == "/dev/stderr" || "${LOG_FILE}" == "-" ]]; then
        echo "${msg}"            # tee -a /dev/stdout 会导致双重输出，直接 echo 即可
    else
        echo "${msg}" | tee -a "${LOG_FILE}"
    fi
}
dry_echo() { [[ "${DRY_RUN}" == "1" ]] && log "[DRY-RUN] $*" || true; }

do_rm() {
    local target="$1"
    if [[ "${DRY_RUN}" == "1" ]]; then
        dry_echo "rm -rf ${target}"
    else
        log "Removing: ${target}"
        rm -rf "${target}"
    fi
}

# 获取 WORKDIR 所在磁盘的使用率 (整数, %)
disk_usage_pct() {
    df "${WORKDIR}" --output=pcent 2>/dev/null | tail -1 | tr -d ' %' || echo "0"
}

# 查询 GitLab MR 状态 → 输出 "open" / "merged" / "closed" / "unknown"
mr_state() {
    local iid="$1"
    if [[ -z "${GITLAB_TOKEN}" ]]; then
        log "WARN: GITLAB_TOKEN is empty — MR state check disabled, all MRs treated as unknown"
        echo "unknown"
        return
    fi
    local url="${GITLAB_URL}/api/v4/projects/${PROJECT_ENC}/merge_requests/${iid}"
    local raw http_code body
    # -w 追加 HTTP 状态码到最后一行，方便调试
    raw=$(curl -s --max-time 10 \
        -w "\n%{http_code}" \
        --header "${GITLAB_TOKEN_HEADER}: ${GITLAB_TOKEN}" \
        "${url}" 2>/dev/null)
    http_code=$(printf '%s' "${raw}" | tail -1)
    body=$(printf '%s' "${raw}" | head -n -1)
    if [[ "${http_code}" != "200" ]]; then
        log "WARN: GitLab API HTTP ${http_code} for MR#${iid} (${url}) — treated as unknown"
        echo "unknown"
        return
    fi
    printf '%s' "${body}" | python3 -c \
        "import json,sys; d=json.load(sys.stdin); print(d.get('state','unknown'))" \
        2>/dev/null || echo "unknown"
}

# 判断 mr<N> 目录是否有活跃进程
# 策略: 检查有无 build.sh 进程明确引用该路径 (避免 grep-self 误匹配)
mr_is_active_process() {
    local mrdir="$1"
    local full_path="${WORKDIR}/${mrdir}"
    # 检查 build.sh 进程是否正在使用该路径 (cleanup 脚本自身不含 build.sh，不会误匹配)
    ps aux 2>/dev/null | grep -v grep | grep "build.sh" | grep -q "${full_path}" && return 0
    # 检查 cmake/make/ninja 等编译进程是否在该目录下运行
    ps aux 2>/dev/null | grep -v grep \
        | grep -E "cmake|ninja|make|taosd|taosadapter" \
        | grep -q "${full_path}" && return 0
    return 1
}

# 判断目录 mtime 是否在 N 分钟内 (返回 0=是/活跃, 1=否)
dir_is_recently_modified() {
    local dir="$1"
    local minutes="$2"
    local now; now=$(date +%s)
    local mtime; mtime=$(stat -c %Y "${dir}" 2>/dev/null || echo 0)
    (( now - mtime < minutes * 60 )) && return 0 || return 1
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

# ── 加互斥锁，防止并发执行 ───────────────────────────────────────────────────
LOCK_FILE="/tmp/ci-cleanup-coordinator.lock"
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
    log "Another cleanup is already running (lock: ${LOCK_FILE}), skipping."
    exit 0
fi

# ── 主逻辑 ────────────────────────────────────────────────────────────────────
log "=== Coordinator cleanup started (DRY_RUN=${DRY_RUN}) ==="
log "WORKDIR=${WORKDIR}  CLEANUP_KEEP_DAYS=${CLEANUP_KEEP_DAYS}  STATE_KEEP_DAYS=${STATE_KEEP_DAYS}"
# 打印 token 诊断（只显示前8字符，帮助排查 CI 变量是否传入）
_tok_preview="${GITLAB_TOKEN:0:8}"
_tok_len="${#GITLAB_TOKEN}"
log "GITLAB_URL=${GITLAB_URL}  GITLAB_TOKEN=${_tok_preview}***  (len=${_tok_len})"
coord_usage=$(disk_usage_pct)
if (( coord_usage >= 90 )); then
    log "Disk usage: ${coord_usage}% (>= 90%, free < 10%) — urgent mode: retention ${CLEANUP_KEEP_DAYS_URGENT} days"
    CLEANUP_KEEP_DAYS=${CLEANUP_KEEP_DAYS_URGENT}
    STATE_KEEP_DAYS=${CLEANUP_KEEP_DAYS_URGENT}
    DAILY_KEEP_DAYS=${CLEANUP_KEEP_DAYS_URGENT}
else
    log "Disk usage: ${coord_usage}% — normal mode: retention ${CLEANUP_KEEP_DAYS} days"
fi

# ── 1. 清理 mr<N>[-p<PipelineID>]/ 目录 ─────────────────────────────────────
# 支持两种命名格式：
#   旧格式：mr<N>/         （多次 push 共享同一目录，已废弃）
#   新格式：mr<N>-p<ID>/  （每次 pipeline 独占，避免新老 pipeline 竞争删 workspace）
log "--- Scanning MR workspace directories ---"

for mrpath in "${WORKDIR}"/mr*/; do
    [[ -d "${mrpath}" ]] || continue
    mrdir=$(basename "${mrpath}")
    # 提取 MR IID：兼容旧格式 mr<N> 和新格式 mr<N>-p<PipelineID>
    if [[ "${mrdir}" =~ ^mr([0-9]+)-p([0-9]+)$ ]]; then
        iid="${BASH_REMATCH[1]}"
    elif [[ "${mrdir}" =~ ^mr([0-9]+)$ ]]; then
        iid="${BASH_REMATCH[1]}"
    else
        log "SKIP ${mrdir}: unrecognized format"
        continue
    fi

    # 检查是否有活跃进程
    if mr_is_active_process "${mrdir}"; then
        log "SKIP ${mrdir}: active process found"
        continue
    fi

    # 检查目录是否最近被修改 (活跃宽限期)
    if dir_is_recently_modified "${mrpath}" "${ACTIVE_GRACE_MIN}"; then
        log "SKIP ${mrdir}: recently modified (< ${ACTIVE_GRACE_MIN}min)"
        continue
    fi

    # 查询 GitLab MR 状态
    state=$(mr_state "${iid}")
    log "CHECK ${mrdir}: state=${state}"

    case "${state}" in
        merged|closed)
            # 已合并/关闭 → 始终删整个目录
            sz=$(du -sh "${mrpath}" 2>/dev/null | cut -f1 || echo "?")
            log "DELETE ${mrdir} (${state}, ${sz})"
            do_rm "${mrpath}"
            ;;
        open|unknown)
            # open: 超过保留期未活跃 → 删整个目录
            # unknown (API 不可用): 同样按时间兜底，CLEANUP_KEEP_DAYS 天后删除
            now=$(date +%s)
            mtime=$(stat -c %Y "${mrpath}" 2>/dev/null || echo "${now}")
            age_days=$(( (now - mtime) / 86400 ))
            if (( age_days >= CLEANUP_KEEP_DAYS )); then
                sz=$(du -sh "${mrpath}" 2>/dev/null | cut -f1 || echo "?")
                log "DELETE ${mrdir} (${state}, idle ${age_days}d >= ${CLEANUP_KEEP_DAYS}d, ${sz})"
                do_rm "${mrpath}"
            else
                log "KEEP ${mrdir} (${state}, idle ${age_days}d < ${CLEANUP_KEEP_DAYS}d)"
            fi
            ;;
    esac
done

# ── 2. 清理 coordinator-state/ 旧 pipeline 目录 ───────────────────────────────
log "--- Cleaning coordinator-state (keep ${STATE_KEEP_DAYS} days) ---"
state_dir="${WORKDIR}/coordinator-state"
if [[ -d "${state_dir}" ]]; then
    count=0
    while IFS= read -r -d '' dir; do
        count=$(( count + 1 ))
        do_rm "${dir}"
    done < <(find "${state_dir}" -mindepth 1 -maxdepth 1 -type d \
                -mtime "+${STATE_KEEP_DAYS}" -print0)
    log "Removed ${count} old pipeline-state directories"
fi

# ── 3. 清理 daily-*/web-*/push-* 目录 ──────────────────────────────────────────
log "--- Cleaning daily/web/push workspace directories ---"

# daily-<branch>-YYYYMMDD: 按分支分组，每组保留最新 DAILY_KEEP_COUNT 个
mapfile -t _daily_branches < <(
    find "${WORKDIR}" -maxdepth 1 -type d -name 'daily-*' 2>/dev/null \
    | while IFS= read -r _d; do
        _b=$(basename "${_d}")
        if [[ "${_b}" =~ ^daily-(.+)-[0-9]{8}$ ]]; then
            echo "${BASH_REMATCH[1]}"
        fi
    done | sort -u
)

for _br in "${_daily_branches[@]}"; do
    mapfile -t _dirs < <(
        find "${WORKDIR}" -maxdepth 1 -type d \
             -name "daily-${_br}-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]" \
             2>/dev/null | sort -r
    )
    _idx=0
    for _dir in "${_dirs[@]}"; do
        _idx=$(( _idx + 1 ))
        if (( _idx <= DAILY_KEEP_COUNT )); then
            log "KEEP $(basename \"${_dir}\") (daily/${_br}, rank ${_idx}/${DAILY_KEEP_COUNT})"
        else
            _sz=$(du -sh "${_dir}" 2>/dev/null | cut -f1 || echo "?")
            log "DELETE $(basename \"${_dir}\") (daily/${_br}, rank ${_idx}, ${_sz})"
            do_rm "${_dir}"
        fi
    done
done

# 不符合 daily-<branch>-YYYYMMDD 格式的 daily-* (旧式/手动创建)，超过 DAILY_KEEP_DAYS 天删除
while IFS= read -r -d '' _dir; do
    _bname=$(basename "${_dir}")
    if [[ "${_bname}" =~ ^daily-.*-[0-9]{8}$ ]]; then continue; fi
    _now=$(date +%s)
    _mt=$(stat -c %Y "${_dir}" 2>/dev/null || echo "${_now}")
    _age=$(( (_now - _mt) / 86400 ))
    if (( _age > DAILY_KEEP_DAYS )); then
        _sz=$(du -sh "${_dir}" 2>/dev/null | cut -f1 || echo "?")
        log "DELETE ${_bname} (non-dated daily, ${_age}d old, ${_sz})"
        do_rm "${_dir}"
    else
        log "KEEP ${_bname} (non-dated daily, ${_age}d old)"
    fi
done < <(find "${WORKDIR}" -maxdepth 1 -type d -name 'daily-*' -print0 2>/dev/null)

# web-<ID>/ 和 push-<branch>-<sha>/: mtime 超过 DAILY_KEEP_DAYS 天则删除
while IFS= read -r -d '' _dir; do
    _bname=$(basename "${_dir}")
    _now=$(date +%s)
    _mt=$(stat -c %Y "${_dir}" 2>/dev/null || echo "${_now}")
    _age=$(( (_now - _mt) / 86400 ))
    if (( _age > DAILY_KEEP_DAYS )); then
        _sz=$(du -sh "${_dir}" 2>/dev/null | cut -f1 || echo "?")
        log "DELETE ${_bname} (${_age}d old, ${_sz})"
        do_rm "${_dir}"
    else
        log "KEEP ${_bname} (${_age}d old)"
    fi
done < <(find "${WORKDIR}" -maxdepth 1 -type d \
         \( -name 'web-*' -o -name 'push-*' \) -print0 2>/dev/null)

log "=== Coordinator cleanup done (disk: ${coord_usage}%) ==="
