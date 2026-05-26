#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# CI 清理脚本部署安装器
#
# 用法:
#   # 在 coordinator (192.168.2.104) 上安装
#   bash deploy-cleanup.sh coordinator
#
#   # 在单个 worker 上安装
#   bash deploy-cleanup.sh worker
#
#   # 批量推送到所有 worker (从任意可 SSH 到所有 worker 的机器运行)
#   bash deploy-cleanup.sh push-workers "192.168.3.141 192.168.3.142 192.168.3.143 192.168.3.145 192.168.3.146 192.168.3.147"
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="/usr/local/bin"
CRON_FILE_COORDINATOR="/etc/cron.d/ci-cleanup-coordinator"
CRON_FILE_WORKER="/etc/cron.d/ci-cleanup-worker"

install_coordinator() {
    echo "[deploy] Installing coordinator cleanup on $(hostname)..."

    install -m 755 "${SCRIPT_DIR}/cleanup-coordinator.sh" "${INSTALL_DIR}/ci-cleanup-coordinator"

    # 写 cron (每 2 小时执行一次)
    cat > "${CRON_FILE_COORDINATOR}" <<'CRON'
# CI Coordinator disk cleanup — runs every 2 hours
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

0 */2 * * * root /usr/local/bin/ci-cleanup-coordinator >> /var/log/ci-cleanup-coordinator.log 2>&1
CRON
    chmod 644 "${CRON_FILE_COORDINATOR}"

    # 创建 token 配置文件模板（如果不存在）
    if [[ ! -f /root/.ci-cleanup.env ]]; then
        cat > /root/.ci-cleanup.env <<'ENV'
# GitLab Personal Access Token (需要 read_api 权限)
# 在 GitLab → User Settings → Access Tokens 创建
GITLAB_TOKEN=glpat-REPLACE_ME
GITLAB_URL=https://git.tdengine.net
PROJECT_PATH=rd-public/tsdb
ENV
        chmod 600 /root/.ci-cleanup.env
        echo "[deploy] Created /root/.ci-cleanup.env — please fill in GITLAB_TOKEN"
    fi

    echo "[deploy] Coordinator cleanup installed. Cron: ${CRON_FILE_COORDINATOR}"
    echo "[deploy] Test run (dry): DRY_RUN=1 ci-cleanup-coordinator"
}

install_worker() {
    echo "[deploy] Installing worker cleanup on $(hostname)..."

    install -m 755 "${SCRIPT_DIR}/cleanup-worker.sh" "${INSTALL_DIR}/ci-cleanup-worker"

    # 写 cron (每 30 分钟执行一次)
    cat > "${CRON_FILE_WORKER}" <<'CRON'
# CI Worker disk cleanup — runs every 30 minutes
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

*/30 * * * * root /usr/local/bin/ci-cleanup-worker >> /var/log/ci-cleanup-worker.log 2>&1
CRON
    chmod 644 "${CRON_FILE_WORKER}"

    echo "[deploy] Worker cleanup installed. Cron: ${CRON_FILE_WORKER}"
    echo "[deploy] Test run (dry): DRY_RUN=1 ci-cleanup-worker"
}

push_workers() {
    local workers=($1)
    local script_path="${SCRIPT_DIR}/cleanup-worker.sh"

    echo "[deploy] Pushing to ${#workers[@]} worker nodes..."
    for ip in "${workers[@]}"; do
        echo "[deploy] → ${ip}"
        scp -q "${script_path}" "root@${ip}:/tmp/cleanup-worker.sh"
        ssh "root@${ip}" "
            install -m 755 /tmp/cleanup-worker.sh ${INSTALL_DIR}/ci-cleanup-worker
            rm /tmp/cleanup-worker.sh
            cat > ${CRON_FILE_WORKER} <<'CRON'
# CI Worker disk cleanup — runs every 30 minutes
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

*/30 * * * * root /usr/local/bin/ci-cleanup-worker >> /var/log/ci-cleanup-worker.log 2>&1
CRON
            chmod 644 ${CRON_FILE_WORKER}
            echo 'Installed on \$(hostname)'
        "
    done
    echo "[deploy] All workers updated."
}

case "${1:-}" in
    coordinator) install_coordinator ;;
    worker)      install_worker ;;
    push-workers) push_workers "${2:-}" ;;
    *)
        echo "Usage: $0 {coordinator|worker|push-workers '<ip1> <ip2> ...'}"
        exit 1
        ;;
esac
