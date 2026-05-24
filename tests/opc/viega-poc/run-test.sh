#!/bin/bash
# ==========================================================================
#  Viega PoC — OPC UA 断链缓存自动化测试
# ==========================================================================
#
#  用法：
#    sudo ./run-test.sh [--case T1|T2|T3|T4|ALL] [--points 1000]
#
#  前置条件：
#    1. 192.168.2.139 已部署 taosx + TDengine，Explorer 可访问
#    2. 本地已编译 taosx-agent（/Users/yangzy/Projects/taosx/target/release/taosx-agent）
#    3. 本地 OPC UA 模拟器可运行（/Users/yangzy/workspace/opcua-demo/viega-poc/viega-poc.js）
#    4. agent.toml 已配置好 endpoint 和 token
#    5. 需要 sudo 权限（pfctl 网络模拟）
#
#  测试用例：
#    T1: persist_data_enable=false, 断链 5min  (对照组)
#    T2: persist_data_enable=true,  断链 30s
#    T3: persist_data_enable=true,  断链 5min  (核心)
#    T4: persist_data_enable=true,  断链 30min
#
# ==========================================================================

set -euo pipefail

# ======================== 配置区 ========================
TAOSX_HOST="192.168.2.139"
EXPLORER_PORT=6060
TDENGINE_REST_PORT=6041
TAOSX_RPC_PORT=6055

AGENT_BIN="/Users/yangzy/Projects/taosx/target/release/taosx-agent"
AGENT_CONF="/etc/taos/agent.toml"
AGENT_DATA_DIR="/Users/yangzy/taosx/data/agent"  # agent.toml 中配置的 data_dir

OPCUA_SIM_DIR="/Users/yangzy/workspace/opcua-demo/viega-poc"
OPCUA_SIM_SCRIPT="viega-poc.js"
OPCUA_PORT=4840
OPCUA_POINTS=1000

VERIFY_SCRIPT="${OPCUA_SIM_DIR}/verify-data.js"

AGENT_LOG_DIR="/Users/yangzy/taosx/log"  # agent.toml 中配置的日志目录

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results/$(date '+%Y%m%d-%H%M%S')"

# 稳定运行时间（断链前/后各运行多久）
WARMUP_SECS=30     # pipeline 就绪后，积累基准数据 30s
COOLDOWN_SECS=120  # 恢复后 2 分钟

TD_USER="root"
TD_PASS="taosdata"

# PF rules 临时文件
PF_RULES="/tmp/pf-viega-test.conf"

# ======================== 解析参数 ========================
TEST_CASE="ALL"
for arg in "$@"; do
  case "$arg" in
    --case=*) TEST_CASE="${arg#*=}" ;;
    --points=*) OPCUA_POINTS="${arg#*=}" ;;
  esac
done

# ======================== 工具函数 ========================
log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { echo -e "\033[32m[$(date '+%H:%M:%S')] ✅ $*\033[0m"; }
warn() { echo -e "\033[33m[$(date '+%H:%M:%S')] ⚠️  $*\033[0m"; }
fail() { echo -e "\033[31m[$(date '+%H:%M:%S')] ❌ $*\033[0m"; }

wait_secs() {
  local total=$1
  local label=${2:-"等待"}
  log "${label}（${total}s）..."
  local i=0
  while [ $i -lt "$total" ]; do
    sleep 10
    i=$((i + 10))
    if [ $i -ge "$total" ]; then break; fi
    printf "  %d/%ds\r" "$i" "$total"
  done
  echo ""
}

td_exec() {
  local sql="$1"
  local db="${2:-}"
  local url="http://${TAOSX_HOST}:${TDENGINE_REST_PORT}/rest/sql"
  [ -n "$db" ] && url="${url}/${db}"
  curl -s -u "${TD_USER}:${TD_PASS}" -d "$sql" "$url"
}

td_query_count() {
  local db="$1"
  local stable="$2"
  local result
  result=$(td_exec "SELECT COUNT(*) FROM ${stable}" "$db" 2>/dev/null)
  echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('data',[[0]])[0][0])" 2>/dev/null || echo "0"
}

explorer_api() {
  local method="$1"
  local path="$2"
  local data="${3:-}"
  local url="http://${TAOSX_HOST}:${EXPLORER_PORT}${path}"
  if [ -n "$data" ]; then
    curl -s -X "$method" -H "Content-Type: application/json" \
      -u "${TD_USER}:${TD_PASS}" -d "$data" "$url"
  else
    curl -s -X "$method" -H "Content-Type: application/json" \
      -u "${TD_USER}:${TD_PASS}" "$url"
  fi
}

# 获取本机 IP（面向 taosx 那个网段的）
get_local_ip() {
  # 取面向目标主机的路由出口 IP
  local ip
  ip=$(python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(('${TAOSX_HOST}', 80))
print(s.getsockname()[0])
s.close()
" 2>/dev/null)
  echo "$ip"
}

# ======================== 网络控制 ========================
network_block() {
  log "🔌 断开到 ${TAOSX_HOST} 的网络..."
  echo "block drop out proto tcp from any to ${TAOSX_HOST}" > "$PF_RULES"
  echo "block drop in proto tcp from ${TAOSX_HOST} to any" >> "$PF_RULES"
  pfctl -ef "$PF_RULES" 2>/dev/null
  info "网络已断开"
}

network_unblock() {
  log "🔗 恢复网络..."
  pfctl -d 2>/dev/null || true
  rm -f "$PF_RULES"
  info "网络已恢复"
}

# ======================== 进程管理 ========================
OPCUA_PID=""
AGENT_PID=""

CSV_START_FILE=""

start_opcua_sim() {
  local log_dir="${RESULTS_DIR}/${1}/opcua-logs"
  local stop_after="${2:-0}"
  mkdir -p "$log_dir"
  CSV_START_FILE="${RESULTS_DIR}/${1}/csv-start-signal"
  rm -f "$CSV_START_FILE"
  log "启动 OPC UA 模拟器（${OPCUA_POINTS} 点位, stop-after=${stop_after}s）..."
  cd "$OPCUA_SIM_DIR"
  local sim_args=(
    --port "$OPCUA_PORT"
    --points "$OPCUA_POINTS"
    --log-dir "$log_dir"
    --csv-start-file "$CSV_START_FILE"
  )
  if [ "$stop_after" -gt 0 ]; then
    sim_args+=(--stop-after "$stop_after")
  fi
  node "$OPCUA_SIM_SCRIPT" "${sim_args[@]}" \
    > "${RESULTS_DIR}/${1}/opcua-sim.log" 2>&1 &
  OPCUA_PID=$!
  cd - > /dev/null
  # 1000 个点位需要 ~15s 完成初始化并开始监听
  sleep 15
  if kill -0 "$OPCUA_PID" 2>/dev/null; then
    info "OPC UA 模拟器已启动 (PID=$OPCUA_PID)"
  else
    fail "OPC UA 模拟器启动失败"
    cat "${RESULTS_DIR}/${1}/opcua-sim.log"
    return 1
  fi
}

stop_opcua_sim() {
  if [ -n "$OPCUA_PID" ] && kill -0 "$OPCUA_PID" 2>/dev/null; then
    log "停止 OPC UA 模拟器..."
    kill -SIGTERM "$OPCUA_PID" 2>/dev/null || true
    wait "$OPCUA_PID" 2>/dev/null || true
    sleep 1  # 等待 shutdown handler 完成 CSV 修剪
    info "OPC UA 模拟器已停止"
  fi
  OPCUA_PID=""
}

start_agent() {
  local case_dir="${RESULTS_DIR}/${1}"
  log "启动 taosx-agent..."
  # 使用 nohup 避免 SIGHUP 导致 agent 退出
  nohup "$AGENT_BIN" --config "$AGENT_CONF" \
    > "${case_dir}/agent-stdout.log" 2>&1 &
  AGENT_PID=$!
  disown "$AGENT_PID" 2>/dev/null || true
  sleep 8  # 等待 agent 完成连接
  if kill -0 "$AGENT_PID" 2>/dev/null; then
    info "taosx-agent 已启动 (PID=$AGENT_PID)"
  else
    fail "taosx-agent 启动失败"
    tail -20 "${case_dir}/agent-stdout.log"
    return 1
  fi
  # 验证 agent 已在 Explorer 中显示为 connected
  local agent_status
  agent_status=$(curl -s -u "${TD_USER}:${TD_PASS}" "http://${TAOSX_HOST}:${EXPLORER_PORT}/api/x/agents" | \
    python3 -c "import sys,json;data=json.load(sys.stdin);agents=data if isinstance(data,list) else data.get('data',[]);print(next((a.get('status','') for a in agents if a.get('status')=='connected'),'disconnected'))" 2>/dev/null)
  if [ "$agent_status" = "connected" ]; then
    info "Agent 状态: connected ✓"
  else
    warn "Agent 状态: ${agent_status}（等待连接...）"
    sleep 5
  fi
}

stop_agent() {
  if [ -n "$AGENT_PID" ] && kill -0 "$AGENT_PID" 2>/dev/null; then
    log "停止 taosx-agent..."
    kill -SIGTERM "$AGENT_PID" 2>/dev/null || true
    wait "$AGENT_PID" 2>/dev/null || true
    info "taosx-agent 已停止"
  fi
  AGENT_PID=""
}

# ======================== 清理函数 ========================
cleanup() {
  warn "清理中..."
  network_unblock 2>/dev/null || true
  stop_opcua_sim 2>/dev/null || true
  stop_agent 2>/dev/null || true
}
trap cleanup EXIT

# ======================== 数据流精确控制 ========================
# 等待 TDengine 出现数据（证明 pipeline 已就绪）
wait_for_data() {
  local db_name="$1"
  local stable="$2"
  local timeout_secs="${3:-60}"
  local elapsed=0
  log "等待 TDengine 出现数据 (超时=${timeout_secs}s)..."
  while [ $elapsed -lt $timeout_secs ]; do
    local cnt
    cnt=$(td_query_count "$db_name" "$stable" 2>/dev/null)
    if [ "${cnt:-0}" -gt 0 ]; then
      info "Pipeline 就绪：已有 ${cnt} 条数据"
      return 0
    fi
    sleep 3
    elapsed=$((elapsed + 3))
  done
  warn "超时：TDengine 中仍无数据"
  return 1
}

# 等待 TDengine 数据量稳定（管道已排空）
# persist_data_enable=true 时数据经过磁盘持久化队列，尾部延迟较大，
# 因此先等待 settle_secs 让 pipeline 充分 flush，再用稳定性检测确认。
wait_for_drain() {
  local db_name="$1"
  local stable="$2"
  local stable_secs="${3:-15}"
  local max_wait="${4:-120}"
  local settle_secs="${5:-20}"
  log "等待数据管道排空 (settle ${settle_secs}s + 稳定${stable_secs}s, 最长${max_wait}s)..."

  # 阶段 1：固定等待，让 persist queue 中残余数据通过 pipeline flush 到 TDengine
  log "  阶段1: 等待 pipeline flush (${settle_secs}s)..."
  sleep "$settle_secs"

  # 阶段 2：稳定性检测，连续 stable_secs 数据量无变化则判定排空
  local last_count=0
  local stable_since=0
  local elapsed=0
  local remaining=$((max_wait - settle_secs))
  if [ $remaining -le 0 ]; then
    remaining=30
  fi
  while [ $elapsed -lt $remaining ]; do
    local cnt
    cnt=$(td_query_count "$db_name" "$stable" 2>/dev/null)
    cnt="${cnt:-0}"
    if [ "$cnt" -eq "$last_count" ] && [ "$cnt" -gt 0 ]; then
      stable_since=$((stable_since + 3))
      if [ $stable_since -ge $stable_secs ]; then
        info "数据已稳定：${cnt} 条（持续 ${stable_secs}s 无变化）"
        return 0
      fi
    else
      stable_since=0
      last_count=$cnt
    fi
    sleep 3
    elapsed=$((elapsed + 3))
  done
  warn "排空超时，当前数据量: $(td_query_count "$db_name" "$stable" 2>/dev/null)"
}

# 触发 CSV 开始记录
signal_csv_start() {
  if [ -n "$CSV_START_FILE" ]; then
    touch "$CSV_START_FILE"
    info "CSV 记录信号已发送"
  fi
}

# ======================== 创建 OPC UA 任务 ========================
# 注意：此函数通过 stdout 返回 task_id，所有日志输出到 stderr
# 使用 from_json 格式（与 Explorer UI 一致）
create_opcua_task() {
  local case_id="$1"
  local persist_enable="$2"  # true 或 false
  local db_name="viega_$(echo "$case_id" | tr '[:upper:]' '[:lower:]')"
  local task_name="viega_${case_id}"
  local local_ip
  local_ip=$(get_local_ip)

  log "创建数据库 ${db_name}（先清理旧数据）..." >&2
  td_exec "DROP DATABASE IF EXISTS ${db_name}" "" >&2
  sleep 1
  td_exec "CREATE DATABASE ${db_name} PRECISION 'ms'" "" >&2

  # 获取 agent ID（via 字段）
  local agents_result
  agents_result=$(explorer_api GET "/api/x/agents" 2>/dev/null)
  local agent_id
  agent_id=$(echo "$agents_result" | python3 -c "
import sys, json
data = json.load(sys.stdin)
agents = data if isinstance(data, list) else data.get('data', [])
for a in agents:
    if a.get('status') == 'connected':
        print(a.get('id', ''))
        break
else:
    if agents:
        print(agents[0].get('id', ''))
" 2>/dev/null)

  if [ -z "$agent_id" ]; then
    fail "无法获取 agent ID，请确认 agent 已连接" >&2
    return 1
  fi
  log "Agent ID: ${agent_id}" >&2

  # 先删除同名旧任务
  local existing
  existing=$(explorer_api GET "/api/x/tasks" 2>/dev/null)
  local old_id
  old_id=$(echo "$existing" | python3 -c "
import sys, json
data = json.load(sys.stdin)
tasks = data if isinstance(data, list) else data.get('data', [])
for t in tasks:
    if t.get('name') == '${task_name}':
        print(t.get('id', ''))
        break
" 2>/dev/null)
  if [ -n "$old_id" ]; then
    log "删除旧任务 ${task_name} (ID=${old_id})..." >&2
    explorer_api POST "/api/x/tasks/${old_id}/stop" > /dev/null 2>&1 || true
    sleep 2
    explorer_api DELETE "/api/x/tasks/${old_id}" > /dev/null 2>&1 || true
    sleep 1
  fi

  # persist_data_enable 需要是 Python boolean (True/False)
  local persist_bool="False"
  [ "$persist_enable" = "true" ] && persist_bool="True"

  # 构建 payload — 使用 from_json 格式（与 Explorer UI 创建任务一致）
  log "创建 OPC UA 任务：${task_name}（persist=${persist_enable}）..." >&2
  local payload
  payload=$(python3 -c "
import json
task = {
    'from': '',
    'from_json': {
        'agent': ${agent_id},
        'type': 'opcua',
        'data': {
            'endpoint': '${local_ip}:${OPCUA_PORT}',
            'failover_endpoints': '',
            'security_mode': '',
            'security_policy': '',
            'certificate': '',
            'private_key': '',
            'connect_timeout': 10,
            'anonymous': '',
            'username': '',
            'password': '',
            'auth_certificate': '',
            'auth_private_key': '',
            'root': '',
            'namespaces': '',
            'opc_points_mode': 'all',
            'node_id_pattern': '',
            'browse_name_pattern': '',
            'super_table_expression': 'opc_{type}',
            'child_table_expression': 't_{ns}_{id#/_}',
            'value_col': 'val',
            'value_transform': '',
            'table_primary_key': 'original_ts',
            'table_primary_key_alias': 'ts',
            'custom_tags': 'VARCHAR(1024)::name::{id#/.};VARCHAR(1024)::BrowseName::{BrowseName};VARCHAR(1024)::DisplayName::{DisplayName};VARCHAR(1024)::Description::{Description};VARCHAR(1024)::Path::{Path}.Value',
            'csv_config_file': '',
            'authentication.currentTab': 'anonymous',
            'datasets.currentTab': 'select_all_points',
            'contains_bad': False,
            'collect_mode': 'subscribe',
            'interval': 10,
            'connection_options.request_timeout': 10,
            'collect_options.request_timeout': 10,
            'update_mode': 'none',
            'update_interval': 600,
            'log_level': 'info',
            'write_concurrency': 0,
            'batch_size': 1000,
            'batch_timeout': 1,
            'persist_data_enable': ${persist_bool},
            'keep_raw_data': False,
            'keep_raw_data_days': 1,
            'health_check_window_in_second': '0s',
            'busy_threshold': '100%',
            'max_queue_length': 1000,
            'max_errors_in_window': 10,
        }
    },
    'name': '${task_name}',
    'to': 'taos+http://${TD_USER}:${TD_PASS}@${TAOSX_HOST}:6041/${db_name}',
    'labels': ['type::datain', 'user::root'],
    'via': ${agent_id}
}
print(json.dumps(task))
")

  local create_result
  create_result=$(explorer_api POST "/api/x/tasks" "$payload")
  log "创建结果: $(echo "$create_result" | python3 -c "import sys,json;d=json.load(sys.stdin);print('name=%s id=%s status=%s' % (d.get('name','?'),d.get('id','?'),d.get('status','?')))" 2>/dev/null || echo "$create_result")" >&2

  # 提取 task ID — 仅此行写到 stdout 作为返回值
  echo "$create_result" | python3 -c "import sys,json;print(json.load(sys.stdin).get('id',''))" 2>/dev/null
}

stop_and_delete_task() {
  local task_id="$1"
  if [ -n "$task_id" ]; then
    log "停止并删除任务 ID=${task_id}..."
    explorer_api POST "/api/x/tasks/${task_id}/stop" > /dev/null 2>&1 || true
    sleep 3
    explorer_api DELETE "/api/x/tasks/${task_id}" > /dev/null 2>&1 || true
  fi
}

# ======================== 运行单个测试用例 ========================
run_test_case() {
  local case_id="$1"
  local persist_enable="$2"
  local disconnect_secs="$3"
  local case_dir="${RESULTS_DIR}/${case_id}"
  local db_name="viega_$(echo "$case_id" | tr '[:upper:]' '[:lower:]')"

  mkdir -p "$case_dir"

  echo ""
  log "============================================================"
  log "  测试用例 ${case_id}"
  log "    persist_data_enable = ${persist_enable}"
  log "    断链时长 = ${disconnect_secs}s"
  log "============================================================"

  # 记录测试参数
  cat > "${case_dir}/params.json" << EOF
{
  "case_id": "${case_id}",
  "persist_data_enable": ${persist_enable},
  "disconnect_seconds": ${disconnect_secs},
  "opcua_points": ${OPCUA_POINTS},
  "warmup_seconds": ${WARMUP_SECS},
  "cooldown_seconds": ${COOLDOWN_SECS},
  "start_time": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
}
EOF

  # 步骤 0：清理旧的 persist queue 数据，避免历史数据污染测试结果
  if [ -d "${AGENT_DATA_DIR}/tasks" ]; then
    log "清理旧 persist queue 数据..."
    rm -rf "${AGENT_DATA_DIR}/tasks"
    info "已清理 ${AGENT_DATA_DIR}/tasks"
  fi

  # 计算模拟器应生成的数据总时长（从 CSV 开始后）
  # warmup + disconnect + cooldown = 模拟器需要持续生成数据的时间
  local sim_duration=$((WARMUP_SECS + disconnect_secs + COOLDOWN_SECS))

  # 步骤 1：启动 OPC UA 模拟器（指定 stop-after，到时间后自动停止生成数据，但 server 保持运行）
  start_opcua_sim "$case_id" "$sim_duration"

  # 步骤 2：启动 Agent
  start_agent "$case_id"
  sleep 5  # 等待 agent 连接稳定

  # 步骤 3：创建并启动 OPC UA 任务
  local task_id
  task_id=$(create_opcua_task "$case_id" "$persist_enable")
  log "任务 ID: ${task_id}"
  echo "$task_id" > "${case_dir}/task_id.txt"

  # 步骤 4：等待 pipeline 就绪（TDengine 中出现数据）
  wait_for_data "$db_name" "opc_double" 90

  # 步骤 5：触发 CSV 开始记录（此时 pipeline 已通，CSV 内容=TDengine 应有的精确基准）
  signal_csv_start
  log "Warmup：等待基准数据稳定积累（${WARMUP_SECS}s）..."
  wait_secs "$WARMUP_SECS" "Warmup"

  # 记录断链前数据量
  local count_before
  count_before=$(td_query_count "$db_name" "opc_double")
  log "断链前 TDengine 数据量: ${count_before}"
  echo "$count_before" > "${case_dir}/count_before.txt"

  local disconnect_start
  disconnect_start=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  echo "$disconnect_start" > "${case_dir}/disconnect_start.txt"

  # 步骤 6：断开网络
  network_block
  wait_secs "$disconnect_secs" "断链中"

  local disconnect_end
  disconnect_end=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  echo "$disconnect_end" > "${case_dir}/disconnect_end.txt"

  # 步骤 6：恢复网络
  network_unblock
  wait_secs "$COOLDOWN_SECS" "Cooldown：等待任务恢复并写入数据"

  # 步骤 7：等待模拟器自动停止生成数据
  # 模拟器设置了 --stop-after，到达 sim_duration 后自动停止更新变量，
  # 但 OPC UA server 保持运行，taosx-opc 的订阅连接不会断开。
  # CSV 在模拟器停止生成时自然定格，是精确基准。
  log "等待模拟器完成数据生成..."
  # 检测模拟器日志中的 stop 标记
  local sim_log="${RESULTS_DIR}/${case_id}/opcua-sim.log"
  local wait_sim=0
  local max_wait_sim=$((sim_duration + 60))
  while [ $wait_sim -lt $max_wait_sim ]; do
    if grep -q "\[stop\]" "$sim_log" 2>/dev/null; then
      info "模拟器已停止数据生成"
      break
    fi
    sleep 3
    wait_sim=$((wait_sim + 3))
  done
  if [ $wait_sim -ge $max_wait_sim ]; then
    warn "等待模拟器停止超时"
  fi

  # 步骤 8：等待管道排空（模拟器已停止生成，等待残余数据经 persist queue flush 到 TDengine）
  # settle=20s 让 persist queue 充分 flush，然后 stable=15s 确认无新增，最长等 120s
  wait_for_drain "$db_name" "opc_double" 15 120 20

  # 步骤 9：停止模拟器进程（数据已全部 flush，可以安全关闭）
  stop_opcua_sim

  # 步骤 10：记录恢复后数据量
  local count_after
  count_after=$(td_query_count "$db_name" "opc_double")
  log "恢复后 TDengine 数据量: ${count_after}"
  echo "$count_after" > "${case_dir}/count_after.txt"

  # 步骤 11：停止任务和 agent
  stop_and_delete_task "$task_id"
  stop_agent

  # 收集 agent 日志（agent 自己写到配置的日志目录）
  log "收集 agent 日志..."
  cp "${AGENT_LOG_DIR}"/taosxagent.log "${case_dir}/agent.log" 2>/dev/null || true

  # 步骤 11：数据完整性验证
  log "验证数据完整性..."
  local csv_file
  csv_file=$(ls -t "${case_dir}/opcua-logs/"*.csv 2>/dev/null | head -1)
  if [ -n "$csv_file" ]; then
    cd "$OPCUA_SIM_DIR"
    node "$VERIFY_SCRIPT" \
      --csv "$csv_file" \
      --host "$TAOSX_HOST" \
      --port "$TDENGINE_REST_PORT" \
      --db "$db_name" \
      --stable "opc_double" \
      2>&1 | tee "${case_dir}/verify-result.txt" || true
    cd - > /dev/null
  else
    warn "未找到 CSV 日志文件"
  fi

  # 保存 agent 日志中的关键事件
  log "提取 agent 日志关键事件..."
  grep -i "cancel\|disconnect\|reconnect\|connect.*success\|error\|exit\|task" \
    "${case_dir}/agent.log" > "${case_dir}/agent-events.txt" 2>/dev/null || true

  # 记录完成时间
  echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" > "${case_dir}/end_time.txt"

  # 输出用例摘要
  echo ""
  info "===== ${case_id} 测试完成 ====="
  log "  persist_data_enable = ${persist_enable}"
  log "  断链时长            = ${disconnect_secs}s"
  log "  断链前数据量        = ${count_before}"
  log "  恢复后数据量        = ${count_after}"
  log "  CSV 日志            = ${csv_file:-N/A}"
  log "  结果目录            = ${case_dir}"
  echo ""
}

# ======================== 生成汇总报告 ========================
generate_summary() {
  local summary_file="${RESULTS_DIR}/summary.md"
  log "生成汇总报告: ${summary_file}"

  cat > "$summary_file" << 'HEADER'
# Viega PoC 测试结果汇总

| 用例 | persist_data | 断链时长 | 断链前数据量 | 恢复后数据量 | 验证结果 |
|------|-------------|---------|------------|------------|---------|
HEADER

  for case_dir in "${RESULTS_DIR}"/T*/; do
    [ -d "$case_dir" ] || continue
    local case_id
    case_id=$(basename "$case_dir")
    local params="${case_dir}/params.json"
    [ -f "$params" ] || continue

    local persist disconnect count_before count_after verify
    persist=$(python3 -c "import json;print(json.load(open('$params'))['persist_data_enable'])" 2>/dev/null)
    disconnect=$(python3 -c "import json;print(json.load(open('$params'))['disconnect_seconds'])" 2>/dev/null)
    count_before=$(cat "${case_dir}/count_before.txt" 2>/dev/null || echo "?")
    count_after=$(cat "${case_dir}/count_after.txt" 2>/dev/null || echo "?")

    if [ -f "${case_dir}/verify-result.txt" ]; then
      verify=$(grep "完整率" "${case_dir}/verify-result.txt" | head -1 | sed 's/.*完整率：//' || echo "?")
    else
      verify="未验证"
    fi

    echo "| ${case_id} | ${persist} | ${disconnect}s | ${count_before} | ${count_after} | ${verify} |" >> "$summary_file"
  done

  echo "" >> "$summary_file"
  echo "生成时间: $(date '+%Y-%m-%d %H:%M:%S')" >> "$summary_file"
  echo "测试环境: taosx@${TAOSX_HOST}, agent@$(get_local_ip), ${OPCUA_POINTS} 点位" >> "$summary_file"

  cat "$summary_file"
}

# ======================== 主入口 ========================
main() {
  log "============================================================"
  log "  Viega PoC 自动化测试"
  log "  结果目录: ${RESULTS_DIR}"
  log "  OPC UA 点位: ${OPCUA_POINTS}"
  log "  目标测试: ${TEST_CASE}"
  log "============================================================"

  mkdir -p "$RESULTS_DIR"

  # 确保网络初始正常
  network_unblock 2>/dev/null || true

  # 检查前置条件
  log "检查 Explorer 连通性..."
  if ! curl -s -o /dev/null -w "%{http_code}" "http://${TAOSX_HOST}:${EXPLORER_PORT}" | grep -q "200\|302"; then
    fail "Explorer 不可达: http://${TAOSX_HOST}:${EXPLORER_PORT}"
    exit 1
  fi
  info "Explorer 可达"

  log "检查 TDengine REST 连通性..."
  if ! td_exec "SELECT server_version()" "" | grep -q "data"; then
    fail "TDengine REST 不可达"
    exit 1
  fi
  info "TDengine 可达"

  # 运行测试用例
  case "$TEST_CASE" in
    T1)
      run_test_case "T1" false 300
      ;;
    T2)
      run_test_case "T2" true 30
      ;;
    T3)
      run_test_case "T3" true 300
      ;;
    T4)
      run_test_case "T4" true 1800
      ;;
    ALL)
      run_test_case "T1" false 300
      run_test_case "T2" true 30
      run_test_case "T3" true 300
      run_test_case "T4" true 1800
      ;;
    *)
      fail "未知测试用例: ${TEST_CASE}，可选: T1, T2, T3, T4, ALL"
      exit 1
      ;;
  esac

  # 汇总
  generate_summary
  echo ""
  info "全部测试完成！结果保存在: ${RESULTS_DIR}"
}

main
