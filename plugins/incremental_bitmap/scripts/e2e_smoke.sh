#!/usr/bin/env bash
set -euo pipefail

# e2e_smoke.sh: 本地联调脚本
# - REAL_TDENGINE=1 时走真实 TMQ + taosdump
# - 否则默认走 USE_MOCK=1 回退路径，仅验证核心流程

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 仓库根目录: 从 plugins/incremental_bitmap/scripts 上跳三级
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
# 插件目录: 从 scripts 上跳一级
PLUGIN_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$PLUGIN_DIR/build"
BIN_TEST="$BUILD_DIR/test_tmq_integration"

# 环境准备
export LD_LIBRARY_PATH="$ROOT_DIR/build/lib:${LD_LIBRARY_PATH:-}"
export PATH="$ROOT_DIR/build/bin:${PATH:-}"

REAL_MODE="${REAL_TDENGINE:-0}"
if [[ "$REAL_MODE" == "1" ]]; then
  echo "[E2E] 使用真实联调模式（TMQ + taosdump）"
  # 确保不使用 mock 模式
  unset USE_MOCK
else
  export USE_MOCK=1
  echo "[E2E] 使用回退模式（USE_MOCK=1）。若要真实联调，设置 REAL_TDENGINE=1"
fi

# 构建测试目标
echo "[E2E] 构建 test_tmq_integration ..."
if [[ "${USE_MOCK:-}" == "1" ]]; then
  cmake -S "$PLUGIN_DIR" -B "$BUILD_DIR" -DBUILD_TESTING=ON -DUSE_MOCK=ON >/dev/null
else
  cmake -S "$PLUGIN_DIR" -B "$BUILD_DIR" -DBUILD_TESTING=ON >/dev/null
fi
cmake --build "$BUILD_DIR" --target test_tmq_integration -j >/dev/null

# 若真实模式，准备数据库/主题/数据
DB_NAME="test"
TOPIC_NAME="incremental_backup_topic"
if [[ "$REAL_MODE" == "1" ]]; then
  echo "[E2E] 准备 TDengine 数据库与主题..."
  taos -s "CREATE DATABASE IF NOT EXISTS ${DB_NAME};
           USE ${DB_NAME};
           CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, v INT) TAGS (t INT);
           CREATE TABLE IF NOT EXISTS d0 USING meters TAGS (1);
           INSERT INTO d0 VALUES (now, 1);
           CREATE TOPIC IF NOT EXISTS ${TOPIC_NAME} AS DATABASE ${DB_NAME};" >/dev/null || {
    echo "[E2E] taos 初始化失败，请确认 TDengine 已启动并可用" >&2
    exit 1
  }
fi

# 运行集成测试
echo "[E2E] 运行 test_tmq_integration ..."
"$BIN_TEST"

# 若真实模式，执行 taosdump 导出并校验
if [[ "$REAL_MODE" == "1" ]]; then
  OUT_DIR="$BUILD_DIR/e2e_dump_out"
  rm -rf "$OUT_DIR" && mkdir -p "$OUT_DIR"
  echo "[E2E] 运行 taosdump 导出数据库 ${DB_NAME} 到 $OUT_DIR ..."
  taosdump -D "$DB_NAME" -o "$OUT_DIR" >/dev/null
  echo "[E2E] 校验导出结果..."
  # 检查多种可能的导出文件格式
  AVRO_COUNT=$(find "$OUT_DIR" -type f -name "*.avro*" | wc -l || true)
  SQL_COUNT=$(find "$OUT_DIR" -type f -name "*.sql" | wc -l || true)
  CSV_COUNT=$(find "$OUT_DIR" -type f -name "*.csv" | wc -l || true)
  
  TOTAL_FILES=$((AVRO_COUNT + SQL_COUNT + CSV_COUNT))
  if [[ "$TOTAL_FILES" -lt 1 ]]; then
    echo "[E2E] 导出校验失败：未发现任何导出文件" >&2
    echo "[E2E] 检查到的文件：AVRO=$AVRO_COUNT, SQL=$SQL_COUNT, CSV=$CSV_COUNT" >&2
    exit 1
  fi
  echo "[E2E] 导出校验通过：发现 $TOTAL_FILES 个文件 (AVRO=$AVRO_COUNT, SQL=$SQL_COUNT, CSV=$CSV_COUNT)"
else
  echo "[E2E] 跳过 taosdump 导出校验（回退模式）"
fi

echo "[E2E] 本地联调完成"


