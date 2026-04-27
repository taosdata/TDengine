#!/bin/bash

# Usage: ./riscvtest.sh [-t <seconds>]
#   -t <seconds>  Per-case timeout in seconds (default: 300)
#   Env var CASE_TIMEOUT is also accepted as a fallback.

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG="$BASE_DIR/riscvtest.log"
CASE_TIMEOUT=${CASE_TIMEOUT:-300}
TAOS_BIN_DIR="${TAOS_BIN_DIR:-/media/eswin/sata/tsdb/release/build/bin}"
TAOS_CFG_EXTRA="${TAOS_CFG_EXTRA:-}"

while getopts "t:b:c:s:" opt; do
  case $opt in
    t) CASE_TIMEOUT="$OPTARG" ;;
    b) TAOS_BIN_DIR="$OPTARG" ;;
    c) TAOS_CFG_EXTRA="$OPTARG" ;;
    s) START_FROM="$OPTARG" ;;
    *) echo "Usage: $0 [-t <timeout>] [-b <bin_dir>] [-c <extra_cfg_file>]" >&2; exit 1 ;;
  esac
done
export TAOS_BIN_DIR TAOS_CFG_EXTRA
START_FROM="${START_FROM:-}"

# Validate -b / -c if supplied
if [ -n "$TAOS_BIN_DIR" ]; then
  if [ ! -f "$TAOS_BIN_DIR/taosd" ]; then
    echo "ERROR: -b '$TAOS_BIN_DIR' does not contain 'taosd'" >&2; exit 1
  fi
  TAOS_BIN_DIR="$(cd "$TAOS_BIN_DIR" && pwd)"   # normalize to realpath
  export TAOS_BIN_DIR
fi
if [ -n "$TAOS_CFG_EXTRA" ]; then
  if [ ! -r "$TAOS_CFG_EXTRA" ]; then
    echo "ERROR: -c '$TAOS_CFG_EXTRA' is not readable" >&2; exit 1
  fi
  TAOS_CFG_EXTRA="$(cd "$(dirname "$TAOS_CFG_EXTRA")" && pwd)/$(basename "$TAOS_CFG_EXTRA")"
  export TAOS_CFG_EXTRA
fi

source "$BASE_DIR/venv/bin/activate"

echo "=== riscvtest start at $(date '+%Y-%m-%d %H:%M:%S') ===" | tee -a "$LOG"
echo "Python: $(python3 --version 2>&1)" | tee -a "$LOG"
echo "Case timeout: ${CASE_TIMEOUT}s" | tee -a "$LOG"
echo "Binary dir:   ${TAOS_BIN_DIR:-(auto-detect)}" | tee -a "$LOG"
echo "Extra cfg:    ${TAOS_CFG_EXTRA:-(none)}" | tee -a "$LOG"
echo "" | tee -a "$LOG"

PASS=0
FAIL=0
TIMEOUT_COUNT=0
TOTAL=0

run_case() {
  local subfolder="$1"
  shift
  local cmd="$@"
  # -s start-from: skip until matching case found
  if [ -n "$START_FROM" ]; then
    if echo "$cmd" | grep -qF "$START_FROM"; then
      START_FROM=""   # found, stop skipping
    else
      return 0        # skip this case
    fi
  fi
  TOTAL=$((TOTAL + 1))
  local start_ts=$(date '+%Y-%m-%d %H:%M:%S')
  echo "[START][$start_ts] $subfolder: $cmd" | tee -a "$LOG"
  (
    cd "$BASE_DIR/$subfolder" && timeout "$CASE_TIMEOUT" bash -c "$cmd"
  ) >> "$LOG" 2>&1
  local rc=$?
  local end_ts=$(date '+%Y-%m-%d %H:%M:%S')
  if [ $rc -eq 0 ]; then
    PASS=$((PASS + 1))
    echo "[PASS ][$end_ts] $subfolder: $cmd" | tee -a "$LOG"
  elif [ $rc -eq 124 ]; then
    TIMEOUT_COUNT=$((TIMEOUT_COUNT + 1))
    FAIL=$((FAIL + 1))
    echo "[TIMEOUT][$end_ts] >${CASE_TIMEOUT}s $subfolder: $cmd" | tee -a "$LOG"
  else
    FAIL=$((FAIL + 1))
    echo "[FAIL ][$end_ts] rc=$rc $subfolder: $cmd" | tee -a "$LOG"
  fi
  echo "" | tee -a "$LOG"
}

run_case system-test python3 ./test.py -f 0-others/compatibility_rolling_upgrade.py -N 3
run_case army python3 ./test.py -f multi-level/mlevel_basic.py -N 3 -L 3 -D 2
run_case army python3 ./test.py -f db-encrypt/basic.py -N 3 -M 3
run_case army python3 ./test.py -f cluster/arbitrator.py -N 3
run_case army python3 ./test.py -f cluster/arbitrator_restart.py -N 3
run_case army python3 ./test.py -f storage/s3/s3Basic.py -N 3
run_case army python3 ./test.py -f cluster/snapshot.py -N 3 -L 3 -D 2
run_case army python3 ./test.py -f vtable/test_vtable_create.py
run_case army python3 ./test.py -f vtable/test_vtable_alter.py
run_case army python3 ./test.py -f vtable/test_vtable_drop.py
run_case army python3 ./test.py -f vtable/test_vtable_meta.py
run_case army python3 ./test.py -f vtable/test_vtable_auth_create.py
run_case army python3 ./test.py -f vtable/test_vtable_auth_select.py
run_case army python3 ./test.py -f vtable/test_vtable_auth_alter_drop.py
run_case army python3 ./test.py -f vtable/test_vtable_auth_alter_drop_child.py
run_case army python3 ./test.py -f vtable/test_vtable_query.py
run_case army python3 ./test.py -f vtable/test_vtable_query_same_db_stb.py
run_case army python3 ./test.py -f vtable/test_vtable_query_same_db_stb_window.py
run_case army python3 ./test.py -f vtable/test_vtable_query_same_db_stb_group.py
run_case army python3 ./test.py -f vtable/test_vtable_query_same_reference_col.py
run_case army python3 ./test.py -f vtable/test_vtable_query_cross_db.py
run_case army python3 ./test.py -f vtable/test_vtable_query_cross_db_stb.py
run_case army python3 ./test.py -f vtable/test_vtable_query_cross_db_stb_window.py
run_case army python3 ./test.py -f vtable/test_vtable_query_cross_db_stb_group.py
run_case army python3 ./test.py -f vtable/test_vtable_query_after_alter.py
run_case army python3 ./test.py -f vtable/test_vtable_query_after_drop_origin_table.py
run_case army python3 ./test.py -f vtable/test_vtable_query_after_alter_origin_table.py
run_case army python3 ./test.py -f vtable/test_vtable_schema_is_old.py
run_case army python3 ./test.py -f vtable/test_vtable_join.py
run_case army python3 ./test.py -f query/decimal/test_TS6333.py
run_case army python3 ./test.py -f query/function/test_func_elapsed.py
run_case army python3 ./test.py -f query/function/test_function.py
run_case army python3 ./test.py -f query/function/test_selection_function_with_json.py
run_case army python3 ./test.py -f query/function/test_func_paramnum.py
run_case army python3 ./test.py -f query/function/test_percentile.py
run_case army python3 ./test.py -f query/function/test_resinfo.py
run_case army python3 ./test.py -f query/function/test_interp.py
run_case army python3 ./test.py -f query/function/test_interval.py
run_case army python3 ./test.py -f query/function/test_interval_diff_tz.py
run_case army python3 ./test.py -f query/function/concat.py
run_case army python3 ./test.py -f query/function/cast.py
run_case army python3 ./test.py -f query/test_join.py
run_case army python3 ./test.py -f query/test_join_const.py
run_case army python3 ./test.py -f query/test_compare.py
run_case army python3 ./test.py -f query/test_case_when.py
run_case army python3 ./test.py -f insert/test_column_tag_boundary.py
run_case army python3 ./test.py -f query/fill/fill_desc.py -N 3 -L 3 -D 2
run_case army python3 ./test.py -f query/fill/fill_null.py
run_case army python3 ./test.py -f cluster/test_drop_table_by_uid.py -N 3
run_case army python3 ./test.py -f cluster/incSnapshot.py -N 3
run_case army python3 ./test.py -f cluster/clusterBasic.py -N 5
run_case army python3 ./test.py -f cluster/tsdbSnapshot.py -N 3 -M 3
run_case army python3 ./test.py -f cluster/strongPassword.py
run_case army python3 ./test.py -f query/query_basic.py -N 3
run_case army python3 ./test.py -f query/accuracy/test_query_accuracy.py
run_case army python3 ./test.py -f query/accuracy/test_ts5400.py
run_case army python3 ./test.py -f query/accuracy/test_having.py
run_case army python3 ./test.py -f insert/insert_basic.py -N 3
run_case army python3 ./test.py -f insert/auto_create_insert.py
run_case army python3 ./test.py -f cluster/splitVgroupByLearner.py -N 3
run_case army python3 ./test.py -f authorith/authBasic.py -N 3
run_case army python3 ./test.py -f cmdline/fullopt.py
run_case army python3 ./test.py -f query/show.py -N 3
run_case army python3 ./test.py -f alter/alterConfig.py -N 3
run_case army python3 ./test.py -f alter/test_alter_config.py -N 3
run_case army python3 ./test.py -f alter/test_alter_config.py -N 3 -M 3
run_case army python3 ./test.py -f alter/alter_db_option.py -N 3
run_case army python3 ./test.py -f query/subquery/subqueryBugs.py -N 3
run_case army python3 ./test.py -f storage/oneStageComp.py -N 3 -L 3 -D 1
run_case army python3 ./test.py -f storage/compressBasic.py -N 3
run_case army python3 ./test.py -f grant/grantBugs.py -N 3
run_case army python3 ./test.py -f query/queryBugs.py -N 3
run_case army python3 ./test.py -f user/test_passwd.py
run_case army python3 ./test.py -f tmq/tmqBugs.py -N 3
run_case army python3 ./test.py -f query/fill/fill_compare_asc_desc.py
run_case army python3 ./test.py -f query/last/test_last.py
run_case army python3 ./test.py -f query/window/base.py
run_case army python3 ./test.py -f query/sys/tb_perf_queries_exist_test.py -N 3
run_case army python3 ./test.py -f query/test_having.py
run_case army python3 ./test.py -f tmq/drop_lost_comsumers.py
run_case army python3 ./test.py -f cmdline/taosCli.py -B
run_case army python3 ./test.py -f whole/checkErrorCode.py
run_case army python3 ./test.py -f create/create_stb_keep.py
run_case army python3 ./test.py -f create/create_stb_keep.py -N 3
run_case army python3 ./test.py -f create/test_stb_keep_compact.py
run_case army python3 ./test.py -f create/test_stb_keep_compact.py -N 3
run_case army python3 ./test.py -f create/test_stb_keep_compact.py -N 3 -M 3
run_case army python3 ./test.py -f create/create_ctb_using_csv_file.py
run_case army python3 ./test.py -f create/create_ctb_using_csv_file.py -N 3
run_case army python3 ./test.py -f insert/insert_csv_file_with_row_split.py
# [AUTO-DISABLED 2026-04-19] max retries (3) exceeded
# # [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case army python3 ./test.py -f stream/test_stream_vtable.py
run_case army python3 ./test.py -f inspect-tools/taosinspect.py
run_case army python3 ./test.py -f tools/benchmark/basic/commandline.py
run_case army python3 ./test.py -f tools/benchmark/basic/commandline-partial-col-numpy.py
run_case army python3 ./test.py -f tools/benchmark/basic/commandline-single-table.py
run_case army python3 ./test.py -f tools/benchmark/basic/commandline-sml-rest.py -R
run_case army python3 ./test.py -f tools/benchmark/basic/commandline-supplement-insert.py
run_case army python3 ./test.py -f tools/benchmark/basic/commandline-vgroups.py
run_case army python3 ./test.py -f tools/benchmark/basic/connMode.py -B
run_case army python3 ./test.py -f tools/benchmark/basic/custom_col_tag.py
run_case army python3 ./test.py -f tools/benchmark/basic/default_json.py
run_case army python3 ./test.py -f tools/benchmark/basic/demo.py
run_case army python3 ./test.py -f tools/benchmark/basic/csv-export.py
run_case army python3 ./test.py -f tools/benchmark/basic/insert-decimal.py
run_case army python3 ./test.py -f tools/benchmark/basic/from-to.py
run_case army python3 ./test.py -f tools/benchmark/basic/from-to-continue.py
run_case army python3 ./test.py -f tools/benchmark/basic/create_table_keywords.py
run_case army python3 ./test.py -f tools/benchmark/basic/json_tag.py
run_case army python3 ./test.py -f tools/benchmark/basic/insert_tag_order_sml.py
run_case army python3 ./test.py -f tools/benchmark/basic/insert_tag_order_sql.py
run_case army python3 ./test.py -f tools/benchmark/basic/insert_tag_order_stmt.py
run_case army python3 ./test.py -f tools/benchmark/basic/insert_tag_order_stmt2.py
run_case army python3 ./test.py -f tools/benchmark/basic/insert-json-csv.py
run_case army python3 ./test.py -f tools/benchmark/basic/insertBasic.py
run_case army python3 ./test.py -f tools/benchmark/basic/insertBindVGroup.py
run_case army python3 ./test.py -f tools/benchmark/basic/insertMix.py
run_case army python3 ./test.py -f tools/benchmark/basic/insertPrecision.py
run_case army python3 ./test.py -f tools/benchmark/basic/invalid_commandline.py
run_case army python3 ./test.py -f tools/benchmark/basic/query_json.py -B
run_case army python3 ./test.py -f tools/benchmark/basic/query_json-with-error-sqlfile.py
run_case army python3 ./test.py -f tools/benchmark/basic/query_json-with-sqlfile.py
run_case army python3 ./test.py -f tools/benchmark/basic/queryMain.py
run_case army python3 ./test.py -f tools/benchmark/basic/rest_insert_alltypes_json.py -R
run_case army python3 ./test.py -f tools/benchmark/basic/reuse-exist-stb.py
run_case army python3 ./test.py -f tools/benchmark/basic/sml_auto_create_table_json.py
run_case army python3 ./test.py -f tools/benchmark/basic/sml_json_alltypes.py
run_case army python3 ./test.py -f tools/benchmark/basic/sml_json_insert_alltypes-same-min-max.py
run_case army python3 ./test.py -f tools/benchmark/basic/sml_taosjson_alltypes.py
run_case army python3 ./test.py -f tools/benchmark/basic/sml_telnet_alltypes.py
run_case army python3 ./test.py -f tools/benchmark/basic/sml_telnet_insert_alltypes-same-min-max.py
run_case army python3 ./test.py -f tools/benchmark/basic/stmt_auto_create_table_json.py
run_case army python3 ./test.py -f tools/benchmark/basic/stmt_insert_alltypes_json.py
run_case army python3 ./test.py -f tools/benchmark/basic/stmt_insert_alltypes-same-min-max.py
run_case army python3 ./test.py -f tools/benchmark/basic/stmt_offset_json.py
run_case army python3 ./test.py -f tools/benchmark/basic/stmt_sample_csv_json_doesnt_use_ts.py
run_case army python3 ./test.py -f tools/benchmark/basic/stmt_sample_csv_json-subtable.py
run_case army python3 ./test.py -f tools/benchmark/basic/stmt_sample_csv_json.py
run_case army python3 ./test.py -f tools/benchmark/basic/stmt2_insert.py
run_case army python3 ./test.py -f tools/benchmark/basic/stream-test.py
run_case army python3 ./test.py -f tools/benchmark/basic/stream_function_test.py
run_case army python3 ./test.py -f tools/benchmark/basic/stt.py
run_case army python3 ./test.py -f tools/benchmark/basic/taosc_auto_create_table_json.py
run_case army python3 ./test.py -f tools/benchmark/basic/taosc_sample_csv_json-subtable.py
run_case army python3 ./test.py -f tools/benchmark/basic/taosc_insert_alltypes-same-min-max.py
run_case army python3 ./test.py -f tools/benchmark/basic/taosc_insert_alltypes_json.py
run_case army python3 ./test.py -f tools/benchmark/basic/taosc_insert_alltypes_json-partial-col.py
run_case army python3 ./test.py -f tools/benchmark/basic/taosc_insert-table-creating-interval.py
run_case army python3 ./test.py -f tools/benchmark/basic/taosc_sample_csv_json.py
run_case army python3 ./test.py -f tools/benchmark/basic/taosdemoTestInsertWithJsonStmt-otherPara.py
run_case army python3 ./test.py -f tools/benchmark/basic/taosdemoTestQueryWithJson.py -R
run_case army python3 ./test.py -f tools/benchmark/basic/telnet_tcp.py -R
run_case army python3 ./test.py -f tools/benchmark/basic/tmqBasic.py
run_case army python3 ./test.py -f tools/benchmark/basic/tmq_case.py
run_case army python3 ./test.py -f tools/benchmark/basic/websiteCase.py
run_case army python3 ./test.py -f tools/benchmark/cloud/cloud-test.py
run_case army python3 ./test.py -f tools/benchmark/ws/websocket.py -R
run_case army python3 ./test.py -f tools/benchmark/basic/bugs.py -B
run_case army python3 ./test.py -f tools/benchmark/basic/commandline-retry.py
run_case army python3 ./test.py -f tools/benchmark/basic/commandline-sml.py
run_case army python3 ./test.py -f tools/benchmark/basic/sml_json_alltypes-interlace.py
run_case army python3 ./test.py -f tools/benchmark/basic/sml_insert_alltypes_json.py
run_case army python3 ./test.py -f tools/benchmark/basic/sml_interlace.py
run_case army python3 ./test.py -f tools/benchmark/basic/sml_taosjson_insert_alltypes-same-min-max.py
run_case army python3 ./test.py -f tools/benchmark/basic/taosadapter_json.py -B
run_case army python3 ./test.py -f tools/benchmark/basic/taosc_insert-mix.py
run_case army python3 ./test.py -f tools/benchmark/basic/taosc_insert-retry-json-global.py
run_case army python3 ./test.py -f tools/benchmark/basic/taosc_insert-retry-json-stb.py
run_case army python3 ./test.py -f tools/benchmark/basic/taosdemoTestQueryWithJson-mixed-query.py -R
run_case army python3 ./test.py -f tools/benchmark/basic/create_table_from_csv.py
run_case army python3 ./test.py -f tools/benchmark/basic/insert_cancle.py
run_case army python3 ./test.py -f tools/benchmark/basic/insert_error_exit.py
run_case army python3 ./test.py -f tools/benchmark/basic/tmq_cancle.py
run_case army python3 ./test.py -f tools/benchmark/basic/taos_config_json.py
run_case army python3 ./test.py -f tools/benchmark/basic/insert_data.py
run_case army python3 ./test.py -f tools/benchmark/basic/insert_auto_create_table_json.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpSchemaChange.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpCompa.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTest.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpDbStb.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestTypeDouble.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestTypeUnsignedBigInt.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpManyCols.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpStartEndTime.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTypeVarbinary.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTypeGeometry.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpDbWithNonRoot.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpEscapedDb.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestTypeJson.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestBasic.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestTypeUnsignedSmallInt.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpDbNtb.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestTypeUnsignedTinyInt.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestTypeUnsignedInt.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestTypeSmallInt.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestTypeInt.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestNanoSupport.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestTypeBigInt.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestTypeBinary.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestTypeFloat.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpStartEndTimeLong.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestLooseMode.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestTypeBool.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestInspect.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpInDiffType.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTest2.py
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpTestTypeTinyInt.py
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpTestTypeDouble.py -B
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpTestTypeUnsignedBigInt.py -B
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpEscapedDb.py -B
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpPrimaryKey.py -B
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpTestTypeJson.py -B
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpTestTypeUnsignedSmallInt.py -B
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpTestTypeUnsignedTinyInt.py -B
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpTestTypeUnsignedInt.py -B
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpTestTypeSmallInt.py -B
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpTestTypeInt.py -B
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpTestTypeBigInt.py -B
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpTestTypeBinary.py -B
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpTestTypeFloat.py -B
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpTestTypeBool.py -B
# [AUTO-DISABLED 2026-04-19] max retries (3) exceeded
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpRetry.py -B
run_case army python3 ./test.py -f tools/taosdump/ws/taosdumpTestTypeTinyInt.py -B
run_case army python3 ./test.py -f tools/taosdump/native/taosdumpCommandline.py -B
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/stream_multi_agg.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/stream_basic.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/scalar_function.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/at_once_interval.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/at_once_session.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/at_once_state_window.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/window_close_interval.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/window_close_session.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/window_close_state_window.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/max_delay_interval.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/max_delay_session.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/at_once_interval_ext.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/max_delay_interval_ext.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/window_close_session_ext.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/partition_interval.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/state_window_case.py
# [AUTO-DISABLED 2026-04-19] time limit (15 min) exceeded
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/snode_restart_with_checkpoint.py -N 4
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/force_window_close_interp.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/force_window_close_interval.py
run_case system-test python3 ./test.py -f 2-query/pk_error.py
run_case system-test python3 ./test.py -f 2-query/pk_func.py
run_case system-test python3 ./test.py -f 2-query/pk_varchar.py
run_case system-test python3 ./test.py -f 2-query/pk_func_group.py
run_case system-test python3 ./test.py -f 2-query/partition_expr.py
run_case system-test python3 ./test.py -f 2-query/project_group.py
run_case system-test python3 ./test.py -f 2-query/tbname_vgroup.py
run_case system-test python3 ./test.py -f 2-query/count_interval.py
run_case system-test python3 ./test.py -f 2-query/compact-col.py
run_case system-test python3 ./test.py -f 2-query/tms_memleak.py
run_case system-test python3 ./test.py -f 2-query/stbJoin.py
run_case system-test python3 ./test.py -f 2-query/stbJoin.py -Q 2
run_case system-test python3 ./test.py -f 2-query/stbJoin.py -Q 3
run_case system-test python3 ./test.py -f 2-query/stbJoin.py -Q 4
run_case system-test python3 ./test.py -f 2-query/hint.py
run_case system-test python3 ./test.py -f 2-query/hint.py -Q 2
run_case system-test python3 ./test.py -f 2-query/hint.py -Q 3
run_case system-test python3 ./test.py -f 2-query/hint.py -Q 4
run_case system-test python3 ./test.py -f 2-query/para_tms.py
run_case system-test python3 ./test.py -f 2-query/para_tms2.py
run_case system-test python3 ./test.py -f 2-query/nestedQuery.py
run_case system-test python3 ./test.py -f 2-query/nestedQuery_str.py
run_case system-test python3 ./test.py -f 2-query/nestedQuery_math.py
run_case system-test python3 ./test.py -f 2-query/nestedQuery_time.py
run_case system-test python3 ./test.py -f 2-query/nestedQuery_26.py
run_case system-test python3 ./test.py -f 2-query/nestedQuery_str.py -Q 2
run_case system-test python3 ./test.py -f 2-query/nestedQuery_math.py -Q 2
run_case system-test python3 ./test.py -f 2-query/nestedQuery_time.py -Q 2
run_case system-test python3 ./test.py -f 2-query/nestedQuery.py -Q 2
run_case system-test python3 ./test.py -f 2-query/nestedQuery_26.py -Q 2
run_case system-test python3 ./test.py -f 2-query/columnLenUpdated.py
run_case system-test python3 ./test.py -f 2-query/columnLenUpdated.py -Q 2
run_case system-test python3 ./test.py -f 2-query/columnLenUpdated.py -Q 3
run_case system-test python3 ./test.py -f 2-query/columnLenUpdated.py -Q 4
run_case system-test python3 ./test.py -f 2-query/nestedQuery.py -Q 4
run_case system-test python3 ./test.py -f 2-query/nestedQuery_str.py -Q 4
run_case system-test python3 ./test.py -f 2-query/nestedQuery_math.py -Q 4
run_case system-test python3 ./test.py -f 2-query/nestedQuery_time.py -Q 4
run_case system-test python3 ./test.py -f 2-query/nestedQuery_26.py -Q 4
run_case system-test python3 ./test.py -f 2-query/interval_limit_opt.py -Q 4
run_case system-test python3 ./test.py -f 2-query/interval_unit.py
run_case system-test python3 ./test.py -f 2-query/interval_unit.py -Q 2
run_case system-test python3 ./test.py -f 2-query/interval_unit.py -Q 3
run_case system-test python3 ./test.py -f 2-query/interval_unit.py -Q 4
run_case system-test python3 ./test.py -f 2-query/partition_by_col.py -Q 4
run_case system-test python3 ./test.py -f 2-query/partition_by_col.py -Q 3
run_case system-test python3 ./test.py -f 2-query/partition_by_col.py -Q 2
run_case system-test python3 ./test.py -f 2-query/partition_by_col.py
run_case system-test python3 ./test.py -f 2-query/partition_by_col_agg.py
run_case system-test python3 ./test.py -f 2-query/partition_by_col_agg.py -Q 2
run_case system-test python3 ./test.py -f 2-query/partition_by_col_agg.py -Q 3
run_case system-test python3 ./test.py -f 2-query/partition_by_col_agg.py -Q 4
run_case system-test python3 ./test.py -f 2-query/interval_limit_opt_2.py -Q 4
run_case system-test python3 ./test.py -f 2-query/interval_limit_opt_2.py -Q 3
run_case system-test python3 ./test.py -f 2-query/interval_limit_opt_2.py -Q 2
run_case system-test python3 ./test.py -f 2-query/interval_limit_opt_2.py
run_case system-test python3 ./test.py -f 2-query/func_to_char_timestamp.py
run_case system-test python3 ./test.py -f 2-query/func_to_char_timestamp.py -Q 2
run_case system-test python3 ./test.py -f 2-query/func_to_char_timestamp.py -Q 3
run_case system-test python3 ./test.py -f 2-query/func_to_char_timestamp.py -Q 4
run_case system-test python3 ./test.py -f 2-query/last_cache_scan.py
run_case system-test python3 ./test.py -f 2-query/last_cache_scan.py -Q 2
run_case system-test python3 ./test.py -f 2-query/last_cache_scan.py -Q 3
run_case system-test python3 ./test.py -f 2-query/last_cache_scan.py -Q 4
run_case system-test python3 ./test.py -f 2-query/tbname.py
run_case system-test python3 ./test.py -f 2-query/tbname.py -Q 2
run_case system-test python3 ./test.py -f 2-query/tbname.py -Q 3
run_case system-test python3 ./test.py -f 2-query/tbname.py -Q 4
run_case system-test python3 ./test.py -f 2-query/decimal.py
run_case system-test python3 ./test.py -f 2-query/decimal.py -Q 4
run_case system-test python3 ./test.py -f 2-query/decimal.py -Q 3
run_case system-test python3 ./test.py -f 2-query/decimal.py -Q 2
run_case system-test python3 ./test.py -f 2-query/decimal.py -Q 1
run_case system-test python3 ./test.py -f 2-query/decimal2.py
run_case system-test python3 ./test.py -f 2-query/decimal2.py -Q 4
run_case system-test python3 ./test.py -f 2-query/decimal2.py -Q 3
run_case system-test python3 ./test.py -f 2-query/decimal2.py -Q 2
run_case system-test python3 ./test.py -f 2-query/decimal2.py -Q 1
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/decimal3.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/decimal3.py -Q 4
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/decimal3.py -Q 3
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/decimal3.py -Q 2
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/decimal3.py -Q 1
run_case system-test python3 ./test.py -f 2-query/tbnameIn.py
run_case system-test python3 ./test.py -f 2-query/tbnameIn.py -R
run_case system-test python3 ./test.py -f 2-query/tbnameIn.py -Q 2
run_case system-test python3 ./test.py -f 2-query/tbnameIn.py -Q 3
run_case system-test python3 ./test.py -f 2-query/tbnameIn.py -Q 4
run_case system-test python3 ./test.py -f 2-query/nestedQuery2.py
run_case system-test python3 ./test.py -f 2-query/interp_extension.py
run_case system-test python3 ./test.py -f 2-query/interp_extension.py -R
run_case system-test python3 ./test.py -f 2-query/interp_extension.py -Q 2
run_case system-test python3 ./test.py -f 2-query/interp_extension.py -Q 3
run_case system-test python3 ./test.py -f 2-query/interp_extension.py -Q 4
run_case system-test python3 ./test.py -f 7-tmq/tmqShow.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 7-tmq/tmqDropStb.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_update_tablelist.py
run_case system-test python3 ./test.py -f 7-tmq/subscribeStb0.py
run_case system-test python3 ./test.py -f 7-tmq/subscribeStb1.py
run_case system-test python3 ./test.py -f 7-tmq/subscribeStb2.py
run_case system-test python3 ./test.py -f 7-tmq/subscribeStb3.py
run_case system-test python3 ./test.py -f 7-tmq/subscribeDb0.py -N 3 -n 3
run_case system-test python3 ./test.py -f 7-tmq/ins_topics_test.py
run_case system-test python3 ./test.py -f 7-tmq/tmqMaxTopic.py
run_case system-test python3 ./test.py -f 7-tmq/tmqParamsTest.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 7-tmq/tmqParamsTest.py -R
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 7-tmq/tmqMaxGroupIds.py
run_case system-test python3 ./test.py -f 7-tmq/tmqConsumeDiscontinuousData.py
run_case system-test python3 ./test.py -f 7-tmq/tmqOffset.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 7-tmq/tmqDropConsumer.py
run_case system-test python3 ./test.py -f 1-insert/insert_stb.py
run_case system-test python3 ./test.py -f 1-insert/delete_stable.py
run_case system-test python3 ./test.py -f 1-insert/stt_blocks_check.py
run_case system-test python3 ./test.py -f 2-query/out_of_order.py -Q 3
run_case system-test python3 ./test.py -f 2-query/out_of_order.py
run_case system-test python3 ./test.py -f 2-query/agg_null.py
run_case system-test python3 ./test.py -f 2-query/insert_null_none.py
# [AUTO-DISABLED 2026-04-19] time limit (15 min) exceeded
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/insert_null_none.py -R
run_case system-test python3 ./test.py -f 2-query/insert_null_none.py -Q 2
run_case system-test python3 ./test.py -f 2-query/insert_null_none.py -Q 3
run_case system-test python3 ./test.py -f 2-query/insert_null_none.py -Q 4
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 1-insert/database_pre_suf.py
run_case system-test python3 ./test.py -f 2-query/concat.py -Q 3
run_case system-test python3 ./test.py -f 2-query/out_of_order.py -Q 2
run_case system-test python3 ./test.py -f 2-query/out_of_order.py -Q 4
run_case system-test python3 ./test.py -f 2-query/nestedQuery.py -Q 3
run_case system-test python3 ./test.py -f 2-query/nestedQuery_str.py -Q 3
run_case system-test python3 ./test.py -f 2-query/nestedQuery_math.py -Q 3
run_case system-test python3 ./test.py -f 2-query/nestedQuery_time.py -Q 3
run_case system-test python3 ./test.py -f 2-query/nestedQuery_26.py -Q 3
run_case system-test python3 ./test.py -f 2-query/select_null.py
run_case system-test python3 ./test.py -f 2-query/select_null.py -R
run_case system-test python3 ./test.py -f 2-query/select_null.py -Q 2
run_case system-test python3 ./test.py -f 2-query/select_null.py -Q 3
run_case system-test python3 ./test.py -f 2-query/select_null.py -Q 4
run_case system-test python3 ./test.py -f 2-query/slimit.py
run_case system-test python3 ./test.py -f 2-query/slimit.py -R
run_case system-test python3 ./test.py -f 2-query/slimit.py -Q 2
run_case system-test python3 ./test.py -f 2-query/slimit.py -Q 3
run_case system-test python3 ./test.py -f 2-query/slimit.py -Q 4
run_case system-test python3 ./test.py -f 2-query/ts-5761.py
run_case system-test python3 ./test.py -f 2-query/ts-5761-scalemode.py
run_case system-test python3 ./test.py -f 2-query/ts-5712.py
run_case system-test python3 ./test.py -f 2-query/ts-4233.py
run_case system-test python3 ./test.py -f 2-query/ts-4233.py -Q 2
run_case system-test python3 ./test.py -f 2-query/ts-4233.py -Q 3
run_case system-test python3 ./test.py -f 2-query/ts-4233.py -Q 4
run_case system-test python3 ./test.py -f 2-query/like.py
run_case system-test python3 ./test.py -f 2-query/like.py -Q 2
run_case system-test python3 ./test.py -f 2-query/like.py -Q 3
run_case system-test python3 ./test.py -f 2-query/like.py -Q 4
run_case system-test python3 ./test.py -f 2-query/match.py
run_case system-test python3 ./test.py -f 2-query/match.py -Q 2
run_case system-test python3 ./test.py -f 2-query/match.py -Q 3
run_case system-test python3 ./test.py -f 2-query/match.py -Q 4
run_case system-test python3 ./test.py -f 2-query/td-28068.py
run_case system-test python3 ./test.py -f 2-query/td-28068.py -Q 2
run_case system-test python3 ./test.py -f 2-query/td-28068.py -Q 3
run_case system-test python3 ./test.py -f 2-query/td-28068.py -Q 4
run_case system-test python3 ./test.py -f 2-query/agg_group_AlwaysReturnValue.py
run_case system-test python3 ./test.py -f 2-query/agg_group_AlwaysReturnValue.py -Q 2
run_case system-test python3 ./test.py -f 2-query/agg_group_AlwaysReturnValue.py -Q 3
run_case system-test python3 ./test.py -f 2-query/agg_group_AlwaysReturnValue.py -Q 4
run_case system-test python3 ./test.py -f 2-query/agg_group_NotReturnValue.py
run_case system-test python3 ./test.py -f 2-query/agg_group_NotReturnValue.py -Q 2
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/agg_group_NotReturnValue.py -Q 3
run_case system-test python3 ./test.py -f 2-query/agg_group_NotReturnValue.py -Q 4
run_case system-test python3 ./test.py -f 2-query/td-32548.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/large_data.py
run_case system-test python3 ./test.py -f 2-query/stddev_test.py
run_case system-test python3 ./test.py -f 2-query/stddev_test.py -Q 2
run_case system-test python3 ./test.py -f 2-query/stddev_test.py -Q 3
run_case system-test python3 ./test.py -f 2-query/stddev_test.py -Q 4
run_case system-test python3 ./test.py -f 8-stream/checkpoint_info.py -N 4
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 8-stream/checkpoint_info2.py -N 4
run_case system-test python3 ./test.py -f 1-insert/test_multi_insert.py
run_case system-test python3 ./test.py -f 3-enterprise/restore/restoreDnode.py -N 5 -M 3 -i False
run_case system-test python3 ./test.py -f 3-enterprise/restore/restoreVnode.py -N 5 -M 3 -i False
run_case system-test python3 ./test.py -f 3-enterprise/restore/restoreMnode.py -N 5 -M 3 -i False
run_case system-test python3 ./test.py -f 3-enterprise/restore/restoreQnode.py -N 5 -M 3 -i False
run_case system-test python3 ./test.py -f 7-tmq/create_wrong_topic.py
run_case system-test python3 ./test.py -f 7-tmq/dropDbR3ConflictTransaction.py -N 3
run_case system-test python3 ./test.py -f 7-tmq/basic5.py
run_case system-test python3 ./test.py -f 7-tmq/subscribeDb.py -N 3 -n 3
run_case system-test python3 ./test.py -f 7-tmq/ts-4674.py -N 3 -n 3
run_case system-test python3 ./test.py -f 7-tmq/td-30270.py
run_case system-test python3 ./test.py -f 7-tmq/subscribeDb1.py
run_case system-test python3 ./test.py -f 7-tmq/subscribeDb2.py
run_case system-test python3 ./test.py -f 7-tmq/subscribeDb3.py
run_case system-test python3 ./test.py -f 7-tmq/subscribeDb4.py
run_case system-test python3 ./test.py -f 7-tmq/subscribeStb.py
run_case system-test python3 ./test.py -f 7-tmq/subscribeStb4.py
run_case system-test python3 ./test.py -f 7-tmq/db.py
run_case system-test python3 ./test.py -f 7-tmq/tmqError.py
run_case system-test python3 ./test.py -f 7-tmq/schema.py
run_case system-test python3 ./test.py -f 7-tmq/stbFilterWhere.py
run_case system-test python3 ./test.py -f 7-tmq/stbFilter.py
run_case system-test python3 ./test.py -f 7-tmq/tmqCheckData.py
run_case system-test python3 ./test.py -f 7-tmq/tmqCheckData1.py
run_case system-test python3 ./test.py -f 7-tmq/tmqConsumerGroup.py
run_case system-test python3 ./test.py -f 7-tmq/tmqAlterSchema.py
run_case system-test python3 ./test.py -f 7-tmq/tmqConsFromTsdb.py -N 3 -n 3
run_case system-test python3 ./test.py -f 7-tmq/tmqConsFromTsdb1.py -N 3 -n 3
run_case system-test python3 ./test.py -f 7-tmq/tmqConsFromTsdb-mutilVg.py
run_case system-test python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-mutilVg.py
run_case system-test python3 ./test.py -f 7-tmq/tmqConsFromTsdb-1ctb.py
run_case system-test python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-1ctb.py
run_case system-test python3 ./test.py -f 7-tmq/tmqConsFromTsdb-1ctb-funcNFilter.py
run_case system-test python3 ./test.py -f 7-tmq/tmqConsFromTsdb-mutilVg-mutilCtb-funcNFilter.py
run_case system-test python3 ./test.py -f 7-tmq/tmqConsFromTsdb-mutilVg-mutilCtb.py
run_case system-test python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-1ctb-funcNFilter.py
run_case system-test python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-mutilVg-mutilCtb-funcNFilter.py
run_case system-test python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-mutilVg-mutilCtb.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 7-tmq/tmqAutoCreateTbl.py
run_case system-test python3 ./test.py -f 7-tmq/tmqDnodeRestart.py
run_case system-test python3 ./test.py -f 7-tmq/tmqDnodeRestart1.py
run_case system-test python3 ./test.py -f 7-tmq/tmqUpdate-1ctb.py
run_case system-test python3 ./test.py -f 7-tmq/tmqUpdateWithConsume.py -N 3 -n 3
run_case system-test python3 ./test.py -f 7-tmq/tmqUpdate-multiCtb-snapshot0.py
run_case system-test python3 ./test.py -f 7-tmq/tmqUpdate-multiCtb-snapshot1.py
run_case system-test python3 ./test.py -f 7-tmq/tmqDelete-1ctb.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 7-tmq/tmqDelete-multiCtb.py -N 3 -n 3
run_case system-test python3 ./test.py -f 7-tmq/tmqDropStbCtb.py
run_case system-test python3 ./test.py -f 7-tmq/tmqDropNtb-snapshot0.py
run_case system-test python3 ./test.py -f 7-tmq/tmqDropNtb-snapshot1.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 7-tmq/tmqUdf.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 7-tmq/tmqUdf-multCtb-snapshot0.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 7-tmq/tmqUdf-multCtb-snapshot1.py
run_case system-test python3 ./test.py -f 7-tmq/stbTagFilter-1ctb.py
run_case system-test python3 ./test.py -f 7-tmq/dataFromTsdbNWal.py
run_case system-test python3 ./test.py -f 7-tmq/dataFromTsdbNWal-multiCtb.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 7-tmq/tmq_taosx.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_connection.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_ts5466.py
run_case system-test python3 ./test.py -f 7-tmq/test_tmq_td38404.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_td33504.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_td37265.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_ts6392.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_ts5906.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_ts7402.py
run_case system-test python3 ./test.py -f 7-tmq/td-32187.py
run_case system-test python3 ./test.py -f 7-tmq/td-33225.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_ts4563.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_ts7662.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_td35698.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_td32526.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_td32471.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_ts6115.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_tx484.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_replay.py
run_case system-test python3 ./test.py -f 7-tmq/tmqSeekAndCommit.py
run_case system-test python3 ./test.py -f 7-tmq/tmq_offset.py
run_case system-test python3 ./test.py -f 7-tmq/tmqDataPrecisionUnit.py
run_case system-test python3 ./test.py -f 7-tmq/raw_block_interface_test.py
run_case system-test python3 ./test.py -f 7-tmq/stbTagFilter-multiCtb.py
run_case system-test python3 ./test.py -f 7-tmq/tmqSubscribeStb-r3.py -N 5
run_case system-test python3 ./test.py -f 7-tmq/tmq3mnodeSwitch.py -N 6 -M 3 -i True
run_case system-test python3 ./test.py -f 7-tmq/tmq3mnodeSwitch.py -N 6 -M 3 -n 3 -i True
run_case system-test python3 test.py -f 7-tmq/tmqVnodeTransform-db-removewal.py -N 2 -n 1
run_case system-test python3 test.py -f 7-tmq/tmqVnodeTransform-stb-removewal.py -N 6 -n 3
run_case system-test python3 test.py -f 7-tmq/tmqVnodeTransform-stb.py -N 2 -n 1
run_case system-test python3 test.py -f 7-tmq/tmqVnodeTransform-stb.py -N 6 -n 3
run_case system-test python3 test.py -f 7-tmq/tmqVnodeSplit-stb-select.py -N 2 -n 1
run_case system-test python3 test.py -f 7-tmq/tmqVnodeSplit-stb-select-duplicatedata.py -N 3 -n 3
run_case system-test python3 test.py -f 7-tmq/tmqVnodeSplit-stb-select-duplicatedata-false.py -N 3 -n 3
run_case system-test python3 test.py -f 7-tmq/tmqVnodeSplit-stb-select.py -N 3 -n 3
run_case system-test python3 test.py -f 7-tmq/tmqVnodeSplit-ntb-select.py -N 3 -n 3
run_case system-test python3 test.py -f 7-tmq/tmqVnodeSplit-stb-select-false.py -N 3 -n 3
run_case system-test python3 test.py -f 7-tmq/tmqVnodeSplit-stb.py -N 3 -n 3
run_case system-test python3 test.py -f 7-tmq/tmqVnodeSplit-stb-false.py -N 3 -n 3
run_case system-test python3 test.py -f 7-tmq/tmqVnodeSplit-column.py -N 3 -n 3
run_case system-test python3 test.py -f 7-tmq/tmqVnodeSplit-column-false.py -N 3 -n 3
run_case system-test python3 test.py -f 7-tmq/tmqVnodeSplit-db.py -N 3 -n 3
run_case system-test python3 test.py -f 7-tmq/tmqVnodeSplit-db-false.py -N 3 -n 3
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 7-tmq/walRemoveLog.py -N 3
run_case system-test python3 test.py -f 7-tmq/tmqVnodeReplicate.py -M 3 -N 3 -n 3
run_case system-test python3 ./test.py -f 99-TDcase/TD-19201.py
run_case system-test python3 ./test.py -f 99-TDcase/TD-21561.py
run_case system-test python3 ./test.py -f 99-TDcase/TS-3404.py
run_case system-test python3 ./test.py -f 99-TDcase/TS-3581.py
run_case system-test python3 ./test.py -f 99-TDcase/TS-3311.py
run_case system-test python3 ./test.py -f 99-TDcase/TS-3821.py
run_case system-test python3 ./test.py -f 99-TDcase/TS-5130.py
run_case system-test python3 ./test.py -f 99-TDcase/TS-5580.py
run_case system-test python3 ./test.py -f 0-others/balance_vgroups_r1.py -N 6
run_case system-test python3 ./test.py -f 0-others/taosShell.py
run_case system-test python3 ./test.py -f 0-others/taosShellError.py
run_case system-test python3 ./test.py -f 0-others/taosShellNetChk.py
run_case system-test python3 ./test.py -f 0-others/telemetry.py
run_case system-test python3 ./test.py -f 0-others/backquote_check.py
run_case system-test python3 ./test.py -f 0-others/taosdMonitor.py
run_case system-test python3 ./test.py -f 0-others/taosdNewMonitor.py
run_case system-test python3 ./test.py -f 0-others/taosd_audit.py
# # [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 0-others/taosdlog.py
run_case system-test python3 ./test.py -f 0-others/taosdShell.py -N 5 -M 3 -Q 3
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 0-others/udfTest.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 0-others/udf_create.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 0-others/udf_restart_taosd.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 0-others/udf_cfg1.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 0-others/udf_cfg2.py
run_case system-test python3 ./test.py -f 0-others/cachemodel.py
run_case system-test python3 ./test.py -f 0-others/sysinfo.py
run_case system-test python3 ./test.py -f 0-others/user_control.py
run_case system-test python3 ./test.py -f 0-others/user_manage.py
run_case system-test python3 ./test.py -f 0-others/user_privilege.py
run_case system-test python3 ./test.py -f 0-others/user_privilege_show.py
run_case system-test python3 ./test.py -f 0-others/user_privilege_all.py
run_case system-test python3 ./test.py -f 0-others/fsync.py
run_case system-test python3 ./test.py -f 0-others/multilevel.py
run_case system-test python3 ./test.py -f 0-others/retention_test.py
run_case system-test python3 ./test.py -f 0-others/retention_test2.py
run_case system-test python3 ./test.py -f 0-others/multilevel_createdb.py
run_case system-test python3 ./test.py -f 0-others/ttl.py
run_case system-test python3 ./test.py -f 0-others/ttlChangeOnWrite.py
run_case system-test python3 ./test.py -f 0-others/compress_tsz1.py
run_case system-test python3 ./test.py -f 0-others/compress_tsz2.py
run_case system-test python3 ./test.py -f 0-others/view/non_marterial_view/test_view.py
run_case system-test python3 ./test.py -f 0-others/test_show_table_distributed.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 0-others/test_show_disk_usage.py
run_case system-test python3 ./test.py -f 0-others/show_disk_usage_multilevel.py
run_case system-test python3 ./test.py -f 0-others/test_dismatch_config.py
run_case system-test python3 ./test.py -f 0-others/sml_restart.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 0-others/tag_index_basic.py
run_case system-test python3 ./test.py -f 0-others/udfpy_main.py
run_case system-test python3 ./test.py -N 3 -f 0-others/walRetention.py
run_case system-test python3 ./test.py -f 0-others/wal_level_skip.py
run_case system-test python3 ./test.py -f 0-others/splitVGroup.py -N 3 -n 1
run_case system-test python3 ./test.py -f 0-others/splitVGroupWal.py -N 3 -n 1
run_case system-test python3 ./test.py -f 0-others/splitVGroup.py -N 3 -n 3
run_case system-test python3 ./test.py -f 0-others/splitVGroupWal.py -N 3 -n 3
run_case system-test python3 ./test.py -f 0-others/timeRangeWise.py -N 3
run_case system-test python3 ./test.py -f 0-others/delete_check.py
run_case system-test python3 ./test.py -f 0-others/test_hot_refresh_configurations.py
run_case system-test python3 ./test.py -f 0-others/subscribe_stream_privilege.py
run_case system-test python3 ./test.py -f 0-others/empty_identifier.py
run_case system-test python3 ./test.py -f 0-others/show_transaction_detail.py -N 3
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 0-others/kill_balance_leader.py -N 3
run_case system-test python3 ./test.py -f 3-enterprise/restore/kill_restore_dnode.py -N 5
run_case system-test python3 ./test.py -f 0-others/persisit_config.py
run_case system-test python3 ./test.py -f 0-others/qmemCtrl.py
run_case system-test python3 ./test.py -f 0-others/compact_vgroups.py
run_case system-test python3 ./test.py -f 0-others/compact_auto.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 0-others/dumpsdb.py
run_case system-test python3 ./test.py -f 0-others/compact.py -N 3
run_case system-test python3 ./test.py -f 1-insert/composite_primary_key_create.py
run_case system-test python3 ./test.py -f 1-insert/composite_primary_key_insert.py
run_case system-test python3 ./test.py -f 1-insert/composite_primary_key_delete.py
run_case system-test python3 ./test.py -f 1-insert/insert_double.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 1-insert/alter_database.py
run_case system-test python3 ./test.py -f 1-insert/alter_replica.py -N 3
run_case system-test python3 ./test.py -f 1-insert/influxdb_line_taosc_insert.py
run_case system-test python3 ./test.py -f 1-insert/opentsdb_telnet_line_taosc_insert.py
run_case system-test python3 ./test.py -f 1-insert/opentsdb_json_taosc_insert.py
run_case system-test python3 ./test.py -f 1-insert/test_stmt_muti_insert_query.py
run_case system-test python3 ./test.py -f 1-insert/test_stmt_set_tbname_tag.py
run_case system-test python3 ./test.py -f 1-insert/alter_stable.py
run_case system-test python3 ./test.py -f 1-insert/alter_table.py
run_case system-test python3 ./test.py -f 1-insert/boundary.py
run_case system-test python3 ./test.py -f 1-insert/insertWithMoreVgroup.py
run_case system-test python3 ./test.py -f 1-insert/table_comment.py
run_case system-test python3 ./test.py -f 1-insert/mutil_stage.py
run_case system-test python3 ./test.py -f 1-insert/table_param_ttl.py
run_case system-test python3 ./test.py -f 1-insert/table_param_ttl.py -R
run_case system-test python3 ./test.py -f 1-insert/update_data_muti_rows.py
run_case system-test python3 ./test.py -f 1-insert/db_tb_name_check.py
run_case system-test python3 ./test.py -f 1-insert/InsertFuturets.py
run_case system-test python3 ./test.py -f 1-insert/insert_wide_column.py
run_case system-test python3 ./test.py -f 1-insert/insert_column_value.py
run_case system-test python3 ./test.py -f 1-insert/insert_from_csv.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 1-insert/rowlength64k_benchmark.py
run_case system-test python3 ./test.py -f 1-insert/rowlength64k.py
run_case system-test python3 ./test.py -f 1-insert/rowlength64k.py -R
run_case system-test python3 ./test.py -f 1-insert/rowlength64k.py -Q 2
run_case system-test python3 ./test.py -f 1-insert/rowlength64k.py -Q 3
run_case system-test python3 ./test.py -f 1-insert/rowlength64k.py -Q 4
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_1.py
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_1.py -R
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_1.py -Q 2
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_1.py -Q 3
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_1.py -Q 4
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_2.py
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_2.py -R
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_2.py -Q 2
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_2.py -Q 3
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_2.py -Q 4
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_3.py
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_3.py -R
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_3.py -Q 2
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_3.py -Q 3
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_3.py -Q 4
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_4.py
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_4.py -R
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_4.py -Q 2
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_4.py -Q 3
run_case system-test python3 ./test.py -f 1-insert/rowlength64k_4.py -Q 4
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 1-insert/precisionUS.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 1-insert/precisionNS.py
run_case system-test python3 ./test.py -f 1-insert/test_ts4219.py
run_case system-test python3 ./test.py -f 1-insert/ts-4272.py
run_case system-test python3 ./test.py -f 1-insert/test_ts4295.py
run_case system-test python3 ./test.py -f 1-insert/test_td27388.py
run_case system-test python3 ./test.py -f 1-insert/test_ts4479.py
run_case system-test python3 ./test.py -f 1-insert/test_td29793.py
run_case system-test python3 ./test.py -f 1-insert/insert_timestamp.py
run_case system-test python3 ./test.py -f 1-insert/test_td29157.py
run_case system-test python3 ./test.py -f 1-insert/ddl_in_sysdb.py
run_case system-test python3 ./test.py -f 0-others/show.py
run_case system-test python3 ./test.py -f 0-others/show_tag_index.py
run_case system-test python3 ./test.py -f 0-others/information_schema.py
run_case system-test python3 ./test.py -f 0-others/ins_filesets.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 0-others/grant.py
run_case system-test python3 ./test.py -f 2-query/abs.py
run_case system-test python3 ./test.py -f 2-query/abs.py -R
run_case system-test python3 ./test.py -f 2-query/and_or_for_byte.py
run_case system-test python3 ./test.py -f 2-query/and_or_for_byte.py -R
run_case system-test python3 ./test.py -f 2-query/apercentile.py
run_case system-test python3 ./test.py -f 2-query/apercentile.py -R
run_case system-test python3 ./test.py -f 2-query/arccos.py
run_case system-test python3 ./test.py -f 2-query/arccos.py -R
run_case system-test python3 ./test.py -f 2-query/arcsin.py
run_case system-test python3 ./test.py -f 2-query/arcsin.py -R
run_case system-test python3 ./test.py -f 2-query/arctan.py
run_case system-test python3 ./test.py -f 2-query/arctan.py -R
run_case system-test python3 ./test.py -f 2-query/avg.py
run_case system-test python3 ./test.py -f 2-query/avg.py -R
run_case system-test python3 ./test.py -f 2-query/between.py
run_case system-test python3 ./test.py -f 2-query/between.py -R
run_case system-test python3 ./test.py -f 2-query/bottom.py
run_case system-test python3 ./test.py -f 2-query/bottom.py -R
run_case system-test python3 ./test.py -f 2-query/cast.py
run_case system-test python3 ./test.py -f 2-query/cast.py -R
run_case system-test python3 ./test.py -f 2-query/ceil.py
run_case system-test python3 ./test.py -f 2-query/ceil.py -R
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/char_length.py
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/char_length.py -R
run_case system-test python3 ./test.py -f 2-query/check_tsdb.py
run_case system-test python3 ./test.py -f 2-query/check_tsdb.py -R
run_case system-test python3 ./test.py -f 2-query/concat.py
run_case system-test python3 ./test.py -f 2-query/concat.py -R
run_case system-test python3 ./test.py -f 2-query/concat_ws.py
run_case system-test python3 ./test.py -f 2-query/concat_ws.py -R
run_case system-test python3 ./test.py -f 2-query/concat_ws2.py
run_case system-test python3 ./test.py -f 2-query/concat_ws2.py -R
run_case system-test python3 ./test.py -f 2-query/cos.py
run_case system-test python3 ./test.py -f 2-query/cos.py -R
run_case system-test python3 ./test.py -f 2-query/group_partition.py
run_case system-test python3 ./test.py -f 2-query/group_partition.py -R
run_case system-test python3 ./test.py -f 2-query/group_partition.py -Q 2
run_case system-test python3 ./test.py -f 2-query/group_partition.py -Q 3
run_case system-test python3 ./test.py -f 2-query/group_partition.py -Q 4
run_case system-test python3 ./test.py -f 2-query/count_partition.py
run_case system-test python3 ./test.py -f 2-query/count_partition.py -R
run_case system-test python3 ./test.py -f 2-query/count.py
run_case system-test python3 ./test.py -f 2-query/count.py -R
run_case system-test python3 ./test.py -f 2-query/countAlwaysReturnValue.py
run_case system-test python3 ./test.py -f 2-query/countAlwaysReturnValue.py -R
run_case system-test python3 ./test.py -f 2-query/db.py
run_case system-test python3 ./test.py -f 2-query/db.py -N 3 -n 3 -R
run_case system-test python3 ./test.py -f 2-query/diff.py
run_case system-test python3 ./test.py -f 2-query/diff.py -R
run_case system-test python3 ./test.py -f 2-query/distinct.py
run_case system-test python3 ./test.py -f 2-query/distinct.py -R
run_case system-test python3 ./test.py -f 2-query/distribute_agg_apercentile.py
run_case system-test python3 ./test.py -f 2-query/distribute_agg_apercentile.py -R
run_case system-test python3 ./test.py -f 2-query/distribute_agg_avg.py
run_case system-test python3 ./test.py -f 2-query/distribute_agg_avg.py -R
run_case system-test python3 ./test.py -f 2-query/distribute_agg_count.py
run_case system-test python3 ./test.py -f 2-query/distribute_agg_count.py -R
run_case system-test python3 ./test.py -f 2-query/distribute_agg_max.py
run_case system-test python3 ./test.py -f 2-query/distribute_agg_max.py -R
run_case system-test python3 ./test.py -f 2-query/distribute_agg_min.py
run_case system-test python3 ./test.py -f 2-query/distribute_agg_min.py -R
run_case system-test python3 ./test.py -f 2-query/distribute_agg_spread.py
run_case system-test python3 ./test.py -f 2-query/distribute_agg_spread.py -R
run_case system-test python3 ./test.py -f 2-query/distribute_agg_stddev.py
run_case system-test python3 ./test.py -f 2-query/distribute_agg_stddev.py -R
run_case system-test python3 ./test.py -f 2-query/distribute_agg_sum.py
run_case system-test python3 ./test.py -f 2-query/distribute_agg_sum.py -R
run_case system-test python3 ./test.py -f 2-query/explain.py
run_case system-test python3 ./test.py -f 2-query/explain.py -R
run_case system-test python3 ./test.py -f 2-query/first.py
run_case system-test python3 ./test.py -f 2-query/first.py -R
run_case system-test python3 ./test.py -f 2-query/floor.py
run_case system-test python3 ./test.py -f 2-query/floor.py -R
run_case system-test python3 ./test.py -f 2-query/function_null.py
run_case system-test python3 ./test.py -f 2-query/function_null.py -R
run_case system-test python3 ./test.py -f 2-query/function_stateduration.py
run_case system-test python3 ./test.py -f 2-query/function_stateduration.py -R
run_case system-test python3 ./test.py -f 2-query/histogram.py
run_case system-test python3 ./test.py -f 2-query/histogram.py -R
run_case system-test python3 ./test.py -f 2-query/hyperloglog.py
run_case system-test python3 ./test.py -f 2-query/hyperloglog.py -R
run_case system-test python3 ./test.py -f 2-query/interp.py
run_case system-test python3 ./test.py -f 2-query/interp.py -R
run_case system-test python3 ./test.py -f 2-query/fill.py
run_case system-test python3 ./test.py -f 2-query/fill2.py
run_case system-test python3 ./test.py -f 2-query/fill2.py -Q 2
run_case system-test python3 ./test.py -f 2-query/fill2.py -Q 3
run_case system-test python3 ./test.py -f 2-query/fill2.py -Q 4
run_case system-test python3 ./test.py -f 2-query/fill2.py -R
run_case system-test python3 ./test.py -f 2-query/irate.py
run_case system-test python3 ./test.py -f 2-query/irate.py -R
run_case system-test python3 ./test.py -f 2-query/join.py
run_case system-test python3 ./test.py -f 2-query/join.py -R
run_case system-test python3 ./test.py -f 2-query/last_row.py
run_case system-test python3 ./test.py -f 2-query/last_row.py -R
run_case system-test python3 ./test.py -f 2-query/last.py
run_case system-test python3 ./test.py -f 2-query/last.py -R
run_case system-test python3 ./test.py -f 2-query/last_and_last_row.py
run_case system-test python3 ./test.py -f 2-query/last_and_last_row.py -R
run_case system-test python3 ./test.py -f 2-query/last_and_last_row.py -Q 2
run_case system-test python3 ./test.py -f 2-query/last_and_last_row.py -Q 3
run_case system-test python3 ./test.py -f 2-query/last_and_last_row.py -Q 4
run_case system-test python3 ./test.py -f 2-query/last+last_row.py
run_case system-test python3 ./test.py -f 2-query/last+last_row.py -Q 2
run_case system-test python3 ./test.py -f 2-query/last+last_row.py -Q 3
run_case system-test python3 ./test.py -f 2-query/last+last_row.py -Q 4
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_1.py
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_1.py -R
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_1.py -Q 2
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_1.py -Q 3
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_1.py -Q 4
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_2.py
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_2.py -R
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_2.py -Q 2
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_2.py -Q 3
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_2.py -Q 4
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_3.py
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_3.py -R
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_3.py -Q 2
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_3.py -Q 3
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_3.py -Q 4
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_4.py
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_4.py -R
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_4.py -Q 2
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_4.py -Q 3
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_4.py -Q 4
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_5.py
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_5.py -R
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_5.py -Q 2
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_5.py -Q 3
run_case system-test python3 ./test.py -f 2-query/primary_ts_base_5.py -Q 4
run_case system-test python3 ./test.py -f 2-query/leastsquares.py
run_case system-test python3 ./test.py -f 2-query/leastsquares.py -R
run_case system-test python3 ./test.py -f 2-query/length.py
run_case system-test python3 ./test.py -f 2-query/length.py -R
run_case system-test python3 ./test.py -f 2-query/limit.py
run_case system-test python3 ./test.py -f 2-query/log.py
run_case system-test python3 ./test.py -f 2-query/log.py -R
run_case system-test python3 ./test.py -f 2-query/logical_operators.py
run_case system-test python3 ./test.py -f 2-query/logical_operators.py -R
run_case system-test python3 ./test.py -f 2-query/lower.py
run_case system-test python3 ./test.py -f 2-query/lower.py -R
run_case system-test python3 ./test.py -f 2-query/ltrim.py
run_case system-test python3 ./test.py -f 2-query/ltrim.py -R
run_case system-test python3 ./test.py -f 2-query/mavg.py
run_case system-test python3 ./test.py -f 2-query/mavg.py -R
run_case system-test python3 ./test.py -f 2-query/max_partition.py
run_case system-test python3 ./test.py -f 2-query/max_partition.py -R
run_case system-test python3 ./test.py -f 2-query/partition_limit_interval.py
run_case system-test python3 ./test.py -f 2-query/partition_limit_interval.py -R
run_case system-test python3 ./test.py -f 2-query/max_min_last_interval.py
run_case system-test python3 ./test.py -f 2-query/last_row_interval.py
run_case system-test python3 ./test.py -f 2-query/max.py
run_case system-test python3 ./test.py -f 2-query/max.py -R
run_case system-test python3 ./test.py -f 2-query/min.py
run_case system-test python3 ./test.py -f 2-query/min.py -R
run_case system-test python3 ./test.py -f 2-query/normal.py
run_case system-test python3 ./test.py -f 2-query/normal.py -R
run_case system-test python3 ./test.py -f 2-query/not.py
run_case system-test python3 ./test.py -f 2-query/not.py -R
run_case system-test python3 ./test.py -f 2-query/mode.py
run_case system-test python3 ./test.py -f 2-query/mode.py -R
run_case system-test python3 ./test.py -f 2-query/Now.py
run_case system-test python3 ./test.py -f 2-query/Now.py -R
run_case system-test python3 ./test.py -f 2-query/orderBy.py -N 5
run_case system-test python3 ./test.py -f 2-query/percentile.py
run_case system-test python3 ./test.py -f 2-query/percentile.py -R
run_case system-test python3 ./test.py -f 2-query/pow.py
run_case system-test python3 ./test.py -f 2-query/pow.py -R
run_case system-test python3 ./test.py -f 2-query/query_cols_tags_and_or.py
run_case system-test python3 ./test.py -f 2-query/query_cols_tags_and_or.py -R
run_case system-test python3 ./test.py -f 2-query/round.py
run_case system-test python3 ./test.py -f 2-query/round.py -R
run_case system-test python3 ./test.py -f 2-query/rtrim.py
run_case system-test python3 ./test.py -f 2-query/rtrim.py -R
run_case system-test python3 ./test.py -f 2-query/sample.py
run_case system-test python3 ./test.py -f 2-query/sample.py -R
run_case system-test python3 ./test.py -f 2-query/sin.py
run_case system-test python3 ./test.py -f 2-query/sin.py -R
run_case system-test python3 ./test.py -f 2-query/smaBasic.py -N 3
run_case system-test python3 ./test.py -f 2-query/smaTest.py
run_case system-test python3 ./test.py -f 2-query/smaTest.py -R
run_case system-test python3 ./test.py -f 2-query/sml_TS-3724.py
run_case system-test python3 ./test.py -f 2-query/sml-TD19291.py
run_case system-test python3 ./test.py -f 2-query/varbinary.py
run_case system-test python3 ./test.py -f 2-query/sml.py
run_case system-test python3 ./test.py -f 2-query/sml.py -R
run_case system-test python3 ./test.py -f 2-query/spread.py
run_case system-test python3 ./test.py -f 2-query/spread.py -R
run_case system-test python3 ./test.py -f 2-query/sqrt.py
run_case system-test python3 ./test.py -f 2-query/sqrt.py -R
run_case system-test python3 ./test.py -f 2-query/statecount.py
run_case system-test python3 ./test.py -f 2-query/statecount.py -R
run_case system-test python3 ./test.py -f 2-query/stateduration.py
run_case system-test python3 ./test.py -f 2-query/stateduration.py -R
run_case system-test python3 ./test.py -f 2-query/substr.py
run_case system-test python3 ./test.py -f 2-query/substr.py -R
run_case system-test python3 ./test.py -f 2-query/sum.py
run_case system-test python3 ./test.py -f 2-query/sum.py -R
run_case system-test python3 ./test.py -f 2-query/tail.py
run_case system-test python3 ./test.py -f 2-query/tail.py -R
run_case system-test python3 ./test.py -f 2-query/tan.py
run_case system-test python3 ./test.py -f 2-query/tan.py -R
run_case system-test python3 ./test.py -f 2-query/Timediff.py
run_case system-test python3 ./test.py -f 2-query/Timediff.py -R
run_case system-test python3 ./test.py -f 2-query/timetruncate.py
run_case system-test python3 ./test.py -f 2-query/timetruncate.py -R
run_case system-test python3 ./test.py -f 2-query/timezone.py
run_case system-test python3 ./test.py -f 2-query/timezone.py -R
run_case system-test python3 ./test.py -f 2-query/To_iso8601.py
run_case system-test python3 ./test.py -f 2-query/To_iso8601.py -R
run_case system-test python3 ./test.py -f 2-query/To_unixtimestamp.py
run_case system-test python3 ./test.py -f 2-query/To_unixtimestamp.py -R
run_case system-test python3 ./test.py -f 2-query/Today.py
run_case system-test python3 ./test.py -f 2-query/Today.py -R
run_case system-test python3 ./test.py -f 2-query/top.py
run_case system-test python3 ./test.py -f 2-query/top.py -R
run_case system-test python3 ./test.py -f 2-query/tsbsQuery.py
run_case system-test python3 ./test.py -f 2-query/tsbsQuery.py -R
run_case system-test python3 ./test.py -f 2-query/ttl_comment.py
run_case system-test python3 ./test.py -f 2-query/ttl_comment.py -R
run_case system-test python3 ./test.py -f 2-query/twa.py
run_case system-test python3 ./test.py -f 2-query/twa.py -R
run_case system-test python3 ./test.py -f 2-query/union.py
run_case system-test python3 ./test.py -f 2-query/union.py -R
run_case system-test python3 ./test.py -f 2-query/unique.py
run_case system-test python3 ./test.py -f 2-query/unique.py -R
run_case system-test python3 ./test.py -f 2-query/upper.py
run_case system-test python3 ./test.py -f 2-query/upper.py -R
run_case system-test python3 ./test.py -f 2-query/varchar.py
run_case system-test python3 ./test.py -f 2-query/varchar.py -R
run_case system-test python3 ./test.py -f 2-query/case_when.py
run_case system-test python3 ./test.py -f 2-query/case_when.py -R
run_case system-test python3 ./test.py -f 2-query/blockSMA.py
run_case system-test python3 ./test.py -f 2-query/blockSMA.py -R
run_case system-test python3 ./test.py -f 2-query/projectionDesc.py
run_case system-test python3 ./test.py -f 2-query/projectionDesc.py -R
run_case system-test python3 ./test.py -f 1-insert/update_data.py
run_case system-test python3 ./test.py -f 1-insert/tb_100w_data_order.py
run_case system-test python3 ./test.py -f 1-insert/delete_childtable.py
run_case system-test python3 ./test.py -f 1-insert/delete_normaltable.py
run_case system-test python3 ./test.py -f 1-insert/keep_expired.py
run_case system-test python3 ./test.py -f 1-insert/stmt_error.py
run_case system-test python3 ./test.py -f 1-insert/drop.py
run_case system-test python3 ./test.py -f 1-insert/drop.py -N 3 -M 3 -i False -n 3
run_case system-test python3 ./test.py -f 2-query/join2.py
run_case system-test python3 ./test.py -f 2-query/union1.py
run_case system-test python3 ./test.py -f 2-query/concat2.py
run_case system-test python3 ./test.py -f 2-query/json_tag.py
run_case system-test python3 ./test.py -f 2-query/nestedQueryInterval.py
run_case system-test python3 ./test.py -f 2-query/systable_func.py
run_case system-test python3 ./test.py -f 2-query/test_ts4382.py
run_case system-test python3 ./test.py -f 2-query/test_ts4403.py
run_case system-test python3 ./test.py -f 2-query/test_td28163.py
run_case system-test python3 ./test.py -f 2-query/stablity.py
run_case system-test python3 ./test.py -f 2-query/stablity_1.py
run_case system-test python3 ./test.py -f 2-query/elapsed.py
run_case system-test python3 ./test.py -f 2-query/csum.py
run_case system-test python3 ./test.py -f 2-query/function_diff.py
run_case system-test python3 ./test.py -f 2-query/tagFilter.py
# duplicate with row# 1155 run_case system-test python3 ./test.py -f 2-query/projectionDesc.py
run_case system-test python3 ./test.py -f 2-query/ts_3405_3398_3423.py -N 3 -n 3
run_case system-test python3 ./test.py -f 2-query/ts-4348-td-27939.py
run_case system-test python3 ./test.py -f 2-query/backslash_g.py
run_case system-test python3 ./test.py -f 2-query/test_ts4467.py
run_case system-test python3 ./test.py -f 2-query/geometry.py
# [AUTO-DISABLED 2026-04-20] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 0-others/compatibility_rolling_upgrade_all.py -N 3
run_case system-test python3 ./test.py -f 2-query/queryQnode.py
run_case system-test python3 ./test.py -f 6-cluster/5dnode1mnode.py
run_case system-test python3 ./test.py -f 6-cluster/5dnode2mnode.py -N 5
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeStop.py -N 5 -M 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeStop.py -N 5 -M 3 -i False
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeStop2Follower.py -N 5 -M 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeStop2Follower.py -N 5 -M 3 -i False
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeStopLoop.py -N 5 -M 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateDb.py -N 6 -M 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateDb.py -N 6 -M 3 -n 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateDb.py -N 6 -M 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateDb.py -N 6 -M 3 -n 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopVnodeCreateDb.py -N 6 -M 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopVnodeCreateDb.py -N 6 -M 3 -n 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeModifyMeta.py  -N 6 -M 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeModifyMeta.py  -N 6 -M 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateStb.py -N 6 -M 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateStb.py -N 6 -M 3 -n 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateStb.py -N 6 -M 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateStb.py -N 6 -M 3 -n 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopVnodeCreateStb.py -N 6 -M 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopVnodeCreateStb.py -N 6 -M 3 -n 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeRestartDnodeInsertData.py -N 6 -M 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeRestartDnodeInsertData.py -N 6 -M 3 -n 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeRestartDnodeInsertDataAsync.py -N 6 -M 3
run_case system-test python3 ./test.py -f 6-cluster/manually-test/6dnode3mnodeInsertLessDataAlterRep3to1to3.py -N 6 -M 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeAdd1Ddnoe.py -N 7 -M 3 -C 6
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeAdd1Ddnoe.py -N 7 -M 3 -C 6 -n 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeRecreateMnode.py -N 6 -M 3
run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeStopFollowerLeader.py -N 5 -M 3
# duplicate with row 1195 run_case system-test python3 ./test.py -f 6-cluster/5dnode3mnodeStop2Follower.py -N 5 -M 3
run_case system-test python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_createDb_replica1.py -N 4 -M 1
run_case system-test python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica1_insertdatas.py -N 4 -M 1
run_case system-test python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica1_insertdatas_querys.py -N 4 -M 1
run_case system-test python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas.py -N 4 -M 1
run_case system-test python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_querys.py -N 4 -M 1
run_case system-test python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_vgroups.py -N 4 -M 1
run_case system-test python3 ./test.py -f 6-cluster/compactDBConflict.py -N 3
run_case system-test python3 ./test.py -f 6-cluster/mnodeEncrypt.py 3
run_case system-test python3 ./test.py -f 2-query/between.py -Q 2
run_case system-test python3 ./test.py -f 2-query/distinct.py -Q 2
run_case system-test python3 ./test.py -f 2-query/varchar.py -Q 2
run_case system-test python3 ./test.py -f 2-query/ltrim.py -Q 2
run_case system-test python3 ./test.py -f 2-query/rtrim.py -Q 2
run_case system-test python3 ./test.py -f 2-query/length.py -Q 2
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/char_length.py -Q 2
run_case system-test python3 ./test.py -f 2-query/upper.py -Q 2
run_case system-test python3 ./test.py -f 2-query/lower.py -Q 2
run_case system-test python3 ./test.py -f 2-query/join.py -Q 2
run_case system-test python3 ./test.py -f 2-query/join2.py -Q 2
run_case system-test python3 ./test.py -f 2-query/cast.py -Q 2
run_case system-test python3 ./test.py -f 2-query/substr.py -Q 2
run_case system-test python3 ./test.py -f 2-query/union.py -Q 2
run_case system-test python3 ./test.py -f 2-query/union1.py -Q 2
run_case system-test python3 ./test.py -f 2-query/concat.py -Q 2
run_case system-test python3 ./test.py -f 2-query/concat2.py -Q 2
run_case system-test python3 ./test.py -f 2-query/concat_ws.py -Q 2
run_case system-test python3 ./test.py -f 2-query/concat_ws2.py -Q 2
run_case system-test python3 ./test.py -f 2-query/check_tsdb.py -Q 2
run_case system-test python3 ./test.py -f 2-query/spread.py -Q 2
run_case system-test python3 ./test.py -f 2-query/hyperloglog.py -Q 2
run_case system-test python3 ./test.py -f 2-query/explain.py -Q 2
run_case system-test python3 ./test.py -f 2-query/leastsquares.py -Q 2
run_case system-test python3 ./test.py -f 2-query/timezone.py -Q 2
run_case system-test python3 ./test.py -f 2-query/Now.py -Q 2
run_case system-test python3 ./test.py -f 2-query/Today.py -Q 2
run_case system-test python3 ./test.py -f 2-query/max.py -Q 2
run_case system-test python3 ./test.py -f 2-query/min.py -Q 2
run_case system-test python3 ./test.py -f 2-query/normal.py -Q 2
run_case system-test python3 ./test.py -f 2-query/not.py -Q 2
run_case system-test python3 ./test.py -f 2-query/mode.py -Q 2
run_case system-test python3 ./test.py -f 2-query/count.py -Q 2
run_case system-test python3 ./test.py -f 2-query/countAlwaysReturnValue.py -Q 2
run_case system-test python3 ./test.py -f 2-query/last.py -Q 2
run_case system-test python3 ./test.py -f 2-query/first.py -Q 2
run_case system-test python3 ./test.py -f 2-query/To_iso8601.py -Q 2
run_case system-test python3 ./test.py -f 2-query/To_unixtimestamp.py -Q 2
run_case system-test python3 ./test.py -f 2-query/timetruncate.py -Q 2
run_case system-test python3 ./test.py -f 2-query/diff.py -Q 2
run_case system-test python3 ./test.py -f 2-query/Timediff.py -Q 2
run_case system-test python3 ./test.py -f 2-query/json_tag.py -Q 2
run_case system-test python3 ./test.py -f 2-query/top.py -Q 2
run_case system-test python3 ./test.py -f 2-query/bottom.py -Q 2
run_case system-test python3 ./test.py -f 2-query/percentile.py -Q 2
run_case system-test python3 ./test.py -f 2-query/apercentile.py -Q 2
run_case system-test python3 ./test.py -f 2-query/abs.py -Q 2
run_case system-test python3 ./test.py -f 2-query/ceil.py -Q 2
run_case system-test python3 ./test.py -f 2-query/floor.py -Q 2
run_case system-test python3 ./test.py -f 2-query/round.py -Q 2
run_case system-test python3 ./test.py -f 2-query/log.py -Q 2
run_case system-test python3 ./test.py -f 2-query/pow.py -Q 2
run_case system-test python3 ./test.py -f 2-query/sqrt.py -Q 2
run_case system-test python3 ./test.py -f 2-query/sin.py -Q 2
run_case system-test python3 ./test.py -f 2-query/cos.py -Q 2
run_case system-test python3 ./test.py -f 2-query/tan.py -Q 2
run_case system-test python3 ./test.py -f 2-query/arcsin.py -Q 2
run_case system-test python3 ./test.py -f 2-query/arccos.py -Q 2
run_case system-test python3 ./test.py -f 2-query/arctan.py -Q 2
run_case system-test python3 ./test.py -f 2-query/query_cols_tags_and_or.py -Q 2
run_case system-test python3 ./test.py -f 2-query/interp.py -Q 2
run_case system-test python3 ./test.py -f 2-query/fill.py -Q 2
run_case system-test python3 ./test.py -f 2-query/nestedQueryInterval.py -Q 2
run_case system-test python3 ./test.py -f 2-query/stablity.py -Q 2
run_case system-test python3 ./test.py -f 2-query/stablity_1.py -Q 2
run_case system-test python3 ./test.py -f 2-query/avg.py -Q 2
run_case system-test python3 ./test.py -f 2-query/elapsed.py -Q 2
run_case system-test python3 ./test.py -f 2-query/csum.py -Q 2
run_case system-test python3 ./test.py -f 2-query/mavg.py -Q 2
run_case system-test python3 ./test.py -f 2-query/sample.py -Q 2
run_case system-test python3 ./test.py -f 2-query/function_diff.py -Q 2
run_case system-test python3 ./test.py -f 2-query/unique.py -Q 2
run_case system-test python3 ./test.py -f 2-query/stateduration.py -Q 2
run_case system-test python3 ./test.py -f 2-query/function_stateduration.py -Q 2
run_case system-test python3 ./test.py -f 2-query/statecount.py -Q 2
run_case system-test python3 ./test.py -f 2-query/tail.py -Q 2
run_case system-test python3 ./test.py -f 2-query/ttl_comment.py -Q 2
run_case system-test python3 ./test.py -f 2-query/distribute_agg_count.py -Q 2
run_case system-test python3 ./test.py -f 2-query/distribute_agg_max.py -Q 2
run_case system-test python3 ./test.py -f 2-query/distribute_agg_min.py -Q 2
run_case system-test python3 ./test.py -f 2-query/distribute_agg_sum.py -Q 2
run_case system-test python3 ./test.py -f 2-query/distribute_agg_spread.py -Q 2
run_case system-test python3 ./test.py -f 2-query/distribute_agg_apercentile.py -Q 2
run_case system-test python3 ./test.py -f 2-query/distribute_agg_avg.py -Q 2
run_case system-test python3 ./test.py -f 2-query/distribute_agg_stddev.py -Q 2
run_case system-test python3 ./test.py -f 2-query/twa.py -Q 2
run_case system-test python3 ./test.py -f 2-query/irate.py -Q 2
run_case system-test python3 ./test.py -f 2-query/function_null.py -Q 2
run_case system-test python3 ./test.py -f 2-query/count_partition.py -Q 2
run_case system-test python3 ./test.py -f 2-query/max_partition.py -Q 2
run_case system-test python3 ./test.py -f 2-query/partition_limit_interval.py  -Q 2
run_case system-test python3 ./test.py -f 2-query/max_min_last_interval.py -Q 2
run_case system-test python3 ./test.py -f 2-query/last_row_interval.py -Q 2
run_case system-test python3 ./test.py -f 2-query/last_row.py -Q 2
run_case system-test python3 ./test.py -f 2-query/tsbsQuery.py -Q 2
run_case system-test python3 ./test.py -f 2-query/sml.py -Q 2
run_case system-test python3 ./test.py -f 2-query/case_when.py -Q 2
run_case system-test python3 ./test.py -f 2-query/blockSMA.py -Q 2
run_case system-test python3 ./test.py -f 2-query/projectionDesc.py -Q 2
run_case system-test python3 ./test.py -f 99-TDcase/TD-21561.py -Q 2
run_case system-test python3 ./test.py -f 2-query/between.py -Q 3
run_case system-test python3 ./test.py -f 2-query/distinct.py -Q 3
run_case system-test python3 ./test.py -f 2-query/varchar.py -Q 3
run_case system-test python3 ./test.py -f 2-query/ltrim.py -Q 3
run_case system-test python3 ./test.py -f 2-query/rtrim.py -Q 3
run_case system-test python3 ./test.py -f 2-query/length.py -Q 3
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/char_length.py -Q 3
run_case system-test python3 ./test.py -f 2-query/upper.py -Q 3
run_case system-test python3 ./test.py -f 2-query/lower.py -Q 3
run_case system-test python3 ./test.py -f 2-query/join.py -Q 3
run_case system-test python3 ./test.py -f 2-query/join2.py -Q 3
run_case system-test python3 ./test.py -f 2-query/cast.py -Q 3
run_case system-test python3 ./test.py -f 2-query/substr.py -Q 3
run_case system-test python3 ./test.py -f 2-query/union.py -Q 3
run_case system-test python3 ./test.py -f 2-query/union1.py -Q 3
run_case system-test python3 ./test.py -f 2-query/concat2.py -Q 3
run_case system-test python3 ./test.py -f 2-query/concat_ws.py -Q 3
run_case system-test python3 ./test.py -f 2-query/concat_ws2.py -Q 3
run_case system-test python3 ./test.py -f 2-query/check_tsdb.py -Q 3
run_case system-test python3 ./test.py -f 2-query/spread.py -Q 3
run_case system-test python3 ./test.py -f 2-query/hyperloglog.py -Q 3
run_case system-test python3 ./test.py -f 2-query/explain.py -Q 3
run_case system-test python3 ./test.py -f 2-query/leastsquares.py -Q 3
run_case system-test python3 ./test.py -f 2-query/timezone.py -Q 3
run_case system-test python3 ./test.py -f 2-query/Now.py -Q 3
run_case system-test python3 ./test.py -f 2-query/Today.py -Q 3
run_case system-test python3 ./test.py -f 2-query/max.py -Q 3
run_case system-test python3 ./test.py -f 2-query/min.py -Q 3
run_case system-test python3 ./test.py -f 2-query/normal.py -Q 3
run_case system-test python3 ./test.py -f 2-query/not.py -Q 3
run_case system-test python3 ./test.py -f 2-query/mode.py -Q 3
run_case system-test python3 ./test.py -f 2-query/count.py -Q 3
run_case system-test python3 ./test.py -f 2-query/countAlwaysReturnValue.py -Q 3
run_case system-test python3 ./test.py -f 2-query/last.py -Q 3
run_case system-test python3 ./test.py -f 2-query/first.py -Q 3
run_case system-test python3 ./test.py -f 2-query/To_iso8601.py -Q 3
run_case system-test python3 ./test.py -f 2-query/To_unixtimestamp.py -Q 3
run_case system-test python3 ./test.py -f 2-query/timetruncate.py -Q 3
run_case system-test python3 ./test.py -f 2-query/diff.py -Q 3
run_case system-test python3 ./test.py -f 2-query/Timediff.py -Q 3
run_case system-test python3 ./test.py -f 2-query/json_tag.py -Q 3
run_case system-test python3 ./test.py -f 2-query/top.py -Q 3
run_case system-test python3 ./test.py -f 2-query/bottom.py -Q 3
run_case system-test python3 ./test.py -f 2-query/percentile.py -Q 3
run_case system-test python3 ./test.py -f 2-query/apercentile.py -Q 3
run_case system-test python3 ./test.py -f 2-query/abs.py -Q 3
run_case system-test python3 ./test.py -f 2-query/ceil.py -Q 3
run_case system-test python3 ./test.py -f 2-query/floor.py -Q 3
run_case system-test python3 ./test.py -f 2-query/round.py -Q 3
run_case system-test python3 ./test.py -f 2-query/log.py -Q 3
run_case system-test python3 ./test.py -f 2-query/pow.py -Q 3
run_case system-test python3 ./test.py -f 2-query/sqrt.py -Q 3
run_case system-test python3 ./test.py -f 2-query/sin.py -Q 3
run_case system-test python3 ./test.py -f 2-query/cos.py -Q 3
run_case system-test python3 ./test.py -f 2-query/tan.py -Q 3
run_case system-test python3 ./test.py -f 2-query/arcsin.py -Q 3
run_case system-test python3 ./test.py -f 2-query/arccos.py -Q 3
run_case system-test python3 ./test.py -f 2-query/arctan.py -Q 3
run_case system-test python3 ./test.py -f 2-query/query_cols_tags_and_or.py -Q 3
run_case system-test python3 ./test.py -f 2-query/nestedQueryInterval.py -Q 3
run_case system-test python3 ./test.py -f 2-query/stablity.py -Q 3
run_case system-test python3 ./test.py -f 2-query/stablity_1.py -Q 3
run_case system-test python3 ./test.py -f 2-query/avg.py -Q 3
run_case system-test python3 ./test.py -f 2-query/elapsed.py -Q 3
run_case system-test python3 ./test.py -f 2-query/csum.py -Q 3
run_case system-test python3 ./test.py -f 2-query/mavg.py -Q 3
run_case system-test python3 ./test.py -f 2-query/sample.py -Q 3
run_case system-test python3 ./test.py -f 2-query/function_diff.py -Q 3
run_case system-test python3 ./test.py -f 2-query/unique.py -Q 3
run_case system-test python3 ./test.py -f 2-query/stateduration.py -Q 3
run_case system-test python3 ./test.py -f 2-query/function_stateduration.py -Q 3
run_case system-test python3 ./test.py -f 2-query/statecount.py -Q 3
run_case system-test python3 ./test.py -f 2-query/tail.py -Q 3
run_case system-test python3 ./test.py -f 2-query/ttl_comment.py -Q 3
run_case system-test python3 ./test.py -f 2-query/distribute_agg_count.py -Q 3
run_case system-test python3 ./test.py -f 2-query/distribute_agg_max.py -Q 3
run_case system-test python3 ./test.py -f 2-query/distribute_agg_min.py -Q 3
run_case system-test python3 ./test.py -f 2-query/distribute_agg_sum.py -Q 3
run_case system-test python3 ./test.py -f 2-query/distribute_agg_spread.py -Q 3
run_case system-test python3 ./test.py -f 2-query/distribute_agg_apercentile.py -Q 3
run_case system-test python3 ./test.py -f 2-query/distribute_agg_avg.py -Q 3
run_case system-test python3 ./test.py -f 2-query/distribute_agg_stddev.py -Q 3
run_case system-test python3 ./test.py -f 2-query/twa.py -Q 3
run_case system-test python3 ./test.py -f 2-query/irate.py -Q 3
run_case system-test python3 ./test.py -f 2-query/function_null.py -Q 3
run_case system-test python3 ./test.py -f 2-query/count_partition.py -Q 3
run_case system-test python3 ./test.py -f 2-query/max_partition.py -Q 3
run_case system-test python3 ./test.py -f 2-query/partition_limit_interval.py -Q 3
run_case system-test python3 ./test.py -f 2-query/max_min_last_interval.py -Q 3
run_case system-test python3 ./test.py -f 2-query/last_row_interval.py -Q 3
run_case system-test python3 ./test.py -f 2-query/last_row.py -Q 3
run_case system-test python3 ./test.py -f 2-query/tsbsQuery.py -Q 3
run_case system-test python3 ./test.py -f 2-query/sml.py -Q 3
run_case system-test python3 ./test.py -f 2-query/interp.py -Q 3
run_case system-test python3 ./test.py -f 2-query/fill.py -Q 3
run_case system-test python3 ./test.py -f 2-query/case_when.py -Q 3
run_case system-test python3 ./test.py -f 2-query/blockSMA.py -Q 3
run_case system-test python3 ./test.py -f 2-query/projectionDesc.py -Q 3
run_case system-test python3 ./test.py -f 99-TDcase/TD-21561.py -Q 3
run_case system-test python3 ./test.py -f 2-query/between.py -Q 4
run_case system-test python3 ./test.py -f 2-query/distinct.py -Q 4
run_case system-test python3 ./test.py -f 2-query/varchar.py -Q 4
run_case system-test python3 ./test.py -f 2-query/ltrim.py -Q 4
run_case system-test python3 ./test.py -f 2-query/rtrim.py -Q 4
run_case system-test python3 ./test.py -f 2-query/length.py -Q 4
# [AUTO-DISABLED 2026-04-19] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/char_length.py -Q 4
run_case system-test python3 ./test.py -f 2-query/upper.py -Q 4
run_case system-test python3 ./test.py -f 2-query/lower.py -Q 4
run_case system-test python3 ./test.py -f 2-query/join.py -Q 4
run_case system-test python3 ./test.py -f 2-query/join2.py -Q 4
run_case system-test python3 ./test.py -f 2-query/substr.py -Q 4
run_case system-test python3 ./test.py -f 2-query/union.py -Q 4
run_case system-test python3 ./test.py -f 2-query/union1.py -Q 4
run_case system-test python3 ./test.py -f 2-query/concat.py -Q 4
run_case system-test python3 ./test.py -f 2-query/concat2.py -Q 4
run_case system-test python3 ./test.py -f 2-query/concat_ws.py -Q 4
run_case system-test python3 ./test.py -f 2-query/concat_ws2.py -Q 4
run_case system-test python3 ./test.py -f 2-query/check_tsdb.py -Q 4
run_case system-test python3 ./test.py -f 2-query/spread.py -Q 4
run_case system-test python3 ./test.py -f 2-query/hyperloglog.py -Q 4
run_case system-test python3 ./test.py -f 2-query/explain.py -Q 4
run_case system-test python3 ./test.py -f 2-query/leastsquares.py -Q 4
run_case system-test python3 ./test.py -f 2-query/timezone.py -Q 4
run_case system-test python3 ./test.py -f 2-query/Now.py -Q 4
run_case system-test python3 ./test.py -f 2-query/Today.py -Q 4
run_case system-test python3 ./test.py -f 2-query/max.py -Q 4
run_case system-test python3 ./test.py -f 2-query/min.py -Q 4
run_case system-test python3 ./test.py -f 2-query/normal.py -Q 4
run_case system-test python3 ./test.py -f 2-query/not.py -Q 4
run_case system-test python3 ./test.py -f 2-query/mode.py -Q 4
run_case system-test python3 ./test.py -f 2-query/count.py -Q 4
run_case system-test python3 ./test.py -f 2-query/countAlwaysReturnValue.py -Q 4
run_case system-test python3 ./test.py -f 2-query/last.py -Q 4
run_case system-test python3 ./test.py -f 2-query/first.py -Q 4
run_case system-test python3 ./test.py -f 2-query/To_iso8601.py -Q 4
run_case system-test python3 ./test.py -f 2-query/To_unixtimestamp.py -Q 4
run_case system-test python3 ./test.py -f 2-query/timetruncate.py -Q 4
run_case system-test python3 ./test.py -f 2-query/diff.py -Q 4
run_case system-test python3 ./test.py -f 2-query/Timediff.py -Q 4
run_case system-test python3 ./test.py -f 2-query/json_tag.py -Q 4
run_case system-test python3 ./test.py -f 2-query/top.py -Q 4
run_case system-test python3 ./test.py -f 2-query/bottom.py -Q 4
run_case system-test python3 ./test.py -f 2-query/percentile.py -Q 4
run_case system-test python3 ./test.py -f 2-query/apercentile.py -Q 4
run_case system-test python3 ./test.py -f 2-query/abs.py -Q 4
run_case system-test python3 ./test.py -f 2-query/ceil.py -Q 4
run_case system-test python3 ./test.py -f 2-query/floor.py -Q 4
run_case system-test python3 ./test.py -f 2-query/round.py -Q 4
run_case system-test python3 ./test.py -f 2-query/log.py -Q 4
run_case system-test python3 ./test.py -f 2-query/pow.py -Q 4
run_case system-test python3 ./test.py -f 2-query/sqrt.py -Q 4
run_case system-test python3 ./test.py -f 2-query/sin.py -Q 4
run_case system-test python3 ./test.py -f 2-query/cos.py -Q 4
run_case system-test python3 ./test.py -f 2-query/tan.py -Q 4
run_case system-test python3 ./test.py -f 2-query/arcsin.py -Q 4
run_case system-test python3 ./test.py -f 2-query/arccos.py -Q 4
run_case system-test python3 ./test.py -f 2-query/arctan.py -Q 4
run_case system-test python3 ./test.py -f 2-query/query_cols_tags_and_or.py -Q 4
run_case system-test python3 ./test.py -f 2-query/nestedQueryInterval.py -Q 4
run_case system-test python3 ./test.py -f 2-query/stablity.py -Q 4
run_case system-test python3 ./test.py -f 2-query/stablity_1.py -Q 4
run_case system-test python3 ./test.py -f 2-query/avg.py -Q 4
run_case system-test python3 ./test.py -f 2-query/elapsed.py -Q 4
run_case system-test python3 ./test.py -f 2-query/csum.py -Q 4
run_case system-test python3 ./test.py -f 2-query/mavg.py -Q 4
run_case system-test python3 ./test.py -f 2-query/sample.py -Q 4
run_case system-test python3 ./test.py -f 2-query/cast.py -Q 4
run_case system-test python3 ./test.py -f 2-query/function_diff.py -Q 4
run_case system-test python3 ./test.py -f 2-query/unique.py -Q 4
run_case system-test python3 ./test.py -f 2-query/tail.py -Q 4
run_case system-test python3 ./test.py -f 2-query/ttl_comment.py -Q 4
run_case system-test python3 ./test.py -f 2-query/distribute_agg_count.py -Q 4
run_case system-test python3 ./test.py -f 2-query/distribute_agg_max.py -Q 4
run_case system-test python3 ./test.py -f 2-query/distribute_agg_min.py -Q 4
run_case system-test python3 ./test.py -f 2-query/distribute_agg_sum.py -Q 4
run_case system-test python3 ./test.py -f 2-query/distribute_agg_spread.py -Q 4
run_case system-test python3 ./test.py -f 2-query/distribute_agg_apercentile.py -Q 4
run_case system-test python3 ./test.py -f 2-query/distribute_agg_avg.py -Q 4
run_case system-test python3 ./test.py -f 2-query/distribute_agg_stddev.py -Q 4
run_case system-test python3 ./test.py -f 2-query/twa.py -Q 4
run_case system-test python3 ./test.py -f 2-query/irate.py -Q 4
run_case system-test python3 ./test.py -f 2-query/function_null.py -Q 4
run_case system-test python3 ./test.py -f 2-query/count_partition.py -Q 4
run_case system-test python3 ./test.py -f 2-query/max_partition.py -Q 4
run_case system-test python3 ./test.py -f 2-query/partition_limit_interval.py -Q 4
run_case system-test python3 ./test.py -f 2-query/max_min_last_interval.py -Q 4
run_case system-test python3 ./test.py -f 2-query/last_row_interval.py -Q 4
run_case system-test python3 ./test.py -f 2-query/last_row.py -Q 4
run_case system-test python3 ./test.py -f 2-query/tsbsQuery.py -Q 4
run_case system-test python3 ./test.py -f 2-query/sml.py -Q 4
run_case system-test python3 ./test.py -f 2-query/interp.py -Q 4
run_case system-test python3 ./test.py -f 2-query/fill.py -Q 4
run_case system-test python3 ./test.py -f 2-query/case_when.py -Q 4
run_case system-test python3 ./test.py -f 2-query/insert_select.py
run_case system-test python3 ./test.py -f 2-query/insert_select.py -R
run_case system-test python3 ./test.py -f 2-query/insert_select.py -Q 2
run_case system-test python3 ./test.py -f 2-query/insert_select.py -Q 3
run_case system-test python3 ./test.py -f 2-query/insert_select.py -Q 4
run_case system-test python3 ./test.py -f 2-query/out_of_order.py -R
run_case system-test python3 ./test.py -f 2-query/blockSMA.py -Q 4
run_case system-test python3 ./test.py -f 2-query/projectionDesc.py -Q 4
run_case system-test python3 ./test.py -f 2-query/odbc.py
run_case system-test python3 ./test.py -f 2-query/fill_with_group.py
run_case system-test python3 ./test.py -f 2-query/state_window.py -Q 3
# [AUTO-DISABLED 2026-04-20] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/cols_function.py
# [AUTO-DISABLED 2026-04-20] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/cols_function.py -Q 2
# [AUTO-DISABLED 2026-04-20] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/cols_function.py -Q 3
# [AUTO-DISABLED 2026-04-20] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/cols_function.py -Q 4
run_case system-test python3 ./test.py -f 99-TDcase/TD-21561.py -Q 4
run_case system-test python3 ./test.py -f 99-TDcase/TD-20582.py
run_case system-test python3 ./test.py -f eco-system/meta/database/keep_time_offset.py
run_case system-test python3 ./test.py -f 2-query/operator.py
run_case system-test python3 ./test.py -f 2-query/operator.py -Q 2
run_case system-test python3 ./test.py -f 2-query/operator.py -Q 3
run_case system-test python3 ./test.py -f 2-query/operator.py -Q 4
run_case system-test python3 ./test.py -f eco-system/manager/schema_change.py -N 3 -M 3
# [AUTO-DISABLED 2026-04-20] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 2-query/test_window_true_for.py
# [AUTO-DISABLED 2026-04-20] no-fix retries (3) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-20] no-fix retries (1) exhausted after 0 attempts
# # [AUTO-DISABLED 2026-04-21] no-fix retries (1) exhausted after 0 attempts
# run_case system-test python3 ./test.py -f 0-others/compatibility.py
run_case develop-test python3 ./test.py -f 2-query/table_count_scan.py
run_case develop-test python3 ./test.py -f 2-query/pseudo_column.py
run_case develop-test python3 ./test.py -f 2-query/ts-range.py
run_case develop-test python3 ./test.py -f 2-query/tag_scan.py
run_case develop-test python3 ./test.py -f 2-query/show_create_db.py
run_case test_new python3 ./test.py -f storage/compact/test_compact_meta.py

echo "" | tee -a "$LOG"
echo "=== riscvtest done at $(date '+%Y-%m-%d %H:%M:%S') ===" | tee -a "$LOG"
echo "Total: $TOTAL  PASS: $PASS  FAIL: $FAIL  (TIMEOUT: $TIMEOUT_COUNT)" | tee -a "$LOG"
