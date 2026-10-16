#!/usr/bin/env bash

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set -euo pipefail

if (($# != 4)); then
  printf 'Usage: %s PACKAGE_SCRIPT TOOL_BINARY FAKE_NATIVE_DIRECTORY WORK_DIRECTORY\n' "$0" >&2
  exit 2
fi

package_script=$1
tool_binary=$2
fake_native_directory=$3
work_directory=$4
archive="$work_directory/taosQueryTest-test.tar.gz"
unpack_directory="$work_directory/unpacked"
package_root="$unpack_directory/taosQueryTest-test-linux-testarch"

rm -rf -- "$work_directory"
mkdir -p -- "$work_directory" "$unpack_directory"

"$package_script" --binary "$tool_binary" --output "$archive" \
  --version test --arch testarch
test -f "$archive"
tar -xzf "$archive" -C "$unpack_directory"
test -d "$package_root"
test -x "$package_root/taosQueryTest"
test -x "$package_root/taosQueryTest.bin"
test -x "$package_root/run-audited-round.py"
test -x "$package_root/run-endurance-matrix.py"
test -f "$package_root/qst-runtime-env.sh"
test -f "$package_root/qst-external-env.sh"
test -x "$package_root/prepare-test-machine.sh"
test -f "$package_root/taosQueryTest.yaml"
test -f "$package_root/dependency-lock.json"
test -f "$package_root/dataset/taosbenchmark.json"
test -f "$package_root/prompts/result_judgment.md"
test -f "$package_root/PACKAGE-METADATA"
test -f "$package_root/SHA256SUMS"
test ! -e "$package_root/bin"
test ! -e "$package_root/lib"
test ! -e "$package_root/latest_run_id"
test ! -e "$package_root/latest_run_id.lock"
test ! -e "$package_root/prompts/submit_feishu_bug.md"
test ! -e "$package_root/prompts/resolve_problem.md"
test -f "$fake_native_directory/libtaos.so"

runtime_test_dir=$(mktemp -d "${TMPDIR:-/tmp}/taosQueryTest-runtime-port-test-XXXXXX")
trap 'rm -rf -- "$runtime_test_dir"' EXIT
cat >"$runtime_test_dir/fake-taosd" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

config_dir=
while (($# > 0)); do
  if [[ "$1" == -c ]]; then
    config_dir=$2
    shift 2
    continue
  fi
  shift
done
port=$(awk '/^serverPort / { print $2; exit }' "$config_dir/taos.cfg")
printf '%s\n' "$port" >>"$QST_RUNTIME_PORT_TEST_LOG"
if [[ ! -e "$QST_RUNTIME_PORT_TEST_RETRY" ]]; then
  : >"$QST_RUNTIME_PORT_TEST_RETRY"
  exit 1
fi
exec python3 - "$port" <<'PY'
import signal
import socket
import sys

listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.bind(("127.0.0.1", int(sys.argv[1])))
listener.listen()
signal.pause()
PY
EOF
chmod 0755 "$runtime_test_dir/fake-taosd"
(
  export QST_RESULT_STORE_RUNTIME_DIR="$runtime_test_dir/result-store"
  source "$package_root/qst-runtime-env.sh"
  source "$package_root/qst-external-env.sh"
  blocked_fd=
  cleanup_runtime_port_test() {
    qst_external_cleanup || true
    qst_runtime_cleanup || true
    if [[ -n "$blocked_fd" ]]; then
      flock -u "$blocked_fd" || true
      eval "exec ${blocked_fd}>&-"
    fi
  }
  trap cleanup_runtime_port_test EXIT
  export QST_RUNTIME_PORT_TEST_LOG="$runtime_test_dir/ports.log"
  export QST_RUNTIME_PORT_TEST_RETRY="$runtime_test_dir/retry"
  export QST_RUNTIME_TEMP_ROOT="$runtime_test_dir/runtime-tmp"
  export QST_PORT_LEASE_DIR="$runtime_test_dir/port-leases"
  QST_RESOLVED_TAOSD="$runtime_test_dir/fake-taosd"
  runtime_seed=
  for candidate_seed in $(seq 1 100); do
    RANDOM=$candidate_seed
    blocked_port=$((20000 + RANDOM % 20000))
    if ! ss -ltn "sport = :$blocked_port" 2>/dev/null | grep -q LISTEN; then
      runtime_seed=$candidate_seed
      break
    fi
  done
  test -n "$runtime_seed"
  mkdir -p "$(qst_port_lease_directory)"
  exec {blocked_fd}>"$(qst_port_lease_directory)/port-$blocked_port.lock"
  flock -n "$blocked_fd"
  RANDOM=$runtime_seed
  qst_runtime_start_private
  test -n "$QST_RUNTIME_PORT_LEASE_FD"
  test -n "$TAOS_PORT"
  test "$(wc -l <"$QST_RUNTIME_PORT_TEST_LOG")" -eq 2
  ! grep -Fx -- "$blocked_port" "$QST_RUNTIME_PORT_TEST_LOG"
  flock -u "$blocked_fd"
  eval "exec ${blocked_fd}>&-"
  if (
    source "$package_root/qst-runtime-env.sh"
    qst_port_lease_acquire "$TAOS_PORT"
  ); then
    printf 'expected active private TDengine port lease to reject a peer\n' >&2
    exit 1
  fi
  qst_runtime_cleanup
  test -z "$QST_RUNTIME_PORT_LEASE_FD"
  qst_port_lease_acquire "$TAOS_PORT"
  qst_port_lease_release "$QST_PORT_LEASE_FD"
  qst_runtime_start_result_store
  test -f "$QST_RESULT_STORE_RUNTIME_DIR/cfg/taos.cfg"
  test -n "$QST_RESULT_STORE_CONFIG_DIR"
  test -n "$QST_RESULT_STORE_ENDPOINT"
  test -n "$QST_RESULT_STORE_TAOSD_PID"
  kill -0 "$QST_RESULT_STORE_TAOSD_PID"
  qst_runtime_result_store_cleanup
  test -d "$QST_RESULT_STORE_RUNTIME_DIR"
  test -f "$QST_RESULT_STORE_RUNTIME_DIR/cfg/taos.cfg"
  qst_external_acquire_port
  test -n "$QST_EXTERNAL_PORT"
  test ${#QST_EXTERNAL_PORT_LEASE_FDS[@]} -eq 1
  qst_external_cleanup
  test ${#QST_EXTERNAL_PORT_LEASE_FDS[@]} -eq 0
  trap - EXIT
)
rm -rf -- "$runtime_test_dir"
trap - EXIT

if "$package_root/run-audited-round.py" --mode random --providers td --round 1 \
    --minutes 19 >"$work_directory/audit-invalid.txt" 2>&1; then
  printf 'expected audited runner to reject a duration below 20 minutes\n' >&2
  exit 1
fi
grep -F -- '--minutes must be at least 20' "$work_directory/audit-invalid.txt"

reset_runtime_dir=$(mktemp -d "${TMPDIR:-/tmp}/taosQueryTest-reset-runtime-XXXXXX")
trap 'rm -rf -- "$reset_runtime_dir"' EXIT
mkdir -p -- "$reset_runtime_dir/bin" "$reset_runtime_dir/lib" \
  "$reset_runtime_dir/result-store"
cp -- "$fake_native_directory/libtaos.so" "$reset_runtime_dir/lib/libtaos.so"
printf '%s\n' '#!/usr/bin/env bash' 'exit 99' \
  >"$reset_runtime_dir/bin/taosd"
chmod 0755 "$reset_runtime_dir/bin/taosd"
printf 'serverPort 16036\n' >"$reset_runtime_dir/result-store/taos.cfg"
QST_TDENGINE_HOME="$reset_runtime_dir" \
QST_RESULT_STORE_CONFIG_DIR="$reset_runtime_dir/result-store" \
QST_RESULT_STORE_ENDPOINT=127.0.0.1:16036 \
QST_RESULT_STORE_ALLOW_RESET=1 \
"$package_root/taosQueryTest" --reset-result-store 99 \
  >"$work_directory/result-store-reset.json" \
  2>"$work_directory/result-store-reset.err"
grep -F -- '"run_id":"99"' "$work_directory/result-store-reset.json"
grep -F -- '"reset":true' "$work_directory/result-store-reset.json"
test ! -s "$work_directory/result-store-reset.err"
rm -rf -- "$reset_runtime_dir"
trap - EXIT

python3 - "$package_root/run-audited-round.py" <<'PY'
import importlib.util
import copy
import sys

spec = importlib.util.spec_from_file_location("qst_audited_round", sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
issues = module.collect_product_issue_samples("normal log\n", ["/tmp/core.1"])
assert issues == ["new core file: /tmp/core.1"]
issues = module.collect_product_issue_samples("Segmentation fault\n", [])
assert issues == ["Segmentation fault"]
module.os.environ["QST_RESULT_STORE_CONFIG_DIR"] = "/tmp/qst-result-store"
module.os.environ["QST_RESULT_STORE_ENDPOINT"] = "127.0.0.1:16036"
module.os.environ["QST_TDENGINE_HOME"] = "/tmp/qst-release"
module.os.environ["QST_PROFILE_PHASES"] = "1"
module.os.environ["TAOS_PORT"] = "16036"
environment = module.build_execution_environment()
assert environment["QST_RESULT_STORE_CONFIG_DIR"] == "/tmp/qst-result-store"
assert environment["QST_RESULT_STORE_ENDPOINT"] == "127.0.0.1:16036"
assert environment["QST_TDENGINE_HOME"] == "/tmp/qst-release"
assert environment["QST_PROFILE_PHASES"] == "1"
assert "TAOS_PORT" not in environment

import pathlib
import tempfile
cell = pathlib.Path(tempfile.mkdtemp(prefix="qst-audited-cell-"))
try:
    module.os.environ.pop("QST_RESULT_STORE_CONFIG_DIR")
    module.os.environ.pop("QST_RESULT_STORE_ENDPOINT")
    isolated = module.prepare_execution_environment(cell)
    assert isolated["TMPDIR"] == str(cell / "runtime-tmp")
    assert isolated["QST_RUNTIME_TEMP_ROOT"] == str(cell / "runtime-tmp")
    assert isolated["QST_PORT_LEASE_DIR"] == str(cell / "runtime-tmp" / "port-leases")
    assert isolated["QST_RESULT_STORE_RUNTIME_DIR"] == str(cell / "result-store-runtime")
    assert (cell / "runtime-tmp").is_dir()
    assert (cell / "result-store-runtime").is_dir()
    assert module.cleanup_result_store_runtime(isolated)
    assert not (cell / "result-store-runtime").exists()
    assert module.cleanup_result_store_runtime(isolated)
finally:
    import shutil
    shutil.rmtree(cell)

profile = {"throughput": {"warmup_window_ns": 10000000000,
                            "sample_window_ns": 1000000000,
                            "persisted_qps_sample_count": 20,
                            "minimum_persisted_qps": 20000.0}}
valid, reasons, details = module.validate_sustained_qps(profile, 20000.0)
assert valid and not reasons and details["minimum_sample_qps"] == 20000.0
profile["throughput"]["minimum_persisted_qps"] = 19999.99
valid, reasons, _ = module.validate_sustained_qps(profile, 20000.0)
assert not valid and reasons == [
    "minimum sustained QPS 19999.99 is below required 20000.00"]
valid, reasons, _ = module.validate_sustained_qps({}, 20000.0)
assert not valid and reasons == ["stage profile has no sustained throughput evidence"]

count = {"ok": True, "result": {"format": "query-result-v1",
         "rows": [[{"kind": "int64", "value": "10"}]]}}
metadata = {"ok": True, "result": {"format": "query-result-v1", "rows": [
    [{"kind": "int8", "value": "1"}, {"kind": "int8", "value": "1"},
     {"kind": "int8", "value": "0"}, {"kind": "int8", "value": "1"},
     {"kind": "int64", "value": "10"}]
]}}
audit = {"run_id": "42", "complete": True, "run_record_count": {
    "ok": True, "result": {"format": "query-result-v1", "rows": [
        [{"kind": "int64", "value": "1"}]
    ]}}, "targets": {"td": {"statement_count": count, "result_count": count,
                              "generation_metadata": metadata,
                              "execution_status": {"ok": True, "result": {
                                  "format": "query-result-v1", "rows": [[
                                      {"kind": "int8", "value": "1"},
                                      {"kind": "int64", "value": "10"}
                                  ]]}},
                              "sql_shape": {
                                  "select": count, "from": count, "where": count,
                                  "aggregate": count}}}}
valid, reasons, target_counts = module.validate_result_store_audit(
    audit, "42", "td", 10, "coverage")
assert valid and not reasons and target_counts["td"]["generation_metadata_count"] == 10
assert target_counts["td"]["expected_execution"]["expected_outcome_ratio"] == 1.0
assert target_counts["td"]["expected_execution"]["abnormal_error_ratio"] == 1.0
missing_shape_audit = copy.deepcopy(audit)
del missing_shape_audit["targets"]["td"]["sql_shape"]["aggregate"]
valid, reasons, _ = module.validate_result_store_audit(
    missing_shape_audit, "42", "td", 10, "coverage")
assert not valid and reasons == ["td: sql shape aggregate is missing or zero"]
external_audit = copy.deepcopy(audit)
external_target = external_audit["targets"].pop("td")
external_target["sql_shape"]["external"] = count
external_target["source_reference_count"] = count
external_audit["targets"]["mysql"] = external_target
valid, reasons, target_counts = module.validate_result_store_audit(
    external_audit, "42", "mysql", 10, "coverage")
assert valid and not reasons and target_counts["mysql"]["source_reference_count"] == 10
external_audit["targets"]["mysql"]["source_reference_count"] = {
    "ok": True, "result": {"format": "query-result-v1", "rows": [[
        {"kind": "int64", "value": "0"}
    ]]}}
valid, reasons, _ = module.validate_result_store_audit(
    external_audit, "42", "mysql", 10, "coverage")
assert not valid and reasons == ["mysql: provider source reference is missing or zero"]
audit["targets"]["td"]["result_count"] = {"ok": True, "result": {
    "format": "query-result-v1", "rows": [[
        {"kind": "int64", "value": "0"}
    ]]}}
audit["targets"]["td"]["execution_status"] = {"ok": True, "result": {
    "format": "query-result-v1", "rows": [[
        {"kind": "int8", "value": "1"}, {"kind": "int64", "value": "10"}
    ]]}}
valid, reasons, target_counts = module.validate_result_store_audit(
    audit, "42", "td", 10, "coverage")
assert valid and not reasons and target_counts["td"]["execution_status_counts"] == {1: 10}
valid, reasons, _ = module.validate_result_store_audit(audit, "42", "td", 10, "correctness")
assert not valid and reasons == [
    "td: correctness result count does not match statement count",
    "result-store result total does not match report persisted tasks",
]
mixed_metadata = {"ok": True, "result": {"format": "query-result-v1", "rows": [
    [{"kind": "int8", "value": "1"}, {"kind": "int8", "value": "0"},
     {"kind": "int8", "value": "0"}, {"kind": "int8", "value": "0"},
     {"kind": "int64", "value": "9"}],
    [{"kind": "int8", "value": "1"}, {"kind": "int8", "value": "1"},
     {"kind": "int8", "value": "0"}, {"kind": "int8", "value": "1"},
     {"kind": "int64", "value": "1"}],
]}}
expected = module.analyze_expected_execution(mixed_metadata["result"]["rows"])
assert expected["expected_outcome_count"] == 10
assert expected["normal_success_ratio"] == 1.0
assert expected["abnormal_error_ratio"] == 1.0
mixed_metadata["result"]["rows"][0][3]["value"] = "1"
expected = module.analyze_expected_execution(mixed_metadata["result"]["rows"])
assert expected["expected_outcome_count"] == 1
assert expected["expected_outcome_ratio"] == 0.1
assert expected["normal_success_ratio"] == 0.0
audit["targets"]["td"]["unique_sql_sha256_count"] = count
audit["targets"]["td"]["sql_shape"] = {"select": count, "where": count}
audit["targets"]["td"]["sql_fingerprint_top"] = {"ok": True, "result": {
    "format": "query-result-v1", "rows": [[
        {"kind": "varchar", "value": "abc"}, {"kind": "int64", "value": "4"}
    ]]}}
audit["targets"]["td"]["sql_samples"] = {"ok": True, "result": {
    "format": "query-result-v1", "rows": [[
        {"kind": "varchar", "value": "abc"},
        {"kind": "varchar", "value": "SELECT * FROM t"}
    ]]}}
distribution = module.analyze_sql_distribution(audit, "td")["targets"]["td"]
assert distribution["unique_sql_ratio"] == 1.0
assert distribution["sql_shape_percent"] == {"select": 100.0, "where": 100.0}
assert distribution["top_fingerprint_percent"] == 40.0
assert distribution["sql_samples"][0]["sql_text"] == "SELECT * FROM t"
PY

if "$package_root/run-endurance-matrix.py" --rounds 0 --focus correctness \
    >"$work_directory/matrix-invalid.txt" 2>&1; then
  printf 'expected endurance matrix runner to reject non-positive rounds\n' >&2
  exit 1
fi
grep -F -- '--rounds must be positive' "$work_directory/matrix-invalid.txt"

(
  source "$package_root/qst-external-env.sh"
  external_secret=$(qst_external_secret)
  test ${#external_secret} -eq 32
  case "$external_secret" in
    *[!A-Za-z0-9]*) exit 1 ;;
  esac
  mysql_external_secret=$(qst_external_secret 15)
  test ${#mysql_external_secret} -eq 30
  case "$mysql_external_secret" in
    *[!A-Za-z0-9]*) exit 1 ;;
  esac
  qst_external_mysql() { return 1; }
  export FQ_MYSQL_PORT=3306
  if qst_external_prepare mysql; then
    printf 'expected failed self-hosted MySQL preparation\n' >&2
    exit 1
  fi
  test -z "${FQ_MYSQL_PORT+x}"
)

external_diagnostic_dir=$(mktemp -d "${TMPDIR:-/tmp}/taosQueryTest-influx-diagnostic-XXXXXX")
trap 'rm -rf -- "$external_diagnostic_dir"' EXIT
printf '%s\n' '#!/usr/bin/env bash' 'exec tail -f /dev/null' \
  >"$external_diagnostic_dir/fake-influxdb3"
chmod 0755 "$external_diagnostic_dir/fake-influxdb3"
(
  source "$package_root/qst-runtime-env.sh"
  source "$package_root/qst-external-env.sh"
  QST_EXTERNAL_ENV_DIR="$external_diagnostic_dir/environment"
  qst_find_client() { printf '%s\n' "$external_diagnostic_dir/fake-influxdb3"; }
  qst_external_acquire_port() {
    QST_EXTERNAL_PORT=20999
    QST_EXTERNAL_PORT_LEASE_FDS=()
  }
  curl() { return 7; }
  export QST_EXTERNAL_HEALTH_ATTEMPTS=1
  if qst_external_influx >"$external_diagnostic_dir/stdout" 2>"$external_diagnostic_dir/stderr"; then
    printf 'expected failed self-hosted InfluxDB health preparation\n' >&2
    exit 1
  fi
  grep -F -- 'stage=health' "$external_diagnostic_dir/stderr"
  grep -F -- "binary=$external_diagnostic_dir/fake-influxdb3" \
    "$external_diagnostic_dir/stderr"
  grep -F -- 'port=20999' "$external_diagnostic_dir/stderr"
  grep -F -- 'health_url=http://127.0.0.1:20999/health health_exit=7' \
    "$external_diagnostic_dir/stderr"
  qst_external_cleanup
)
rm -rf -- "$external_diagnostic_dir"
trap - EXIT

(
  cd -- "$package_root"
  sha256sum -c SHA256SUMS
)
mkdir -p -- "$work_directory/other-cwd"
(
  cd -- "$work_directory/other-cwd"
  "$package_root/taosQueryTest" --help > "$work_directory/help.txt"
)
grep -F -- "Usage: $package_root/taosQueryTest" "$work_directory/help.txt"
(
  cd -- "$work_directory/other-cwd"
  env -i PATH=/usr/bin:/bin "$package_root/prepare-test-machine.sh" --targets td \
    > "$work_directory/preflight.txt"
)
grep -F -- 'OK   taosd' "$work_directory/preflight.txt"
grep -F -- 'OK   libtaos.so' "$work_directory/preflight.txt"

explicit_result_store="$work_directory/explicit-result-store"
mkdir -p -- "$explicit_result_store"
printf 'serverPort 16036\n' > "$explicit_result_store/taos.cfg"
(
  source "$package_root/qst-runtime-env.sh"
  export QST_RESULT_STORE_CONFIG_DIR="$explicit_result_store"
  qst_runtime_resolve_config
  test "$TAOS_CONFIG_DIR" = "$explicit_result_store"
)

generate_test_dir=$(mktemp -d "${TMPDIR:-/tmp}/taosQueryTest-generate-only-XXXXXX")
trap 'rm -rf -- "$generate_test_dir"' EXIT
mkdir -p -- "$generate_test_dir/runtime/bin" "$generate_test_dir/runtime/lib" \
  "$generate_test_dir/result-store"
cp -- "$fake_native_directory/libtaos.so" "$generate_test_dir/runtime/lib/libtaos.so"
cat >"$generate_test_dir/runtime/bin/taosd" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >>"${QST_GENERATE_TAOSD_LOG:?}"
exit 1
EOF
chmod 0755 "$generate_test_dir/runtime/bin/taosd"
printf 'serverPort 16036\n' > "$generate_test_dir/result-store/taos.cfg"
# Pin a seed in the packaged configuration so JSONL equality has a frozen
# input contract. A later run changes only this value to check divergence.
python3 - "$package_root/taosQueryTest.yaml" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
assert "  # random_seed: 20260902\n" in text
path.write_text(text.replace("  # random_seed: 20260902\n", "  random_seed: 20260902\n"))
PY
generate_sql_a="$generate_test_dir/random-a.jsonl"
generate_sql_b="$generate_test_dir/random-b.jsonl"
generate_sql_c="$generate_test_dir/random-c.jsonl"
generate_native_log="$generate_test_dir/native.sql"
generate_taosd_log="$generate_test_dir/taosd.argv"
export QST_TDENGINE_HOME="$generate_test_dir/runtime"
export QST_RESULT_STORE_CONFIG_DIR="$generate_test_dir/result-store"
export QST_RESULT_STORE_ENDPOINT='127.0.0.1:16036'
export QST_GENERATE_TAOSD_LOG="$generate_taosd_log"
export QST_FAKE_TAOS_SQL_LOG="$generate_native_log"
"$package_root/taosQueryTest" -t random -n 128 -g "$generate_sql_a"
"$package_root/taosQueryTest" -t random -n 128 -g "$generate_sql_b"
cmp -- "$generate_sql_a" "$generate_sql_b"
if "$package_root/taosQueryTest" -t random -n 128 -g "$generate_sql_a" \
    >"$generate_test_dir/overwrite.txt" 2>&1; then
  printf 'expected -g to reject an existing SQL evidence file\n' >&2
  exit 1
fi
cmp -- "$generate_sql_a" "$generate_sql_b"
python3 - "$package_root/taosQueryTest.yaml" "$generate_sql_a" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
assert "  random_seed: 20260902\n" in text
path.write_text(text.replace("  random_seed: 20260902\n", "  random_seed: 20260903\n"))
records = [__import__("json").loads(line) for line in pathlib.Path(sys.argv[2]).read_text().splitlines()]
assert len(records) == 128
assert all(record["format"] == "taosQueryTest-generated-sql-v1" for record in records)
assert all(record["mode"] == "random" and record["target"] == "td" for record in records)
assert all(record["dialect"] == 1 and record["random_seed"] == 20260902 for record in records)
assert all(record["generation_seq"] == index + 1 for index, record in enumerate(records))
assert all(isinstance(record["generation_abnormal"], bool) for record in records)
assert all(isinstance(record["generation_unsupported_fallback"], bool) for record in records)
assert all(len(record["sql_sha256"]) == 64 and record["sql_text"] for record in records)
assert len({record["sql_sha256"] for record in records}) > 1
PY
"$package_root/taosQueryTest" -t random -n 128 -g "$generate_sql_c"
if cmp -s -- "$generate_sql_a" "$generate_sql_c"; then
  printf 'expected a changed random_seed to alter finite JSONL output\n' >&2
  exit 1
fi
! grep -F -- '-c ' "$generate_taosd_log"
! grep -F -- 'res_db.' "$generate_native_log"
latest_generate_report=$(python3 - "$package_root/results" <<'PY'
import pathlib
import sys

reports = sorted(pathlib.Path(sys.argv[1]).glob("*/dataset/generation-debug.json"))
assert reports
print(reports[-1])
PY
)
test -n "$latest_generate_report"
grep -F -- '"generated_task_count":0' "$latest_generate_report"
grep -F -- '"execution_pairs_started":0' "$latest_generate_report"
grep -F -- '"result_store_ledger":false' "$latest_generate_report"
unset QST_FAKE_TAOS_SQL_LOG QST_GENERATE_TAOSD_LOG QST_RESULT_STORE_ENDPOINT \
  QST_RESULT_STORE_CONFIG_DIR QST_TDENGINE_HOME
rm -rf -- "$generate_test_dir"
trap - EXIT

assert_package_root() {
  local launch_name=$1
  local output_path=$2

  if (
    cd -- "$work_directory/other-cwd"
    "$launch_name" -v 1 >"$output_path" 2>&1
  ); then
    printf 'expected manual AI entry to reject unknown run\n' >&2
    exit 1
  fi
  grep -F -- "taosQueryTest: run failed" "$output_path"
  ! grep -F -- "cannot open configuration" "$output_path"
  ! grep -F -- "cannot resolve the pre-deployed result-store" "$output_path"
  ! grep -F -- "managed result-store preparation failed" "$output_path"
}

assert_package_root "$package_root/taosQueryTest" "$work_directory/absolute-root.txt"
(
  export PATH="$package_root:$PATH"
  assert_package_root taosQueryTest "$work_directory/path-root.txt"
)
ln -s -- "$package_root/taosQueryTest" "$work_directory/taosQueryTest-link"
assert_package_root "$work_directory/taosQueryTest-link" "$work_directory/symlink-root.txt"
