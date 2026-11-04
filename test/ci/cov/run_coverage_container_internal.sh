#!/bin/bash

# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

function usage() {
    echo "$0"
    echo -e "\t -w work dir"
    echo -e "\t -d execution dir"
    echo -e "\t -c command"
    echo -e "\t -t thread number"
    echo -e "\t -e enterprise edition"
    echo -e "\t -o default timeout value"
    echo -e "\t -s build with sanitizer"
    echo -e "\t -h help"
}

# 新增：在容器内生成覆盖率信息的函数
function generate_coverage_script() {
    local workdir="$1"
    local thread_no="$2"
    local case_name="$3"
    local container_testdir="$4"
    local debugpath="$5"
    
    # 创建覆盖率生成脚本
    local coverage_script="${TMP_DIR}/thread_volume/${thread_no}/generate_coverage.sh"
    
    cat > "$coverage_script" << 'EOF'
#!/bin/bash

# 接收参数
WORKDIR="$1"
THREAD_NO="$2"
CASE_NAME="$3"
CONTAINER_TESTDIR="$4"
DEBUGPATH="$5"
TEST_EXIT="${6:-0}"

echo "=== 在容器内生成覆盖率信息 ==="
echo "WORKDIR: $WORKDIR"
echo "THREAD_NO: $THREAD_NO"
echo "CASE_NAME: $CASE_NAME"
echo "DEBUGPATH: $DEBUGPATH"
echo "TEST_EXIT: $TEST_EXIT"

if [ "$TEST_EXIT" -ne 0 ]; then
    echo "Note: collecting lcov despite test exit code=$TEST_EXIT"
fi

COVERAGE_LCOV_EVERY="${COVERAGE_LCOV_EVERY:-5}"
LCOV_COUNTER="${CONTAINER_TESTDIR}/test/.coverage_lcov_counter"
count=0
if [ -f "$LCOV_COUNTER" ]; then
    count=$(cat "$LCOV_COUNTER" 2>/dev/null || echo 0)
fi
count=$((count + 1))
echo "$count" > "$LCOV_COUNTER"
force_lcov=0
if [[ "$CASE_NAME" == *UnitTest* ]] || [[ "$CASE_NAME" == *82-UnitTest* ]]; then
    force_lcov=1
    echo "Force lcov for UnitTest case: $CASE_NAME"
fi
if [ "$force_lcov" -eq 0 ] && [ "$COVERAGE_LCOV_EVERY" -gt 1 ] && [ $((count % COVERAGE_LCOV_EVERY)) -ne 0 ]; then
    echo "Skip lcov: counter=$count interval=$COVERAGE_LCOV_EVERY"
    exit 0
fi

# 定义输出目录
COVERAGE_OUTPUT_DIR="${CONTAINER_TESTDIR}/test/coverage_info"
mkdir -p "$COVERAGE_OUTPUT_DIR"

# 查找可能的 debug 目录 - 注意容器内的路径
DEBUG_DIRS=()
if [ "$DEBUGPATH" = "debugSan" ]; then
    DEBUG_DIRS+=("/home/TDinternal/debug:debugSan")
elif [ "$DEBUGPATH" = "debugNoSan" ]; then
    DEBUG_DIRS+=("/home/TDinternal/debug:debugNoSan")
else
    # 如果不确定，检查两个可能的路径
    if [ -d "/home/TDinternal/debug" ]; then
        DEBUG_DIRS+=("/home/TDinternal/debug:debug")
    fi
fi

GENERATED_FILES=()

# 为每个 debug 目录生成覆盖率信息
for debug_entry in "${DEBUG_DIRS[@]}"; do
    DEBUG_PATH=$(echo "$debug_entry" | cut -d: -f1)
    DEBUG_TYPE=$(echo "$debug_entry" | cut -d: -f2)
    
    echo "检查目录: $DEBUG_PATH"
    
    # 检查目录是否存在
    if [ ! -d "$DEBUG_PATH" ]; then
        echo "目录不存在，跳过: $DEBUG_PATH"
        continue
    fi
    
    # 统计 GCDA 文件数量
    GCDA_COUNT=$(find "$DEBUG_PATH" -name "*.gcda" -type f 2>/dev/null | wc -l)
    
    if [ "$GCDA_COUNT" -eq 0 ]; then
        echo "目录 $DEBUG_PATH 中没有 GCDA 文件，跳过"
        continue
    fi
    
    echo "发现 $GCDA_COUNT 个 GCDA 文件在 $DEBUG_PATH"
    
    # 生成覆盖率信息文件
    INFO_FILENAME="coverage_${CASE_NAME}_${DEBUG_TYPE}.info"
    INFO_FILE_PATH="${COVERAGE_OUTPUT_DIR}/${INFO_FILENAME}"
    
    echo "生成覆盖率信息: $INFO_FILENAME"

    # ASAN+gcov and parallel tests may produce negative hit counts or missing
    # generated sources (e.g. parser sql.c); ignore those for CI capture.
    LCOV_IGNORE="--ignore-errors negative,inconsistent,deprecated,source,count,usage,missing,unused"
    LCOV_RC="--rc lcov_branch_coverage=0 --rc max_message_count=0"

    # Product-code dirs equivalent to main's "lcov -d .", but skip contrib/externals.
    LCOV_DIR_ARGS=()
    for rel in community/source community/tools community/utils; do
        if [ ! -d "${DEBUG_PATH}/${rel}" ]; then
            continue
        fi
        if ! find "${DEBUG_PATH}/${rel}" -name '*.gcda' -print -quit 2>/dev/null | grep -q .; then
            continue
        fi
        LCOV_DIR_ARGS+=("-d" "${rel}")
    done

    if [ ${#LCOV_DIR_ARGS[@]} -eq 0 ]; then
        echo "目录 $DEBUG_PATH 中没有可采集的 GCDA 子目录，跳过"
        continue
    fi

    LCOV_LOG="${COVERAGE_OUTPUT_DIR}/lcov_${CASE_NAME}.log"
    LCOV_CMD="cd ${DEBUG_PATH} && lcov --quiet ${LCOV_DIR_ARGS[*]} -capture ${LCOV_RC} ${LCOV_IGNORE} --no-external -b /home/TDinternal -o ${INFO_FILE_PATH}"

    echo "执行 lcov 命令: $LCOV_CMD"

    LCOV_OUTPUT=$(eval "$LCOV_CMD" 2>&1)
    lcov_ret=$?
    echo "$LCOV_OUTPUT" | tee "$LCOV_LOG" | tail -30

    if [ $lcov_ret -eq 0 ] && [ -s "$INFO_FILE_PATH" ]; then
        FILE_SIZE=$(stat -c%s "$INFO_FILE_PATH" 2>/dev/null || echo "0")
        echo "✓ 成功生成: $INFO_FILENAME (大小: $FILE_SIZE 字节)"
        GENERATED_FILES+=("$INFO_FILE_PATH")
    elif [ $lcov_ret -eq 0 ]; then
        echo "✗ 生成的文件为空: $INFO_FILENAME"
        rm -f "$INFO_FILE_PATH"
    else
        echo "✗ lcov 执行失败: $DEBUG_PATH (详见 $LCOV_LOG)"
        echo "调试：显示 GCDA 文件示例:"
        find "$DEBUG_PATH" -name "*.gcda" -type f | head -5
        echo "调试：显示 GCNO 文件示例:"
        find "$DEBUG_PATH" -name "*.gcno" -type f | head -5
    fi
done

# 如果生成了多个文件，尝试合并
if [ "${#GENERATED_FILES[@]}" -gt 1 ]; then
    echo "合并 ${#GENERATED_FILES[@]} 个覆盖率信息文件..."
    MERGED_FILE="${COVERAGE_OUTPUT_DIR}/coverage_${CASE_NAME}_merged.info"
    
    # 构建合并命令
    MERGE_CMD="lcov --quiet --rc lcov_branch_coverage=0 ${LCOV_IGNORE}"
    for info_file in "${GENERATED_FILES[@]}"; do
        MERGE_CMD="$MERGE_CMD --add-tracefile '$info_file'"
    done
    MERGE_CMD="$MERGE_CMD -o '$MERGED_FILE'"
    
    echo "执行合并命令: $MERGE_CMD"
    MERGE_OUTPUT=$(eval "$MERGE_CMD" 2>&1)
    merge_ret=$?
    echo "$MERGE_OUTPUT" | tail -20
    if [ $merge_ret -eq 0 ] && [ -s "$MERGED_FILE" ]; then
        MERGED_SIZE=$(stat -c%s "$MERGED_FILE" 2>/dev/null || echo "0")
        echo "✓ 成功合并: coverage_${CASE_NAME}_merged.info (大小: $MERGED_SIZE 字节)"

        # 删除单独文件，保留合并文件
        for info_file in "${GENERATED_FILES[@]}"; do
            rm -f "$info_file"
        done
        GENERATED_FILES=("$MERGED_FILE")
    elif [ $merge_ret -eq 0 ]; then
        echo "✗ 合并文件为空，保留单独文件"
        rm -f "$MERGED_FILE"
    else
        echo "✗ 合并失败，保留单独文件"
    fi
fi

# 生成索引文件
if [ "${#GENERATED_FILES[@]}" -gt 0 ]; then
    INDEX_FILE="${COVERAGE_OUTPUT_DIR}/coverage_files.list"
    printf "%s\n" "${GENERATED_FILES[@]}" > "$INDEX_FILE"
    echo "创建索引文件: $INDEX_FILE"
    echo "容器内覆盖率信息生成完成，共 ${#GENERATED_FILES[@]} 个文件"
    
    # 显示生成的文件详情
    echo "生成的覆盖率信息文件:"
    for info_file in "${GENERATED_FILES[@]}"; do
        if [ -f "$info_file" ]; then
            FILE_SIZE=$(stat -c%s "$info_file" 2>/dev/null || echo "0")
            FILE_LINES=$(wc -l < "$info_file" 2>/dev/null || echo "0")
            echo "  $(basename "$info_file"): $FILE_SIZE 字节, $FILE_LINES 行"
        fi
    done
    exit 0
else
    echo "警告: 未生成任何覆盖率信息文件"
    exit 1
fi
EOF

    # 使脚本可执行
    chmod +x "$coverage_script"
    echo "创建覆盖率生成脚本: $coverage_script"
}

ent=0
while getopts "w:d:c:t:o:s:eh" opt; do
    case $opt in
        w)
            WORKDIR=$OPTARG
            ;;
        d)
            exec_dir=$OPTARG
            ;;
        c)
            cmd=$OPTARG
            ;;
        t)
            thread_no=$OPTARG
            ;;
        e)
            ent=1
            ;;
        o)
            extra_param="-o $OPTARG"
            ;;
        s)
            buildSan=$OPTARG
            ;;
        h)
            usage
            exit 0
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            usage
            exit 0
            ;;
    esac
done

if [ -z "$WORKDIR" ] || [ -z "$exec_dir" ] || [ -z "$cmd" ] || [ -z "$thread_no" ]; then
    echo "coverage container missing arguments: workdir=[$WORKDIR] exec_dir=[$exec_dir] thread=[$thread_no] san=[$buildSan] cmd=[$cmd]"
    usage
    exit 1
fi

export COVERAGE_LCOV_EVERY="${COVERAGE_LCOV_EVERY:-5}"

#select whether the compilation environment  includes sanitizer
if [ "${buildSan}" == "y" ]; then
    DEBUGPATH="debugSan"
else
    DEBUGPATH="debugNoSan"
fi

if [ $ent -ne 0 ]; then
    # TSDB-Enterprise edition
    extra_param="$extra_param -e"
    INTERNAL_REPDIR=$WORKDIR/TDinternal
    REPDIR=$INTERNAL_REPDIR/community
    REPDIR_DEBUG=$WORKDIR/$DEBUGPATH/
    CONTAINER_TESTDIR=/home/TDinternal/community
    SIM_DIR=/home/TDinternal/sim
    REP_MOUNT_PARAM="$INTERNAL_REPDIR:/home/TDinternal"
    REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/home/TDinternal/debug/"
    REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/home/TDinternal/debug/build/lib:ro"
else
    # TSDB-OSS edition
    REPDIR=$WORKDIR/TDengine
    REPDIR_DEBUG=$WORKDIR/$DEBUGPATH/
    CONTAINER_TESTDIR=/home/TDengine
    SIM_DIR=/home/TDengine/sim
    REP_MOUNT_PARAM="$REPDIR:/home/TDengine"
    REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/home/TDengine/debug/"
    REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/home/TDinternal/debug/build/lib:ro"
fi

ulimit -c unlimited

TMP_DIR=$WORKDIR/tmp
MOUNT_DIR=""

# 清理和创建必要的目录
rm -rf ${TMP_DIR}/thread_volume/$thread_no/sim
_CI_SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../compat_packages_mount.sh
source "${_CI_SCRIPT_DIR}/../compat_packages_mount.sh"
SOURCEDIR=$(prepare_compat_packages_sourcedir "$WORKDIR")
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/sim/var_taoslog
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/sim/tsim
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/coredump
# 新增：创建覆盖率信息输出目录
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/coverage_info

rm -rf ${TMP_DIR}/thread_volume/$thread_no/coredump/*

# 如果 test 目录不存在，复制整个 test 目录内容
if [ ! -d "${TMP_DIR}/thread_volume/$thread_no/test" ]; then
    echo "cp -rf ${REPDIR}/test/* ${TMP_DIR}/thread_volume/$thread_no/"
    cp -rf "${REPDIR}/test/"* "${TMP_DIR}/thread_volume/$thread_no/"
fi

MOUNT_SOURCE="${TMP_DIR}/thread_volume/${thread_no}"
MOUNT_TARGET="${CONTAINER_TESTDIR}/test"
MOUNT_DIR="${MOUNT_SOURCE}:${MOUNT_TARGET}"

# 提取 case 名称用于覆盖率文件命名
case_name=$(echo "$cmd" | sed 's/[^a-zA-Z0-9._-]/_/g' | sed 's/__*/_/g' | sed 's/^_//g' | sed 's/_$//g')
if [ -z "$case_name" ]; then
    case_name="test_${thread_no}_$(date +%s)"
fi

echo "$thread_no -> ${exec_dir}:$cmd"
echo "Case name: $case_name"

# 生成覆盖率生成脚本
generate_coverage_script "$WORKDIR" "$thread_no" "$case_name" "$CONTAINER_TESTDIR" "$DEBUGPATH"

coredump_dir=`cat /proc/sys/kernel/core_pattern | xargs dirname`
if [ -z "$coredump_dir" ] || [ "$coredump_dir" = "." ]; then
    coredump_dir="/home/coredump"
fi

# 修改：创建一个复合命令，先运行测试，然后生成覆盖率信息
composite_cmd="${CONTAINER_TESTDIR}/test/ci/run_case.sh -d \"$exec_dir\" -c \"$cmd\" $extra_param; coverage_exit_code=\$?; echo \"Test execution completed with exit code: \$coverage_exit_code\"; if [ -f ${CONTAINER_TESTDIR}/test/generate_coverage.sh ]; then echo \"Generating coverage information...\"; bash ${CONTAINER_TESTDIR}/test/generate_coverage.sh \"$WORKDIR\" \"$thread_no\" \"$case_name\" \"$CONTAINER_TESTDIR\" \"$DEBUGPATH\" \"\$coverage_exit_code\"; coverage_gen_code=\$?; echo \"Coverage generation completed with exit code: \$coverage_gen_code\"; else echo \"Coverage generation script not found\"; fi; exit \$coverage_exit_code"

echo "执行复合命令: $composite_cmd"

# shellcheck source=../../cases/09-DataQuerying/19-FederatedQuery/fq_runtime_libs.sh
source "${REPDIR}/test/cases/09-DataQuerying/19-FederatedQuery/fq_runtime_libs.sh"

# Coverage checks out TDinternal/community only. Use the same runner lookup as
# run_container.sh so federated cases get a database lease instead of 127.0.0.1.
_fq_ci_resolve_runner() {
    if [[ -n "${FQ_CI_RUNNER:-}" && -f "${FQ_CI_RUNNER}" ]]; then
        echo "${FQ_CI_RUNNER}"
        return 0
    fi
    local community_root candidate
    community_root="$(cd "$REPDIR" && pwd -P)"
    candidate="$(dirname "$(dirname "$community_root")")/tools/cicd/tsdb-test-pipeline/fq/run-ci.py"
    if [[ -f "$candidate" ]]; then
        echo "$candidate"
        return 0
    fi
    if [[ -n "${FQ_CI_TOOLS_DIR:-}" && -f "${FQ_CI_TOOLS_DIR}/fq/run-ci.py" ]]; then
        echo "${FQ_CI_TOOLS_DIR}/fq/run-ci.py"
        return 0
    fi
    candidate="/root/tsdb/tools/cicd/tsdb-test-pipeline/fq/run-ci.py"
    if [[ -f "$candidate" ]]; then
        echo "$candidate"
        return 0
    fi
    return 1
}

fq_runner=()
fq_default_services=()
if fq_ci_needs_databases "$cmd" && [[ "${FQ_CI_CONTAINER_ENV:-1}" == "1" ]]; then
    FQ_RUNNER="$(_fq_ci_resolve_runner)" || {
        echo "ERROR: FQ container runner not found. Set FQ_CI_RUNNER to tools/cicd/tsdb-test-pipeline/fq/run-ci.py" >&2
        exit 1
    }
    fq_cert_dir="${REPDIR}/test/cases/09-DataQuerying/19-FederatedQuery/certs"
    if [[ ! -d "${fq_cert_dir}" ]]; then
        echo "ERROR: FQ cert dir missing: ${fq_cert_dir}" >&2
        exit 1
    fi
    # Tasks that omit FQ_REQUIRED_SERVICES still talk to MySQL, PostgreSQL,
    # Influx and SQL Server. Leave Oracle off unless the task asks for it.
    if [[ "$cmd" != *FQ_REQUIRED_SERVICES=* ]]; then
        fq_default_services=(-e FQ_REQUIRED_SERVICES=mysql,pg,influx,sqlserver)
    fi
    export FQ_CI_PLACEMENT="${FQ_CI_PLACEMENT:-local-first}"
    # Do not share /tmp/tsdb-fq-ci-slots. That directory also holds the tsdb
    # runner, and a full set of quarantined slots makes try_local_lease fail
    # before docker starts. Coverage keeps its own pool and a port base above
    # the runner's 23000 range. Eight slots use the idle capacity on the
    # coverage host (40 CPUs, ~200 GiB free). Memory admission in run-ci.py
    # still refuses a stack that would not fit.
    export FQ_CI_LOCAL_LOCK_DIR="${FQ_CI_LOCAL_LOCK_DIR:-/tmp/tdengine-coverage-fq-slots}"
    export FQ_CI_LOCAL_PORT_BASE="${FQ_CI_LOCAL_PORT_BASE:-26000}"
    export FQ_CI_LOCAL_MAX_SLOTS="${FQ_CI_LOCAL_MAX_SLOTS:-8}"
    export FQ_CI_PREPARE_TIMEOUT="${FQ_CI_PREPARE_TIMEOUT:-1200}"
    export FQ_CI_AIO_MAX_NR="${FQ_CI_AIO_MAX_NR:-4194304}"
    export FQ_CI_HYBRID="${FQ_CI_HYBRID:-1}"
    # The lease timer wraps the container, including lcov after the case.
    fq_case_timeout=1230
    if [[ "${extra_param}" =~ -o[[:space:]]+([0-9]+) ]]; then
        fq_case_timeout="${BASH_REMATCH[1]}"
    fi
    export FQ_CI_CASE_TIMEOUT="${FQ_CI_CASE_TIMEOUT:-$((fq_case_timeout + 900))}"
    # Admission denial is not a test result. The tsdb scheduler retries outside
    # the case; coverage calls the runner directly, so wait here until a slot
    # is free. The case timeout still bounds only the docker process.
    fq_lease_wait="${FQ_CI_LEASE_WAIT:-10800}"
    echo "[FQ CI] lease runner: ${FQ_RUNNER}"
    fq_runner=(python3 "${FQ_RUNNER}" --artifacts "${MOUNT_SOURCE}/fq-artifacts/attempt-${FQ_CI_ATTEMPT:-1}"
               --cert-dir "${fq_cert_dir}" --)
fi

docker_cmd=(docker run)
if [[ ${#fq_default_services[@]} -gt 0 ]]; then
    docker_cmd+=("${fq_default_services[@]}")
fi
docker_cmd+=(
    -e "COVERAGE_LCOV_EVERY=${COVERAGE_LCOV_EVERY}"
    -v "${REP_MOUNT_PARAM}"
    -v "${REP_MOUNT_DEBUG}"
    -v "${REP_MOUNT_LIB}"
    -v "${MOUNT_DIR}"
    -v "${SOURCEDIR}:/usr/local/src/"
    -v "${TMP_DIR}/thread_volume/${thread_no}/sim:${SIM_DIR}"
    -v "${TMP_DIR}/thread_volume/${thread_no}/coredump:${coredump_dir}"
    --rm --ulimit core=-1
    "${DOCKER_IMAGE_NAME:-tdengine-ci:0.4}"
    bash -c "${composite_cmd}"
)

if [[ ${#fq_runner[@]} -gt 0 ]]; then
    python3 "${FQ_RUNNER}" --reap-local || true
    fq_lease_deadline=$((SECONDS + fq_lease_wait))
    while true; do
        fq_lease_log=$(mktemp)
        "${fq_runner[@]}" "${docker_cmd[@]}" 2>&1 | tee "$fq_lease_log"
        ret=${PIPESTATUS[0]}
        if [[ $ret -eq 0 ]] || ! grep -q 'runner-local FQ reservation unavailable' "$fq_lease_log"; then
            rm -f "$fq_lease_log"
            break
        fi
        rm -f "$fq_lease_log"
        if (( SECONDS >= fq_lease_deadline )); then
            echo "[FQ CI] coverage waited ${fq_lease_wait}s and still has no local database lease" >&2
            break
        fi
        echo "[FQ CI] local database lease denied; reaping coverage slots and retrying" >&2
        python3 "${FQ_RUNNER}" --reap-local || true
        sleep $(( (RANDOM % 5) + 5 ))
    done
else
    "${docker_cmd[@]}"
    ret=$?
fi

# 检查覆盖率信息是否生成成功
echo "检查覆盖率信息生成结果..."
coverage_output_dir="${TMP_DIR}/thread_volume/${thread_no}/coverage_info"
if [ -d "$coverage_output_dir" ]; then
    info_count=$(find "$coverage_output_dir" -name "*.info" -type f 2>/dev/null | wc -l)
    echo "生成了 $info_count 个覆盖率信息文件"
    
    if [ "$info_count" -gt 0 ]; then
        echo "覆盖率信息文件列表:"
        find "$coverage_output_dir" -name "*.info" -type f | while read -r info_file; do
            file_size=$(stat -c%s "$info_file" 2>/dev/null || echo "0")
            echo "  $(basename "$info_file"): $file_size 字节"
        done
    else
        echo "警告: 未生成任何覆盖率信息文件"
    fi
else
    echo "警告: 覆盖率信息输出目录不存在: $coverage_output_dir"
fi

exit $ret
