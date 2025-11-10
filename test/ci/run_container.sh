#!/bin/bash

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

echo "=== 在容器内生成覆盖率信息 ==="
echo "WORKDIR: $WORKDIR"
echo "THREAD_NO: $THREAD_NO"
echo "CASE_NAME: $CASE_NAME"
echo "DEBUGPATH: $DEBUGPATH"

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
    
    # 执行 lcov 命令 - 关键：使用正确的源码路径
    LCOV_CMD="cd ${DEBUG_PATH} && lcov --quiet -d . -capture \
        --rc lcov_branch_coverage=0 \
        --rc genhtml_branch_coverage=0 \
        --no-external \
        -b /home/TDinternal/community/ \
        -o ${INFO_FILE_PATH}"
    
    echo "执行 lcov 命令: $LCOV_CMD"
    
    if eval "$LCOV_CMD" 2>/dev/null; then
        if [ -s "$INFO_FILE_PATH" ]; then
            FILE_SIZE=$(stat -c%s "$INFO_FILE_PATH" 2>/dev/null || echo "0")
            echo "✓ 成功生成: $INFO_FILENAME (大小: $FILE_SIZE 字节)"
            GENERATED_FILES+=("$INFO_FILE_PATH")
        else
            echo "✗ 生成的文件为空: $INFO_FILENAME"
            rm -f "$INFO_FILE_PATH"
        fi
    else
        echo "✗ lcov 执行失败: $DEBUG_PATH"
        # 调试：显示一些文件示例
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
    MERGE_CMD="lcov --quiet --rc lcov_branch_coverage=0"
    for info_file in "${GENERATED_FILES[@]}"; do
        MERGE_CMD="$MERGE_CMD --add-tracefile '$info_file'"
    done
    MERGE_CMD="$MERGE_CMD -o '$MERGED_FILE'"
    
    echo "执行合并命令: $MERGE_CMD"
    if eval "$MERGE_CMD" 2>/dev/null; then
        if [ -s "$MERGED_FILE" ]; then
            MERGED_SIZE=$(stat -c%s "$MERGED_FILE" 2>/dev/null || echo "0")
            echo "✓ 成功合并: coverage_${CASE_NAME}_merged.info (大小: $MERGED_SIZE 字节)"
            
            # 删除单独文件，保留合并文件
            for info_file in "${GENERATED_FILES[@]}"; do
                rm -f "$info_file"
            done
            GENERATED_FILES=("$MERGED_FILE")
        else
            echo "✗ 合并文件为空，保留单独文件"
            rm -f "$MERGED_FILE"
        fi
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

if [ -z "$WORKDIR" ]; then
    usage
    exit 1
fi
if [ -z "$exec_dir" ]; then
    usage
    exit 1
fi
if [ -z "$cmd" ]; then
    usage
    exit 1
fi
if [ -z "$thread_no" ]; then
    usage
    exit 1
fi

#select whether the compilation environment  includes sanitizer
if [ "${buildSan}" == "y" ]; then
    DEBUGPATH="debugSan"
elif [[ "${buildSan}" == "n" ]] || [[ "${case_build_san}" == "" ]]; then
    DEBUGPATH="debugNoSan"
else
    usage
    exit 1
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
SOURCEDIR=$WORKDIR/src
MOUNT_DIR=""

# 清理和创建必要的目录
rm -rf ${TMP_DIR}/thread_volume/$thread_no/sim
mkdir -p $SOURCEDIR
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/sim/var_taoslog
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/sim/tsim
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/coredump
# 新增：创建覆盖率信息输出目录
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/coverage_info

rm -rf ${TMP_DIR}/thread_volume/$thread_no/coredump/*

if [ ! -d "${TMP_DIR}/thread_volume/$thread_no/test" ]; then
    if [ "$exec_dir" != "." ]; then
        subdir=`echo "$exec_dir"|cut -d/ -f1`
        echo "cp -rf ${REPDIR}/test/$subdir ${TMP_DIR}/thread_volume/$thread_no/"
        cp -rf ${REPDIR}/test/$subdir ${TMP_DIR}/thread_volume/$thread_no/
    else
        echo "cp -rf ${REPDIR}/test/* ${TMP_DIR}/thread_volume/$thread_no/"
        cp -rf "${REPDIR}/test/"* "${TMP_DIR}/thread_volume/$thread_no/"
    fi
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
composite_cmd="$CONTAINER_TESTDIR/test/ci/run_case.sh -d \"$exec_dir\" -c \"$cmd\" $extra_param; coverage_exit_code=\$?; echo \"Test execution completed with exit code: \$coverage_exit_code\"; if [ -f $CONTAINER_TESTDIR/test/generate_coverage.sh ]; then echo \"Generating coverage information...\"; bash $CONTAINER_TESTDIR/test/generate_coverage.sh \"$WORKDIR\" \"$thread_no\" \"$case_name\" \"$CONTAINER_TESTDIR\" \"$DEBUGPATH\"; coverage_gen_code=\$?; echo \"Coverage generation completed with exit code: \$coverage_gen_code\"; else echo \"Coverage generation script not found\"; fi; exit \$coverage_exit_code"

echo "执行复合命令: $composite_cmd"

docker run \
    -v $REP_MOUNT_PARAM \
    -v $REP_MOUNT_DEBUG \
    -v $REP_MOUNT_LIB \
    -v $MOUNT_DIR \
    -v ${SOURCEDIR}:/usr/local/src/ \
    -v "$TMP_DIR/thread_volume/$thread_no/sim:${SIM_DIR}" \
    -v ${TMP_DIR}/thread_volume/$thread_no/coredump:$coredump_dir \
    --rm --ulimit core=-1 tdengine-ci:0.1 bash -c "$composite_cmd"

ret=$?

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