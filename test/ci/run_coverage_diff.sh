#!/bin/bash

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

function print_color() {
    local color="$1"
    local message="$2"
    echo -e "${color}${message}${NC}"
}

function printHelp() {
    echo "Usage: $(basename $0) [options]"
    echo
    echo "Options:"
    echo "    -d [TDengine dir]           Project directory (default: outermost project directory)"
    echo "                                    e.g., -d /home/TDinternal/"
    echo "    -f [Capture gcda dir]       Capture gcda directory (default: <project dir>/debug)"
    echo "    -b [Coverage branch]        Covevrage branch "
    echo "    -l [Test log dir]           Test log directory containing gcda files"
    exit 0
}

function collect_gcda_from_tests() {
    local test_log_dir="$1"
    local target_debug_dir="$2"
    
    if [ -z "$test_log_dir" ] || [ ! -d "$test_log_dir" ]; then
        echo "测试日志目录不存在或未指定，跳过 GCDA 收集: $test_log_dir"
        return 0
    fi
    
    echo "=== 从测试日志收集 GCDA 文件 ==="
    echo "源目录: $test_log_dir"
    echo "目标目录: $target_debug_dir"
    
    python3 - << EOF
import os
import glob
import shutil
import hashlib
from datetime import datetime

def get_file_md5(file_path):
    try:
        md5_hash = hashlib.md5()
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()
    except Exception as e:
        print(f"计算 MD5 失败 {file_path}: {e}")
        return None

def process_gcda_files(test_log_dir, target_debug_dir):
    gcda_pattern = os.path.join(test_log_dir, "**", "*.gcda")
    gcda_files = glob.glob(gcda_pattern, recursive=True)
    
    print(f"找到 {len(gcda_files)} 个 GCDA 文件")
    
    processed_count = 0
    for gcda_file in gcda_files:
        try:
            # 计算 MD5
            md5_value = get_file_md5(gcda_file)
            if md5_value is None:
                continue
                
            # 获取相对路径结构
            rel_path = os.path.relpath(gcda_file, test_log_dir)
            
            # 构建目标路径，保持目录结构
            target_path = os.path.join(target_debug_dir, rel_path)
            target_dir = os.path.dirname(target_path)
            
            # 确保目标目录存在
            os.makedirs(target_dir, exist_ok=True)
            
            # 生成带 MD5 的文件名
            filename = os.path.basename(gcda_file)
            name_parts = filename.rsplit('.', 1)
            if len(name_parts) == 2:
                base_name, extension = name_parts
            else:
                base_name = filename
                extension = ""
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            md5_short = md5_value[:8]
            
            if extension:
                if '.' in base_name:
                    name_without_ext, first_ext = base_name.rsplit('.', 1)
                    new_filename = f"{name_without_ext}_{timestamp}_{md5_short}.{first_ext}.{extension}"
                else:
                    new_filename = f"{base_name}_{timestamp}_{md5_short}.{extension}"
            else:
                new_filename = f"{base_name}_{timestamp}_{md5_short}"
            
            final_target_path = os.path.join(target_dir, new_filename)
            
            # 复制文件
            shutil.copy2(gcda_file, final_target_path)
            print(f"已处理: {gcda_file} -> {final_target_path} (MD5: {md5_value[:8]})")
            processed_count += 1
            
        except Exception as e:
            print(f"处理文件失败 {gcda_file}: {e}")
    
    print(f"成功处理 {processed_count} 个 GCDA 文件")
    return processed_count

# 执行处理
if __name__ == "__main__":
    process_gcda_files("$test_log_dir", "$target_debug_dir")
EOF

    echo "=== GCDA 文件收集完成 ==="
}

function lcovFunc {
    echo "collect data by lcov"
    cd $TDENGINE_DIR || exit


    # 如果指定了测试日志目录，先收集 GCDA 文件
    if [ -n "$TEST_LOG_DIR" ]; then
        collect_gcda_from_tests "$TEST_LOG_DIR" "$CAPTURE_GCDA_DIR"
    fi


#     # 创建 lcov 配置文件
#     cat > lcov_tdengine.config << EOF
# # lcov 配置文件 - 只包含指定的源文件
# genhtml_branch_coverage = 1
# lcov_branch_coverage = 1
# EOF

#     # 调试输出配置文件内容
#     echo "lcov_tdengine.config 内容:"
#     cat lcov_tdengine.config

    # 显示 GCDA 文件统计
    echo "=== GCDA 文件统计 ==="
    gcda_count=$(find "$CAPTURE_GCDA_DIR" -name "*.gcda" -type f | wc -l)
    echo "debug 目录中的 GCDA 文件数量: $gcda_count"
    
    if [ "$gcda_count" -eq 0 ]; then
        echo "警告: 未找到任何 GCDA 文件，覆盖率报告可能为空"
    fi

    # 在 lcov 的 --capture、--remove 和 --list 操作中添加 --quiet 参数，减少冗余输出,仅减少输出信息，不影响功能。
    lcov --quiet -d ../debug/ -capture \
        --rc lcov_branch_coverage=1 \
        --rc genhtml_branch_coverage=1 \
        --no-external \
        --config-file lcov_tdengine.config \
        -b $TDENGINE_DIR/ \
        -o coverage_tdengine_raw.info 
    
    # # remove exclude paths (确保只保留 community 相关的文件)
    # lcov --quiet --remove coverage_tdengine.info \
    #     '*/enterprise/*' '*/contrib/*' '*/test/*' '*/packaging/*' '*/docs/*' '*/debug/*' '*/sql.c' '*/sql.y' \
    #     '*/source/*' \
    #     '*/include/*' \
    #     '*/tools/src/*' \
    #     '*/taos-tools/deps/*' '*/taosadapter/*' '*/TSZ/*' \
    #     '*/AccessBridgeCalls.c' '*/ttszip.c' '*/dataInserter.c' '*/tlinearhash.c' '*/tsimplehash.c' '*/tsdbDiskData.c' '/*/enterprise/*' '*/docs/*' '*/sim/*'\
    #     '*/texpr.c' '*/runUdf.c' '*/schDbg.c' '*/syncIO.c' '*/tdbOs.c' '*/pushServer.c' '*/osLz4.c'\
    #     '*/tbase64.c' '*/tbuffer.c' '*/tdes.c' '*/texception.c' '*/examples/*' '*/tidpool.c' '*/tmempool.c'\
    #     '*/clientJniConnector.c' '*/clientTmqConnector.c' '*/version.cc' '*/strftime.c' '*/localtime.c'\
    #     '*/tthread.c' '*/tversion.c'  '*/ctgDbg.c' '*/schDbg.c' '*/qwDbg.c' '*/version.c' '*/tencode.h' \
    #     '*/shellAuto.c' '*/shellTire.c' '*/shellCommand.c' '*/debug/*' '*/tests/*'\
    #     '*/tsdbFile.c' '*/tsdbUpgrade.c' '*/tsdbFS.c' '*/tsdbReaderWriter.c' \
    #     '*/sql.c' '*/sql.y' '*/smaSnapshot.c' '*/smaCommit.c'\
    #     '*/streamsessionnonblockoperator.c' '*/streameventnonblockoperator.c' '*/streamstatenonblockoperator.c' '*/streamfillnonblockoperator.c' \
    #     '*/streamclient.c' '*/cos_cp.c' '*/cos.c' '*/trow.c' '*/trow.h' '*/tsdbSnapshot.c' '*/smaTimeRange.c' \
    #     '*/metaSma.c' '*/mndDump.c' '*/td_block_blob_client.cpp' \
    #     '*/taos-tools/deps/toolscJson/src/*' '*/taos-tools/deps/jansson/src/*' \
    #      --rc lcov_branch_coverage=1  -o coverage_tdengine.info 

    # 使用 coverage.txt 文件来过滤需要的文件
    if [ -f "$TDENGINE_DIR/test/ci/coverage.txt" ]; then
        echo "使用 coverage.txt 文件过滤覆盖率数据..."
        
        # 使用 --extract 参数，从 coverage.txt 读取文件列表，为每个文件添加路径前缀
        while IFS= read -r file_pattern; do
            # 跳过空行和注释行
            [[ -z "$file_pattern" || "$file_pattern" =~ ^[[:space:]]*# ]] && continue
            
            # 添加到 include 列表
            include_patterns="$include_patterns '*/$file_pattern'"
        done < "$TDENGINE_DIR/test/ci/coverage.txt"
        
        # 使用 lcov --extract 提取指定的文件
        eval "lcov --quiet --extract coverage_tdengine_raw.info $include_patterns \
            --rc lcov_branch_coverage=1 \
            -o coverage_tdengine.info"
            
    else
        echo "Warning: coverage.txt 文件不存在，使用原始数据"
        cp coverage_tdengine_raw.info coverage_tdengine.info
    fi

    # # 清理临时文件
    # rm -f coverage_tdengine_raw.info

    # generate result
    echo "generate result"
    lcov --quiet -l --rc lcov_branch_coverage=1 coverage_tdengine.info 

    # echo "lcov --list 修正前输出:"
    # lcov --list $TDENGINE_DIR/coverage_tdengine.info --rc lcov_branch_coverage=1
    
    # 修正路径以确保与 TDengine 仓库根目录匹配    
    sed -i "s|SF:/home/TDinternal/community/|SF:|g" $TDENGINE_DIR/coverage_tdengine.info

    ## 添加详细的调试信息
    echo "=== 文件检查 ==="
    echo "当前目录: $(pwd)"
    echo "目标文件: $TDENGINE_DIR/coverage_tdengine.info"
    
    ## 使用绝对路径和相对路径都检查一遍
    for file_path in "$TDENGINE_DIR/coverage_tdengine.info" "./coverage_tdengine.info" "coverage_tdengine.info"; do
        if [ -f "$file_path" ]; then
            size=$(stat -c%s "$file_path" 2>/dev/null)
            echo "✓ 找到文件: $file_path (大小: $size 字节)"
        else
            echo "✗ 文件不存在: $file_path"
        fi
    done
    
    ## 列出当前目录所有文件
    echo "当前目录文件列表:"
    ls -la | grep -E "\.info$" || echo "未找到相关文件"

    # 确保 coverage_tdengine.info 文件不为空
    if [ ! -s $TDENGINE_DIR/coverage_tdengine.info ]; then
        echo "Error: coverage_tdengine.info 文件为空，无法上传到 Codecov"
        exit 1
    fi

    # 调试输出 coverage_tdengine.info 内容
    echo "coverage_tdengine.info 内容:"
    cat $TDENGINE_DIR/coverage_tdengine.info | grep SF

    # push result to https://app.codecov.io/
    pip install codecov
    echo "开始上传覆盖率数据到 Codecov..."
    echo "BRANCH: $BRANCH"
    echo "coverage_tdengine.info: $TDENGINE_DIR/coverage_tdengine.info"
    timeout 300 codecov -t b0e18192-e4e0-45f3-8942-acab64178afe \
        -f $TDENGINE_DIR/coverage_tdengine.info \
        -b $BRANCH \
        -n "TDengine Coverage Report" \
        -F "TDengine" \
        --no-gcov-out \
        --required 

    # 检查上传结果
    if [ $? -ne 0 ]; then
        echo "Error: 上传到 Codecov 失败，请检查日志输出。"
    else
        echo "覆盖率数据已成功上传到 Codecov。"
    fi  

}


######################
# main entry
######################

# Initialization parameter
TDINTERNAL_DIR="/home/TDinternal" 
TDENGINE_DIR="/home/TDinternal/community"
CAPTURE_GCDA_DIR="/home/TDinternal/debug"
TEST_LOG_DIR=""

# Parse command line parameters
while getopts "hd:b:f:l:" arg; do
  case $arg in
    d)
      TDINTERNAL_DIR=$OPTARG
      ;;
    b)
      BRANCH=$OPTARG
      ;;
    f)
      CAPTURE_GCDA_DIR=$OPTARG
      ;;
    l)
      TEST_LOG_DIR=$OPTARG
      ;;
    h)
      printHelp
      ;;
    ?)
      echo "Usage: ./$(basename $0) -h"
      exit 1
      ;;
  esac
done

# Show all parameters
print_color "$GREEN" "Run coverage test on workflow!"

echo "TDENGINE_DIR = $TDENGINE_DIR"
echo "CAPTURE_GCDA_DIR = $CAPTURE_GCDA_DIR"
echo "TEST_LOG_DIR = $TEST_LOG_DIR"
echo "BRANCH = $BRANCH"

lcovFunc

COVERAGE_INFO="$TDENGINE_DIR/coverage.info"
OUTPUT_DIR="$CAPTURE_GCDA_DIR/coverage_report"


print_color "$GREEN" "End of coverage test on workflow!"

echo "For more details: https://app.codecov.io/github/taosdata/TDengine"