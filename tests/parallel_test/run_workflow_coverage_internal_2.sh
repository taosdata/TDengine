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
    echo "    -b [Coverage branch]        Covevrage branch (default:3.0)"
    exit 0
}

function lcovFunc {
    echo "collect data by lcov"
    cd $TDINTERNAL_DIR || exit

#     # 创建 lcov 配置文件
#     cat > lcov_internal.config << EOF
# # 设置要忽略的文件和目录
# exclude_patterns=/home/TDinternal/community/*
# # /home/TDinternal/community/contrib/*
# # /home/TDinternal/community/tests/*
# # /home/TDinternal/community/packaging/*
# # /home/TDinternal/community/documentation/*
# # /home/TDinternal/community/scripts/*
# # /home/TDinternal/community/tools/*
# # /home/TDinternal/community/debug/*
# EOF


    # # 调试输出配置文件内容
    # echo "lcov_internal.config 内容:"
    # cat lcov_internal.config

    # # 收集数据时仅处理 enterprise 开头的文件
    # # 在 lcov 的 --capture、--remove 和 --list 操作中添加 --quiet 参数，减少冗余输出,仅减少输出信息，不影响功能。
    # lcov --quiet -d $TDINTERNAL_DIR/debug/ -capture \
    #     --rc lcov_branch_coverage=1 \
    #     --rc genhtml_branch_coverage=1 \
    #     --no-external \
    #     --config-file lcov_internal.config \
    #     -b $TDINTERNAL_DIR/enterprise \
    #     -o $TDINTERNAL_DIR/coverage_internal_2.info 

    # 创建临时目录
    mkdir -p "$TEMP_DIR"
    #trap 'rm -rf "$TEMP_DIR"' EXIT # 退出时自动清理临时文件

    export TEMP_DIR  # 导出临时目录变量，供子进程使用

    # 步骤1：遍历所有 .gcda 文件并生成临时 .info 文件
    find "$SEARCH_DIR" -name "*.gcda" | xargs -P 24 -I {} bash -c '
      gcda_file="{}"
      temp_info="$TEMP_DIR/$(basename "$gcda_file")_$(echo "$gcda_file" | md5sum | cut -d" " -f1).info"
      
      echo "处理文件：$gcda_file"
      # 打印完整的 lcov 命令
      echo "执行命令：lcov --quiet --capture --directory \"$gcda_file\" --output-file \"$temp_info\" --rc lcov_branch_coverage=1 --rc genhtml_branch_coverage=1 --no-external --config-file lcov_tdengine.config" >> debug_lcov.log
      
      lcov --capture --initial \
        --directory "$gcda_file" \
        --output-file "$temp_info" \
        --rc lcov_branch_coverage=1 \
        --rc genhtml_branch_coverage=1 2>> lcov_errors.log
      
      if [ $? -ne 0 ]; then
        echo "错误：处理 $gcda_file 失败" >> lcov_errors.log
        exit 1
      fi
      
      if [ ! -s "$temp_info" ]; then
        echo "警告：临时文件 $temp_info 为空" >> lcov_errors.log
      fi
    '      

    # 步骤2：合并所有临时 .info 文件
    echo "合并所有 .info 文件..."
    temp_merged_file="$TEMP_DIR/temp_merged.info" # 临时合并文件
    > "$temp_merged_file" # 初始化临时合并文件

    for temp_info in "$TEMP_DIR"/*.info; do
      echo "正在合并文件：$temp_info"
      
      if [ ! -s "$temp_merged_file" ]; then
        # 如果临时合并文件为空，直接初始化为当前文件
        cp "$temp_info" "$temp_merged_file"
      else
        # 合并当前文件到临时合并文件
        lcov --add-tracefile "$temp_info" \
            --add-tracefile "$temp_merged_file" \
            --output-file "$temp_merged_file" \
            --rc lcov_branch_coverage=1 \
            --rc genhtml_branch_coverage=1 2>> lcov_errors.log
        if [ $? -ne 0 ]; then
          echo "错误：合并 $temp_info 失败" >> lcov_errors.log
          exit 1
        fi
      fi
      
      # 打印完整的 lcov 命令
      echo "执行命令：lcov --add-tracefile \"$temp_info\" --add-tracefile \"$temp_merged_file\" --output-file \"$temp_merged_file\" --rc lcov_branch_coverage=1 --rc genhtml_branch_coverage=1" >> debug_merge.log
    done

    # 将最终合并的文件复制到 OUTPUT_FILE
    cp "$temp_merged_file" "$OUTPUT_FILE"
    cp "$temp_merged_file" $TDINTERNAL_DIR/coverage_internal_2.info 

    # remove exclude paths (确保只保留 enterprise 相关的文件)
    lcov --quiet --remove $TDINTERNAL_DIR/coverage_internal_2.info \
        '*/community/*' '*/contrib/*' '*/test/*' '*/packaging/*' '*/docs/*' '*/debug/*' '*/sql.c' '*/sql.y'\
         --rc lcov_branch_coverage=1  -o $TDINTERNAL_DIR/coverage_internal_2.info 

    sed -i "s|SF:/home/TDinternal/|SF:|g" $TDINTERNAL_DIR/coverage_internal.info

    # 确保 coverage_internal_2.info 文件不为空
    if [ ! -s $TDINTERNAL_DIR/coverage_internal_2.info ]; then
        echo "Error: coverage_internal_2.info 文件为空，无法上传到 Codecov"
        exit 1
    fi
    # generate result
    echo "generate result"
    cat $TDINTERNAL_DIR/coverage_internal_2.info | grep SF
    lcov -l --rc lcov_branch_coverage=1 $TDINTERNAL_DIR/coverage_internal_2.info    

    # push result to https://app.codecov.io/
    pip install codecov
    #codecov -t b0e18192-e4e0-45f3-8942-acab64178afe -f $TDENGINE_DIR/coverage.info  -b $BRANCH
    #codecov -t b0e18192-e4e0-45f3-8942-acab64178afe -f coverage.info  -b $BRANCH -X gcov 
    #如果覆盖率数据已经由其他工具（如 lcov）生成，可以通过 -X gcov 禁用 gcov 的自动收集，以避免冲突或冗余。
    
    # 调试输出 coverage_internal_2.info 内容
    echo "coverage_internal_2.info 内容:"
    cat $TDINTERNAL_DIR/coverage_internal_2.info | grep SF

    # push result to https://app.codecov.io/
    echo "开始上传覆盖率数据到 Codecov..."
    echo "BRANCH: $BRANCH"
    echo "coverage_internal_2.info: $TDINTERNAL_DIR/coverage_internal_2.info"
    codecov -t 88ed2789-23be-455d-acb6-3f729d285d1c \
        -f $TDINTERNAL_DIR/coverage_internal_2.info \
        -b $BRANCH \
        -n "TDinternal Coverage Report" \
        -F "TDinternal" \
        --gcov-args="-q" --no-gcov-out
         # \--verbose

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
#TDENGINE_DIR="/home/TDinternal/community"
CAPTURE_GCDA_DIR="/home/TDinternal/debug"
BRANCH="cover/3.0"

# 配置参数
SEARCH_DIR="/home/TDinternal/debug/enterprise/"          # 要扫描的根目录
OUTPUT_FILE="merged.info"     # 最终合并的 .info 文件
TEMP_DIR="/home/merge/temp_info"        # 临时文件存放目录

# Parse command line parameters
while getopts "hd:b:f:" arg; do
  case $arg in
    d)
      TDINTRENAL_DIR=$OPTARG
      ;;
    b)
      BRANCH=$OPTARG
      ;;
    f)
      CAPTURE_GCDA_DIR=$OPTARG
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

echo "TDINTERNAL_DIR = $TDINTERNAL_DIR"
#echo "TDENGINE_DIR = $TDENGINE_DIR"
echo "CAPTURE_GCDA_DIR = $CAPTURE_GCDA_DIR"
echo "BRANCH = $BRANCH"
echo "SEARCH_DIR = $SEARCH_DIR"
echo "TEMP_DIR = $TEMP_DIR"

lcovFunc

COVERAGE_INFO="$TDINTERNAL_DIR/coverage_internal_2.info"
OUTPUT_DIR="$CAPTURE_GCDA_DIR/coverage_report"
# Generate local HTML reports
# genhtml "$COVERAGE_INFO"  --branch-coverage --function-coverage --output-directory "$OUTPUT_DIR"


print_color "$GREEN" "End of coverage test on workflow!"

echo "For more details: https://app.codecov.io/github/taosdata/TDinternal"
