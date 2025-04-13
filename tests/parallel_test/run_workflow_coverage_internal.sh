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

    # 创建 lcov 配置文件
    cat > lcov_internal.config << EOF
# 设置要忽略的文件和目录
exclude_patterns=/home/TDinternal/community/*
# /home/TDinternal/community/contrib/*
# /home/TDinternal/community/tests/*
# /home/TDinternal/community/packaging/*
# /home/TDinternal/community/documentation/*
# /home/TDinternal/community/scripts/*
# /home/TDinternal/community/tools/*
# /home/TDinternal/community/debug/*
EOF


    # 调试输出配置文件内容
    echo "lcov_internal.config 内容:"
    cat lcov_internal.config

    # 收集数据时仅处理 enterprise 开头的文件
    # 在 lcov 的 --capture、--remove 和 --list 操作中添加 --quiet 参数，减少冗余输出,仅减少输出信息，不影响功能。
    lcov --quiet -d $TDINTERNAL_DIR/debug/ -capture \
        --rc lcov_branch_coverage=1 \
        --rc genhtml_branch_coverage=1 \
        --no-external \
        --config-file lcov_internal.config \
        -b $TDINTERNAL_DIR/enterprise \
        -o $TDINTERNAL_DIR/coverage_internal.info 
    

    # remove exclude paths (确保只保留 enterprise 相关的文件)
    lcov --quiet --remove $TDINTERNAL_DIR/coverage_internal.info \
        '*/community/*' '*/contrib/*' '*/test/*' '*/packaging/*' '*/docs/*' '*/debug/*' '*/sql.c' '*/sql.y'\
         --rc lcov_branch_coverage=1  -o $TDINTERNAL_DIR/coverage_internal.info 

    sed -i "s|SF:/home/TDinternal/|SF:|g" $TDINTERNAL_DIR/coverage_internal.info

    # 确保 coverage_internal.info 文件不为空
    if [ ! -s $TDINTERNAL_DIR/coverage_internal.info ]; then
        echo "Error: coverage_internal.info 文件为空，无法上传到 Codecov"
        exit 1
    fi
    # generate result
    echo "generate result"
    cat $TDINTERNAL_DIR/coverage_internal.info | grep SF
    lcov -l --rc lcov_branch_coverage=1 $TDINTERNAL_DIR/coverage_internal.info    

    # push result to https://app.codecov.io/
    pip install codecov
    #codecov -t b0e18192-e4e0-45f3-8942-acab64178afe -f $TDENGINE_DIR/coverage.info  -b $BRANCH
    #codecov -t b0e18192-e4e0-45f3-8942-acab64178afe -f coverage.info  -b $BRANCH -X gcov 
    #如果覆盖率数据已经由其他工具（如 lcov）生成，可以通过 -X gcov 禁用 gcov 的自动收集，以避免冲突或冗余。
    
    # 调试输出 coverage_internal.info 内容
    echo "coverage_internal.info 内容:"
    cat $TDINTERNAL_DIR/coverage_internal.info | grep SF

    # push result to https://app.codecov.io/
    echo "开始上传覆盖率数据到 Codecov..."
    echo "branch: $branch"
    echo "coverage_internal.info: $TDINTERNAL_DIR/coverage_internal.info"
    codecov -t 88ed2789-23be-455d-acb6-3f729d285d1c \
        -f $TDINTERNAL_DIR/coverage_internal.info \
        -b $branch \
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

lcovFunc

COVERAGE_INFO="$TDINTERNAL_DIR/coverage.info"
OUTPUT_DIR="$CAPTURE_GCDA_DIR/coverage_report"
# Generate local HTML reports
genhtml "$COVERAGE_INFO"  --branch-coverage --function-coverage --output-directory "$OUTPUT_DIR"


print_color "$GREEN" "End of coverage test on workflow!"

echo "For more details: https://app.codecov.io/github/taosdata/TDinternal"
