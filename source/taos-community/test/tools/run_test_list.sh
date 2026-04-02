#!/bin/bash

set -e

SIM_DIR="sim"
LOG_DIR=""
RESULT_FILE=""
CMD_FILES=()
RUN_IN_FOREGROUND=0

usage() {
    echo "Usage:"
    echo "  $0 -f file1 [-f file2 ...] [-p]"
    echo "    -f <file>   指定命令文件（可多次）"
    echo "    -p          前台运行（默认后台）"
    echo "    -o <output_dir> 指定日志输出目录(默认 sim/)"
    echo "    -h, --help  显示帮助信息"
    echo "默认情况下，脚本会读取 test_list.txt 文件中的命令"
    echo "  并在 sim/ 目录下创建日志和结果文件"
    echo "  日志和结果在 sim/ 目录下"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -f)
            shift
            [[ -z "$1" ]] && { echo "Missing file after -f"; exit 1; }
            CMD_FILES+=("$1")
            shift
            ;;
        -p)
            RUN_IN_FOREGROUND=1
            shift
            ;;
        -o)
            shift
            [[ -z "$1" ]] && { echo "Missing directory after -o"; usage; exit 1; }
            SIM_DIR="$1"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        -*)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
        *)
            CMD_FILES+=("$1")
            shift
            ;;
    esac
done

LOG_DIR="$SIM_DIR/logs"
RESULT_FILE="$SIM_DIR/case_result.txt"

if [[ ${#CMD_FILES[@]} -eq 0 ]]; then
    CMD_FILES=("test_list.txt")
fi

mkdir -p "$LOG_DIR"
: > "$RESULT_FILE"

run_commands() {
    local idx=1
    for cmd_file in "${CMD_FILES[@]}"; do
        [[ ! -f "$cmd_file" ]] && { echo "Command file $cmd_file not found!"; continue; }
        while IFS= read -r line || [[ -n "$line" ]]; do
            [[ -z "$line" || "$line" =~ ^# ]] && continue
            log_file="$LOG_DIR/case_${idx}.log"
            echo "[$(date '+%F %T')] Running: $line" | tee -a "$RESULT_FILE"
            echo "日志文件: $log_file" | tee -a "$RESULT_FILE"
            if eval "$line" >"$log_file" 2>&1; then
                echo "[$(date '+%F %T')] [PASS] $line" | tee -a "$RESULT_FILE"
            else
                echo "[$(date '+%F %T')] [FAIL] $line" | tee -a "$RESULT_FILE"
            fi
            echo "-----------------------------" >> "$RESULT_FILE"
            ((idx++))
        done < "$cmd_file"
    done
}

if [[ $RUN_IN_FOREGROUND -eq 1 ]]; then
    run_commands
else
    (
        run_commands
    ) > "$SIM_DIR/run_tests.log" 2>&1 &
    echo "Test started in background. See:"
    echo "  $RESULT_FILE   # case execute results"
    echo "  $LOG_DIR/case_*.log   # each case output"
    echo "  $SIM_DIR/run_tests.log   # all output"
fi