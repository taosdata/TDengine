#!/bin/bash

# 定义日志目录
LOG_DIR="test_logs"

# 清理日志目录
if [ -d "$LOG_DIR" ]; then
    rm -rf "$LOG_DIR"
fi
mkdir -p "$LOG_DIR"
rm -f stop.txt
#> "$LOG_DIR/case_result.txt"
#> "$LOG_DIR/run_tests.log"

# 检查是否提供了用例列表文件
if [ $# -ne 1 ]; then
    echo "Usage: $0 <test_case_list_file>" >> "$LOG_DIR/case_result.txt"
    exit 1
fi

test_case_list=$1

# 检查文件是否存在
if [ ! -f "$test_case_list" ]; then
    echo "Error: File $test_case_list does not exist." >> "$LOG_DIR/case_result.txt"
    exit 1
fi

# 记录开始时间
echo "Test execution started at: $(date)" >> "$LOG_DIR/case_result.txt"
echo "Test execution started at: $(date)" >> "$LOG_DIR/run_tests.log"

# 逐行读取文件
while IFS= read -r line || [[ -n "$line" ]]; do
    # 去除行首行尾空白字符
    line=$(echo "$line" | xargs)
    
    # 跳过空行和被注释的行
    if [ -z "$line" ] || [[ "$line" == \#* ]]; then
        continue
    fi
    
    # 检查是否包含有效的 pytest 命令
    if [[ ! "$line" =~ pytest\ .+ ]]; then
        echo "SKIPPED: Invalid pytest command - $line" >> "$LOG_DIR/case_result.txt"
        echo "SKIPPED: Invalid pytest command - $line" >> "$LOG_DIR/run_tests.log"
        continue
    fi
    
    # 提取测试文件路径（最后一个.py文件）
    test_file=$(echo "$line" | grep -oE '[^ ]+\.py' | tail -1)
    
    # 检查测试文件是否存在
    if [ -z "$test_file" ] || [ ! -f "$test_file" ]; then
        echo "ERROR: Test file not found in command - $line" >> "$LOG_DIR/case_result.txt"
        echo "ERROR: Test file not found in command - $line" >> "$LOG_DIR/run_tests.log"
        continue
    fi
    
    # 准备日志文件
    case_log="$LOG_DIR/case.log"
    > "$case_log"

    #清除工作目录
    rm -rf ../../sim
    
    # 执行pytest命令
    echo "Executing: $line" >> "$LOG_DIR/run_tests.log"
    start_time=$(date +%s)
    
    # 在子进程中执行pytest命令
    if eval "$line" > "$case_log" 2>&1; then
        # 检查执行结果
        if grep -q "successfully executed" "$case_log"; then
            result="PASS"
        elif grep -q "AsanFileSuccessLen: 1" "$case_log"; then
            result="PASS"
        else
            result="FAILED"
            # 保存失败日志
            cp "$case_log" "$LOG_DIR/${test_file//\//_}.log"
        fi
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        echo "$test_file $result (${duration}s) - Command: $line" >> "$LOG_DIR/case_result.txt"
        echo "$test_file $result (${duration}s) - Command: $line" >> "$LOG_DIR/run_tests.log"
    else
        # 异常退出
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        echo "$test_file ERROR (${duration}s) - Command: $line" >> "$LOG_DIR/case_result.txt"
        echo "$test_file ERROR (${duration}s) - Command: $line" >> "$LOG_DIR/run_tests.log"
        # 保存错误日志
        cp "$case_log" "$LOG_DIR/${test_file//\//_}.log"
    fi
    
    # 将用例日志追加到总日志
    cat "$case_log" >> "$LOG_DIR/run_tests.log"
    
    # 检查stop.txt
    if [ -f "stop.txt" ] && [ "$(cat stop.txt | xargs)" == "1" ]; then
        echo "Found stop.txt with content '1', stopping execution." >> "$LOG_DIR/case_result.txt"
        echo "Found stop.txt with content '1', stopping execution." >> "$LOG_DIR/run_tests.log"
        break
    fi
done < "$test_case_list"

# 记录结束时间
echo "Test execution finished at: $(date)" >> "$LOG_DIR/case_result.txt"
echo "Test execution finished at: $(date)" >> "$LOG_DIR/run_tests.log"

# 检查并生成Allure报告
if command -v allure &>/dev/null; then
    echo "Generating Allure report..." >> "$LOG_DIR/run_tests.log"
    allure generate --clean -o "$LOG_DIR/allure-report" >> "$LOG_DIR/run_tests.log" 2>&1
else
    echo "Allure command not found, skipping report generation." >> "$LOG_DIR/run_tests.log"
fi