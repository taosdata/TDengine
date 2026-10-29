#!/bin/bash

# Copyright (c) TAOS Data, Inc. All rights reserved.
#
# SPDX-License-Identifier: LicenseRef-TAOSData-Commercial
#
# This file is proprietary to TAOS Data, Inc. and may contain confidential
# information. No rights are granted by this file. Use is permitted only
# under a written agreement with TAOS Data, Inc. or an authorized affiliate.
# Unauthorized copying, modification, distribution, or disclosure, in whole
# or in part, by any medium, is strictly prohibited.

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default config
EDITION="enterprise"   # default edition
OLD_VERSION_OSS="tdengine/tsdb:3.3.7.5"
NEW_VERSION_OSS="tdengine/tsdb-arm64:3.3.8.4.1129"
OLD_VERSION_EE="tdengine/tsdb-ee:3.3.7.5"
NEW_VERSION_EE="tdengine/tsdb-ee-arm64:3.3.8.4.1130"
TEST_PASSWORD="Abc123!@#"
DATA_DIR="$HOME/data/taos/test"
LOG_DIR="./test-logs"
# 超时配置
TAOSD_STARTUP_TIMEOUT=60
PROCESS_CHECK_TIMEOUT=60
# taosBenchmark 配置
BENCHMARK_TABLES=324
BENCHMARK_ROWS=1000



show_help() {
    echo "Usage: $0 [--edition|-e oss|enterprise] [case_numbers...]"
    echo ""
    echo "Options:"
    echo "  --edition, -e   Specify TDengine edition: oss or enterprise (default: enterprise)"
    echo "  --help, -h      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                  # Run all cases with enterprise edition"
    echo "  $0 -e oss 2 3       # Run case 2 and 3 with oss edition"
    echo "  $0 --help           # Show help"
}


# 工具函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $1"
}

cleanup() {
    local container_name=$1
    log_info "清理容器: $container_name"
    docker stop $container_name 2>/dev/null || true
    docker rm $container_name 2>/dev/null || true
}

cleanup_data() {
    log_info "清理测试数据目录"
    rm -rf $DATA_DIR
    mkdir -p $DATA_DIR
}

# 新增：检查 TDengine 相关进程
check_tdengine_processes() {
    local container=$1
    local max_wait=${2:-$PROCESS_CHECK_TIMEOUT}
    local count=0
    local interval=10
    local max_process=${3:-${#expected_processes[@]}}

    log_info "检查 TDengine 进程 (最多等待 ${max_wait}s)..."

    while [ $count -lt $max_wait ]; do
        local found_count=0
        local process_output
        process_output=$(docker exec $container ps aux 2>/dev/null || echo "")

        for proc in "${expected_processes[@]}"; do
            if echo "$process_output" | grep -w "$proc" >/dev/null; then
                log_info "  ✓ $proc 进程已启动"
                found_count=$((found_count + 1))
            else
                log_warn "  ✗ $proc 进程未找到"
            fi
        done

        log_info "已找到 $found_count/$max_process 个必要进程"
        if [ "$found_count" -ge "$max_process" ]; then
            log_info "✓ 所有必要进程已启动"
            return 0
        fi

        sleep $interval
        count=$((count + interval))
    done

    log_error "进程检查超时 (${max_wait}s)，未发现所有必要进程"
    return 1
}

# 增强的 wait_for_taosd 函数
wait_for_taosd() {
    local container=$1
    local password=$2
    local max_wait=${3:-$TAOSD_STARTUP_TIMEOUT}
    local count=0
    
    log_info "等待 taosd 启动 (最多等待 ${max_wait}s)..."
    
    while [ $count -lt $max_wait ]; do
        if docker exec $container taos -s "show databases;" -p"$password" &>/dev/null; then
            log_info "✓ taosd 启动成功，数据库连接正常"
            return 0
        fi
        sleep 2
        count=$((count + 2))
        
        # 每 10 秒输出一次等待状态
        if [ $((count % 10)) -eq 0 ]; then
            log_debug "等待中... (${count}s/${max_wait}s)"
        fi
    done
    
    log_error "taosd 启动超时 (${max_wait}s)"
    
    # 输出容器日志帮助调试
    log_error "容器日志 (最后 50 行):"
    docker logs --tail 50 $container 2>&1 | sed 's/^/  /'
    
    return 1
}

# 综合启动验证函数
verify_startup() {
    local container=$1
    local password=$2
    local version_label=$3
    
    log_info "========== 验证 $version_label 启动状态 =========="
    
    # 1. 检查进程
    if ! check_tdengine_processes $container $PROCESS_CHECK_TIMEOUT; then
        log_error "进程检查失败"
        return 1
    fi
    
    # 2. 检查数据库连接
    if ! wait_for_taosd $container "$password" $TAOSD_STARTUP_TIMEOUT; then
        log_error "数据库连接失败"
        return 1
    fi
    
    # 3. 检查版本信息
    log_info "查询版本信息:"
    docker exec $container taos -s "select server_version();" -p"$password" 2>/dev/null | sed 's/^/  /'
    
    # 4. 检查集群状态 (如果适用)
    log_info "查询 dnode 状态:"
    docker exec $container taos -s "show dnodes;" -p"$password" 2>/dev/null | sed 's/^/  /'
    
    log_info "✓ $version_label 启动验证通过"
    return 0
}

verify_password() {
    local container=$1
    local password=$2
    
    log_info "验证密码: $password"
    
    # 测试正确密码
    if docker exec $container taos -s "show databases;" -p"$password" &>/dev/null; then
        log_info "✓ 正确密码可以登录"

    else
        log_error "✗ 正确密码无法登录"
        return 1
    fi
    
    # 测试错误密码
    if docker exec $container taos -s "show databases;" -p"wrongpassword" &>/dev/null; then
        log_error "✗ 错误密码可以登录(安全问题)"
        return 1
    else
        log_info "✓ 错误密码被正确拒绝"
    fi
    
    return 0
}

change_password() {
    local container=$1
    local old_pass=$2
    local new_pass=$3
    
    log_info "修改密码: $old_pass -> $new_pass"
    docker exec $container taos -s "ALTER USER root PASS '$new_pass';" -p"$old_pass"
}

# 新增：使用 taosBenchmark 写入测试数据
run_benchmark() {
    local container=$1
    local password=$2
    local tables=${3:-$BENCHMARK_TABLES}
    local rows=${4:-$BENCHMARK_ROWS}

    log_info "========== 运行 taosBenchmark 写入测试数据 =========="
    log_info "表数量: $tables"
    log_info "每表行数: $rows"
    log_info "预计总行数: $((tables * rows))"

    local benchmark_cmd="taosBenchmark -t $tables -n $rows -y"
    if [ "$password" != "taosdata" ]; then
        benchmark_cmd="$benchmark_cmd -p$password"
    fi

    log_info "执行命令: $benchmark_cmd"

    docker exec $container bash -c "$benchmark_cmd" 2>&1 | tee /tmp/benchmark_output.log
    local exit_code=${PIPESTATUS[0]}

    # 检查日志中是否有 ERROR
    if grep -q "ERROR:" /tmp/benchmark_output.log; then
        log_error "✗ taosBenchmark 执行失败（检测到 ERROR）"
        cat /tmp/benchmark_output.log
        return 1
    fi

    if [ $exit_code -eq 0 ]; then
        log_info "✓ taosBenchmark 执行成功"
        # ...统计信息解析...
        return 0
    else
        log_error "✗ taosBenchmark 执行失败（退出码 $exit_code）"
        cat /tmp/benchmark_output.log
        return 1
    fi
}

# 新增：验证数据完整性
verify_benchmark_data() {
    local container=$1
    local password=$2
    local expected_tables=${3:-$BENCHMARK_TABLES}
    local expected_rows=${4:-$BENCHMARK_ROWS}
    
    log_info "========== 验证数据完整性 =========="
    log_info "期望表数量: $expected_tables"
    log_info "期望每表行数: $expected_rows"
    
    # 1. 检查数据库是否存在
    log_info "检查 test 数据库..."
    local db_exists
    db_exists=$(docker exec $container taos -s "show databases;" -p"$password" 2>/dev/null | grep -c "test" || echo "0")
    
    if [ "$db_exists" -eq 0 ]; then
        log_error "✗ test 数据库不存在"
        docker exec $container taos -s "show databases;" -p"$password" 2>/dev/null
        return 1
    fi
    log_info "✓ test 数据库存在"
    
    # 2. 检查超级表
    log_info "检查超级表 meters..."
    local stable_exists
    stable_exists=$(docker exec $container taos -s "use test; show stables;" -p"$password" 2>/dev/null | grep -c "meters" || echo "0")
    
    if [ "$stable_exists" -eq 0 ]; then
        log_error "✗ 超级表 meters 不存在"
        docker exec $container taos -s "use test; show stables;" -p"$password" 2>/dev/null
        return 1
    fi
    log_info "✓ 超级表 meters 存在"
    
    # 3. 检查子表数量（最简单方法：直接找包含数字和 | 的行）
    log_info "检查子表数量..."
    
    local temp_file="/tmp/tdengine_table_count_$$.txt"
    docker exec $container taos -s "use test; select count(*) from information_schema.ins_tables where stable_name='meters';" -p"$password" 2>/dev/null > "$temp_file"
    
    # 调试：显示原始查询结果
    log_debug "子表查询结果:"
    cat "$temp_file" | sed 's/^/    /'
    
    # 方法1：找到包含 | 的数据行（排除表头、分隔线、Query OK 行）
    # 数据行格式：                      3243 |
    local actual_tables=$(grep -E '^\s+[0-9]+\s+\|' "$temp_file" | head -1 | awk '{print $1}')
    
    # 方法2（备用）：找到最后一个包含 | 的行，排除 "Query OK" 行
    if [ -z "$actual_tables" ]; then
        log_debug "使用备用提取方法..."
        actual_tables=$(grep '|' "$temp_file" | grep -v 'Query OK' | grep -v 'count' | grep -v '=' | tail -1 | awk '{print $1}')
    fi
    
    rm -f "$temp_file"
    
    log_info "实际子表数量: $actual_tables"
    
    # 验证数字有效性
    if ! [[ "$actual_tables" =~ ^[0-9]+$ ]]; then
        log_error "✗ 无法获取有效的子表数量: '$actual_tables'"
        return 1
    fi
    
    if [ "$actual_tables" -ne "$expected_tables" ]; then
        log_warn "⚠ 子表数量不匹配 (期望: $expected_tables, 实际: $actual_tables)"
    else
        log_info "✓ 子表数量正确"
    fi
    
    # 4. 检查数据行数（使用相同的简单方法）
    log_info "检查数据总行数..."
    
    local temp_file2="/tmp/tdengine_row_count_$$.txt"
    docker exec $container taos -s "use test; select count(*) from meters;" -p"$password" 2>/dev/null > "$temp_file2"
    
    # 调试：显示原始查询结果
    log_debug "行数查询结果:"
    cat "$temp_file2" | sed 's/^/    /'
    
    # 方法1：找到包含 | 的数据行
    local actual_rows=$(grep -E '^\s+[0-9]+\s+\|' "$temp_file2" | head -1 | awk '{print $1}')
    
    # 方法2（备用）
    if [ -z "$actual_rows" ]; then
        log_debug "使用备用提取方法..."
        actual_rows=$(grep '|' "$temp_file2" | grep -v 'Query OK' | grep -v 'count' | grep -v '=' | tail -1 | awk '{print $1}')
    fi
    
    rm -f "$temp_file2"
    
    local expected_total=$((expected_tables * expected_rows))
    
    log_info "实际数据行数: $actual_rows"
    log_info "期望数据行数: $expected_total"
    
    # 验证数字有效性
    if ! [[ "$actual_rows" =~ ^[0-9]+$ ]]; then
        log_error "✗ 无法获取有效的数据行数: '$actual_rows'"
        return 1
    fi
    
    if [ "$actual_rows" -ne "$expected_total" ]; then
        log_warn "⚠ 数据行数不匹配 (期望: $expected_total, 实际: $actual_rows)"
        # 计算差异百分比
        local diff=$((actual_rows - expected_total))
        local diff_abs=${diff#-}
        if [ "$expected_total" -gt 0 ]; then
            local diff_percent=$((diff_abs * 100 / expected_total))
            log_warn "   差异: $diff 行 ($diff_percent%)"
        fi
    else
        log_info "✓ 数据行数正确"
    fi
    
    # 5. 抽样检查数据
    log_info "抽样检查数据..."
    local sample_data=$(docker exec $container taos -s "use test; select * from meters limit 5;" -p"$password" 2>/dev/null)
    
    if [ -n "$sample_data" ]; then
        log_info "✓ 数据可以正常查询"
        log_debug "样本数据:"
        echo "$sample_data" | sed 's/^/    /'
    else
        log_error "✗ 数据查询失败"
        return 1
    fi
    
    # 6. 检查数据统计信息
    log_info "检查数据统计..."
    local stats_result=$(docker exec $container taos -s "use test; select count(*), avg(current), max(voltage), min(phase) from meters;" -p"$password" 2>/dev/null)
    
    if [ -n "$stats_result" ]; then
        log_info "统计信息:"
        echo "$stats_result" | sed 's/^/  /'
    else
        log_warn "⚠ 统计信息查询失败"
    fi
    
    # 7. 输出摘要
    log_info "========== 数据验证摘要 =========="
    log_info "数据库: test ✓"
    log_info "超级表: meters ✓"
    log_info "子表数量: $actual_tables / $expected_tables"
    log_info "数据行数: $actual_rows / $expected_total"
    
    log_info "✓ 数据验证完成"
    return 0
}

# 新增：对比升级前后数据
compare_data_before_after() {
    local before_file=$1
    local after_file=$2
    
    log_info "========== 对比升级前后数据 =========="
    
    if [ ! -f "$before_file" ] || [ ! -f "$after_file" ]; then
        log_error "数据文件不存在"
        log_error "Before file: $before_file (exists: $([ -f "$before_file" ] && echo 'yes' || echo 'no'))"
        log_error "After file: $after_file (exists: $([ -f "$after_file" ] && echo 'yes' || echo 'no'))"
        return 1
    fi
    
    # 使用更通用的 grep -E 而不是 grep -oP
    local before_count=$(grep "实际数据行数:" "$before_file" | grep -oE '[0-9]+' | tail -1 || echo "0")
    local after_count=$(grep "实际数据行数:" "$after_file" | grep -oE '[0-9]+' | tail -1 || echo "0")
    
    log_info "升级前数据行数: $before_count"
    log_info "升级后数据行数: $after_count"
    
    # 验证数字有效性
    if ! [[ "$before_count" =~ ^[0-9]+$ ]] || ! [[ "$after_count" =~ ^[0-9]+$ ]]; then
        log_error "✗ 无法提取有效的数据行数"
        log_debug "Before count: '$before_count'"
        log_debug "After count: '$after_count'"
        return 1
    fi
    
    if [ "$before_count" -eq "$after_count" ] && [ "$before_count" -gt 0 ]; then
        log_info "✓ 数据行数一致，升级未丢失数据"
        return 0
    elif [ "$before_count" -eq 0 ] || [ "$after_count" -eq 0 ]; then
        log_error "✗ 数据行数为 0，验证失败"
        return 1
    else
        log_error "✗ 数据行数不一致，可能存在数据丢失"
        local diff=$((after_count - before_count))
        log_error "   差异: $diff 行"
        return 1
    fi
}

# 测试用例 1: 旧版本修改密码，升级新版本 (增强版)
test_1_upgrade_with_password_change() {
    log_info "========== 测试 1: 旧版本修改密码，升级新版本 (含数据验证) =========="
    local container="test1_upgrade"
    
    cleanup $container
    cleanup_data
    
    # 启动旧版本
    log_info "启动旧版本: $OLD_VERSION"
    docker run -d \
        -v $DATA_DIR:/var/lib/taos \
        -p 6030:6030 -p 6041:6041 -p 6043:6043 \
        -p 6044-6049:6044-6049 -p 6044-6045:6044-6045/udp \
        -p 6060:6060 \
        --hostname test-tsdb \
        --name $container \
        $OLD_VERSION
    
    # 验证旧版本启动
    verify_startup $container "taosdata" "旧版本"
    
    # 修改密码
    change_password $container "taosdata" "$TEST_PASSWORD"
    verify_password $container "$TEST_PASSWORD"
    
    # 写入测试数据
    run_benchmark $container "$TEST_PASSWORD"
    
    # 验证数据（升级前）
    verify_benchmark_data $container "$TEST_PASSWORD" | tee /tmp/test1_before_upgrade.log
    
    # 升级到新版本
    log_info "升级到新版本: $NEW_VERSION"
    cleanup $container
    
    docker run -d \
        -v $DATA_DIR:/var/lib/taos \
        -p 6030:6030 -p 6041:6041 -p 6043:6043 \
        -p 6044-6049:6044-6049 -p 6044-6045:6044-6045/udp \
        -p 6060:6060 \
        -e TAOS_ROOT_PASSWORD="$TEST_PASSWORD" \
        --hostname test-tsdb \
        --name $container \
        $NEW_VERSION
    
    # 验证新版本启动
    verify_startup $container "$TEST_PASSWORD" "新版本"
    verify_password $container "$TEST_PASSWORD"
    
    # 验证数据（升级后）
    verify_benchmark_data $container "$TEST_PASSWORD" | tee /tmp/test1_after_upgrade.log
    
    # 对比升级前后数据
    compare_data_before_after /tmp/test1_before_upgrade.log /tmp/test1_after_upgrade.log
    
    cleanup $container
    log_info "✓✓✓ 测试 1: 通过 ✓✓✓"
}

# 测试用例 2: 初始启动单节点，第一次设置密码 (增强版)
test_2_fresh_start_with_password() {
    log_info "========== 测试 2: 初始启动单节点，第一次设置密码 (含数据验证) =========="
    local container="test2_fresh"
    
    cleanup $container
    cleanup_data
    
    log_info "启动新版本并设置密码"
    docker run -d \
        -v $DATA_DIR:/var/lib/taos \
        -p 6030:6030 -p 6041:6041 -p 6043:6043 \
        -p 6044-6049:6044-6049 -p 6044-6045:6044-6045/udp \
        -p 6060:6060 \
        --hostname test-tsdb \
        -e TAOS_ROOT_PASSWORD="$TEST_PASSWORD" \
        --name $container \
        $NEW_VERSION
    
    verify_startup $container "$TEST_PASSWORD" "新版本(带密码)"
    verify_password $container "$TEST_PASSWORD"
    
    # 写入和验证数据
    run_benchmark $container "$TEST_PASSWORD"
    verify_benchmark_data $container "$TEST_PASSWORD"
    
    cleanup $container
    log_info "✓✓✓ 测试 2: 通过 ✓✓✓"
}

# 测试用例 3: 初始启动集群模式，第一次设置密码 (增强版)
test_3_cluster_fresh_with_password() {
    log_info "========== 测试 3: 初始启动集群模式，第一次设置密码 (含数据验证) =========="
    
    cleanup_data
    docker-compose -f /tmp/test3-compose.yaml down -v

    # 准备初始化 SQL 文件
    log_info "准备初始化 SQL 文件"
    echo "CREATE DATABASE IF NOT EXISTS init_test;" > /tmp/init.sql

    # 创建 docker-compose 文件，挂载初始化 SQL
    cat > /tmp/test3-compose.yaml <<EOF
services:
  td1:
    image: $NEW_VERSION
    container_name: test3_td1
    environment:
      - TAOS_FQDN=td1
      - TAOS_ROOT_PASSWORD=$TEST_PASSWORD
      - EXPLORER_SKIP_REGISTER=true
      - TAOS_FIRST_EP=td1:6030
    ports:
      - "6030:6030"
      - "6041:6041"
    volumes:
      - $DATA_DIR/td1:/var/lib/taos
      - /tmp/init.sql:/docker-entrypoint-initdb.d/init.sql
      
  td2:
    image: $NEW_VERSION
    container_name: test3_td2
    environment:
      - TAOS_FQDN=td2
      - TAOS_ROOT_PASSWORD=$TEST_PASSWORD
      - TAOS_FIRST_EP=td1:6030
    volumes:
      - $DATA_DIR/td2:/var/lib/taos
      
  td3:
    image: $NEW_VERSION
    container_name: test3_td3
    environment:
      - TAOS_FQDN=td3
      - TAOS_ROOT_PASSWORD=$TEST_PASSWORD
      - TAOS_FIRST_EP=td1:6030
    volumes:
      - $DATA_DIR/td3:/var/lib/taos
EOF
    
    log_info "启动集群"
    docker-compose -f /tmp/test3-compose.yaml up -d
    
    # 验证主节点启动
    verify_startup test3_td1 "$TEST_PASSWORD" "集群主节点"
    verify_password test3_td1 "$TEST_PASSWORD"
    
    # 验证集群状态
    log_info "验证集群状态"
    # docker exec test3_td1 taos -s "show dnodes;" -p"$TEST_PASSWORD" 

    cluster_status=$(docker exec test3_td1 taos -s "show dnodes;" -p"$TEST_PASSWORD")
    if echo "$cluster_status" | grep -q "td1" && \
    echo "$cluster_status" | grep -q "td2" && \
    echo "$cluster_status" | grep -q "td3"; then
        log_info "✓ 集群节点 td1、td2、td3 均已发现"
    else
        log_error "✗ 集群节点验证失败，缺少 td1/td2/td3"
        echo "$cluster_status"
        exit 1
    fi

    # 检查主节点 snode
    log_info "检查主节点 snode 是否存在"
    docker exec test3_td1 taos -s "show snodes;" -p"$TEST_PASSWORD" 2>/dev/null | grep -q "snode"
    if [ $? -eq 0 ]; then
        log_info "✓ 主节点 snode 已存在"
    else
        log_error "✗ 主节点 snode 不存在"
    fi

    # 检查 run_init_scripts 执行初始化 SQL（主节点）
    log_info "检查主节点初始化 SQL"
    docker exec test3_td1 taos -s "show databases;" -p"$TEST_PASSWORD" | grep -q "init_test"
    if [ $? -eq 0 ]; then
        log_info "✓ 主节点初始化 SQL 已执行，init_test 数据库已创建"
    else
        log_error "✗ 主节点初始化 SQL 未执行或数据库未创建"
    fi

    # 检查从节点进程
    log_info "检查从节点 td2 进程"
    check_tdengine_processes test3_td2 60

    log_info "检查从节点 td3 进程"
    check_tdengine_processes test3_td3 60

    # 检查从节点 snode（理论上只有主节点有 snode，从节点可选查验）
    log_info "检查从节点 td2 snode"
    docker exec test3_td2 taos -s "show snodes;" -p"$TEST_PASSWORD" 2>/dev/null | grep -q "snode"
    if [ $? -eq 0 ]; then
        log_info "✓ 从节点 td2 snode 已存在"
    else
        log_error "✗ 从节点 td2 snode 不存在（正常）"
    fi

    log_info "检查从节点 td3 snode"
    docker exec test3_td3 taos -s "show snodes;" -p"$TEST_PASSWORD" 2>/dev/null | grep -q "snode"
    if [ $? -eq 0 ]; then
        log_info "✓ 从节点 td3 snode 已存在"
    else
        log_error "✗ 从节点 td3 snode 不存在（正常）"
    fi

    # 检查从节点初始化 SQL（理论上只在主节点执行，其他节点可选查验）
    log_info "检查从节点 td2 初始化 SQL"
    docker exec test3_td2 taos -s "show databases;" -p"$TEST_PASSWORD" | grep -q "init_test"
    if [ $? -eq 0 ]; then
        log_info "✓ 从节点 td2 init_test 数据库已创建"
    else
        log_error "✗ 从节点 td2 init_test 数据库未创建"
    fi

    log_info "检查从节点 td3 初始化 SQL"
    docker exec test3_td3 taos -s "show databases;" -p"$TEST_PASSWORD" | grep -q "init_test"
    if [ $? -eq 0 ]; then
        log_info "✓ 从节点 td3 init_test 数据库已创建"
    else
        log_error "✗ 从节点 td3 init_test 数据库未创建"
    fi

    # 写入和验证数据
    run_benchmark test3_td1 "$TEST_PASSWORD"
    verify_benchmark_data test3_td1 "$TEST_PASSWORD"
    
    docker-compose -f /tmp/test3-compose.yaml down -v
    log_info "✓✓✓ 测试 3: 通过 ✓✓✓"
}

# 测试用例 4: 旧版本三节点集群，升级新版本，启动后镜像正常，taos 客户端修改密码后，重启验证生效
test_4_cluster_upgrade_and_client_password_change() {
    log_info "========== 用例4: 旧版三节点集群升级新版本，客户端改密码后重启验证 =========="
    cleanup_data
    docker-compose -f /tmp/test4-old-compose.yaml down -v
    docker-compose -f /tmp/test4-new-compose.yaml down -v
    # 启动旧版本三节点集群
    cat > /tmp/test4-old-compose.yaml <<EOF
services:
  td1:
    image: $OLD_VERSION
    container_name: test4_td1
    environment:
      - TAOS_FQDN=td1
    ports:
      - "6030:6030"
    volumes:
      - $DATA_DIR/td1:/var/lib/taos
  td2:
    image: $OLD_VERSION
    container_name: test4_td2
    environment:
      - TAOS_FQDN=td2
      - TAOS_FIRST_EP=td1:6030
    volumes:
      - $DATA_DIR/td2:/var/lib/taos
  td3:
    image: $OLD_VERSION
    container_name: test4_td3
    environment:
      - TAOS_FQDN=td3
      - TAOS_FIRST_EP=td1:6030
    volumes:
      - $DATA_DIR/td3:/var/lib/taos
EOF

    docker-compose -f /tmp/test4-old-compose.yaml up -d
    verify_startup test4_td1 "taosdata" "旧版本集群"
    # 写入测试数据 企业版需要等待 20s 让集群稳定
    sleep 20
    run_benchmark test4_td1 taosdata
    verify_benchmark_data test4_td1 taosdata | tee /tmp/test4_before_upgrade.log
    # 升级到新版本
    docker-compose -f /tmp/test4-old-compose.yaml down 
    cat > /tmp/test4-new-compose.yaml <<EOF
services:
  td1:
    image: $NEW_VERSION
    container_name: test4_td1
    environment:
      - TAOS_FQDN=td1
    ports:
      - "6030:6030"
    volumes:
      - $DATA_DIR/td1:/var/lib/taos
  td2:
    image: $NEW_VERSION
    container_name: test4_td2
    environment:
      - TAOS_FQDN=td2
      - TAOS_FIRST_EP=td1:6030
    volumes:
      - $DATA_DIR/td2:/var/lib/taos
  td3:
    image: $NEW_VERSION
    container_name: test4_td3
    environment:
      - TAOS_FQDN=td3
      - TAOS_FIRST_EP=td1:6030
    volumes:
      - $DATA_DIR/td3:/var/lib/taos
EOF

    docker-compose -f /tmp/test4-new-compose.yaml up -d
    verify_startup test4_td1 "taosdata" "新版本集群"

    # 用 taos 客户端修改密码
    change_password test4_td1 "taosdata" "$TEST_PASSWORD"

    # 重启主节点
    docker restart test4_td1
    sleep 10

    # 验证新密码生效
    verify_password test4_td1 "$TEST_PASSWORD"
    verify_benchmark_data test4_td1 "$TEST_PASSWORD" | tee /tmp/test4_after_upgrade.log
    
    # 验证集群状态
    log_info "验证集群状态"
    docker exec test4_td1 taos -s "show dnodes;" -p"$TEST_PASSWORD" 

    # 清理
    docker-compose -f /tmp/test4-new-compose.yaml down -v
    log_info "✓✓✓ 用例4: 通过 ✓✓✓"
}

# 测试用例 5: 旧版本三节点集群，升级新版本，启动后镜像正常，环境变量修改密码，对镜像不起作用，密码不变化
test_5_cluster_upgrade_env_password_fail() {
    log_info "========== 用例5: 旧版三节点集群升级新版本，环境变量改密码无效 =========="
    cleanup_data
    docker-compose -f /tmp/test5-old-compose.yaml down -v
    docker-compose -f /tmp/test5-new-compose.yaml down -v
    # 启动旧版本三节点集群
    cat > /tmp/test5-old-compose.yaml <<EOF
services:
  td1:
    image: $OLD_VERSION
    container_name: test5_td1
    environment:
      - TAOS_FQDN=td1
    ports:
      - "6030:6030"
    volumes:
      - $DATA_DIR/td1:/var/lib/taos
  td2:
    image: $OLD_VERSION
    container_name: test5_td2
    environment:
      - TAOS_FQDN=td2
      - TAOS_FIRST_EP=td1:6030
    volumes:
      - $DATA_DIR/td2:/var/lib/taos
  td3:
    image: $OLD_VERSION
    container_name: test5_td3
    environment:
      - TAOS_FQDN=td3
      - TAOS_FIRST_EP=td1:6030
    volumes:
      - $DATA_DIR/td3:/var/lib/taos
EOF

    docker-compose -f /tmp/test5-old-compose.yaml up -d
    verify_startup test5_td1 "taosdata" "旧版本集群"
    sleep 20
    run_benchmark test5_td1 "taosdata"
    verify_benchmark_data test5_td1 "taosdata" | tee /tmp/test5_before_upgrade.log


    # 升级到新版本，尝试用环境变量修改密码
    docker-compose -f /tmp/test5-old-compose.yaml down
    cat > /tmp/test5-new-compose.yaml <<EOF
services:
  td1:
    image: $NEW_VERSION
    container_name: test5_td1
    environment:
      - TAOS_FQDN=td1
      - TAOS_ROOT_PASSWORD=$TEST_PASSWORD
    ports:
      - "6030:6030"
    volumes:
      - $DATA_DIR/td1:/var/lib/taos
  td2:
    image: $NEW_VERSION
    container_name: test5_td2
    environment:
      - TAOS_FQDN=td2
      - TAOS_FIRST_EP=td1:6030
      - TAOS_ROOT_PASSWORD=$TEST_PASSWORD
    volumes:
      - $DATA_DIR/td2:/var/lib/taos
  td3:
    image: $NEW_VERSION
    container_name: test5_td3
    environment:
      - TAOS_FQDN=td3
      - TAOS_FIRST_EP=td1:6030
      - TAOS_ROOT_PASSWORD=$TEST_PASSWORD
    volumes:
      - $DATA_DIR/td3:/var/lib/taos
EOF

    docker-compose -f /tmp/test5-new-compose.yaml up -d
    verify_startup test5_td1 "taosdata" "新版本集群"

    # 验证密码未变化（仍为 taosdata）
    verify_password test5_td1 "taosdata"
    verify_benchmark_data test5_td1 "taosdata" | tee /tmp/test5_after_upgrade.log

    # 验证集群状态
    log_info "验证集群状态"
    docker exec test5_td1 taos -s "show dnodes;" -p"taosdata" 

    # 清理环境
    docker-compose -f /tmp/test5-new-compose.yaml down -v 
    log_info "✓✓✓ 用例5: 通过 ✓✓✓"
}

# 测试用例 6: 初始启动新版本单节点，停止后重启并用环境变量设置密码，密码不变，taoskeeper能正常启动
test_6_single_node_env_password_fail_after_restart() {
    log_info "========== 用例6: 新版单节点重启后用环境变量改密码无效，taoskeeper正常 =========="
    local container="test6_single_env_fail"
    local container2="test6_single_env_fail_restart"

    cleanup $container
    cleanup $container2
    cleanup_data

    # 准备初始化 SQL 文件并拷贝到容器
    log_info "准备初始化 SQL 文件"
    echo "CREATE DATABASE IF NOT EXISTS init_test;" > /tmp/init.sql

    # 第一次启动新版本单节点（默认密码）
    log_info "首次启动新版本单节点（默认密码 taosdata）"
    docker run -d \
        -v $DATA_DIR:/var/lib/taos \
        -v /tmp/init.sql:/docker-entrypoint-initdb.d/init.sql \
        -p 6030:6030 -p 6041:6041 -p 6043:6043 \
        --hostname test-tsdb \
        --name $container \
        $NEW_VERSION

    verify_startup $container "taosdata" "新版本单节点(首次)"
    verify_password $container "taosdata"

    # 检查 snode 是否存在
    log_info "检查 snode 是否存在"
    docker exec $container taos -s "show snodes;" -p"taosdata" 2>/dev/null | grep -q "snode"
    if [ $? -eq 0 ]; then
        log_info "✓ snode 已存在"
    else
        log_error "✗ snode 不存在"
    fi

    # 检查 run_init_scripts 执行初始化 SQL
    docker exec $container taos -s "show databases;" -p"taosdata" | grep -q "init_test"
    if [ $? -eq 0 ]; then
        log_info "✓ 初始化 SQL 已执行，init_test 数据库已创建"
    else
        log_error "✗ 初始化 SQL 未执行或数据库未创建"
    fi

    # 停止容器
    log_info "停止容器"
    docker stop $container

    # 用环境变量尝试修改密码后重启（实际不会生效）
    log_info "重启容器并尝试用环境变量设置密码（应无效）"
    docker run -d \
        -v "$DATA_DIR":/var/lib/taos \
        -p 6030:6030 -p 6041:6041 -p 6043:6043 \
        --hostname test-tsdb \
        -e TAOS_ROOT_PASSWORD="$TEST_PASSWORD" \
        --name $container2 \
        $NEW_VERSION

    verify_startup $container2 "taosdata" "新版本单节点(重启)"
    verify_password $container2 "taosdata"

    # 检查 snode 是否存在（重启后）
    log_info "重启后再次检查 snode 是否存在"
    docker exec $container2 taos -s "show snodes;" -p"taosdata" 2>/dev/null | grep -q "snode"
    if [ $? -eq 0 ]; then
        log_info "✓ snode 仍然存在"
    else
        log_error "✗ snode 不存在"
    fi

    # 检查初始化 SQL 是否依然存在
    log_info "重启后检查 init_test 数据库是否存在"
    docker exec $container2 taos -s "show databases;" -p"taosdata" | grep -q "init_test"
    if [ $? -eq 0 ]; then
        log_info "✓ init_test 数据库依然存在"
    else
        log_error "✗ init_test 数据库丢失或未创建"
    fi

    # 检查 taoskeeper 是否正常启动
    log_info "检查 taoskeeper 进程"
    docker exec $container2 bash -c 'ps aux | grep taoskeeper | grep -v grep'
    
    cleanup $container
    cleanup $container2
    log_info "✓✓✓ 用例6: 通过 ✓✓✓"
}



main() {
    mkdir -p $LOG_DIR

    log_info "================================================"
    log_info "  TDengine Docker 自动化测试套件 ($EDITION 版本)"
    log_info "================================================"
    log_info "旧版本: $OLD_VERSION"
    log_info "新版本: $NEW_VERSION"
    log_info "测试密码: $TEST_PASSWORD"
    log_info "taosBenchmark 配置:"
    log_info "  - 表数量: $BENCHMARK_TABLES"
    log_info "  - 每表行数: $BENCHMARK_ROWS"
    log_info "  - 预计总行数: $((BENCHMARK_TABLES * BENCHMARK_ROWS))"
    log_info "进程检查超时: ${PROCESS_CHECK_TIMEOUT}s"
    log_info "数据库启动超时: ${TAOSD_STARTUP_TIMEOUT}s"
    log_info "==========================================="
    echo ""

    test_results=()

    # 用例列表
    cases=(1 2 3 4 5 6)
    if [ $# -gt 0 ]; then
        cases=("$@")
    fi

    for case in "${cases[@]}"; do
        case $case in
            1)
                test_1_upgrade_with_password_change 2>&1 | tee $LOG_DIR/test1.log
                status=${PIPESTATUS[0]}
                [ $status -eq 0 ] && test_results+=("测试1:成功") || test_results+=("测试1:失败")
                ;;
            2)
                test_2_fresh_start_with_password 2>&1 | tee $LOG_DIR/test2.log
                status=${PIPESTATUS[0]}
                [ $status -eq 0 ] && test_results+=("测试2:成功") || test_results+=("测试2:失败")
                ;;
            3)
                test_3_cluster_fresh_with_password 2>&1 | tee $LOG_DIR/test3.log
                status=${PIPESTATUS[0]}
                [ $status -eq 0 ] && test_results+=("测试3:成功") || test_results+=("测试3:失败")
                ;;
            4)
                test_4_cluster_upgrade_and_client_password_change 2>&1 | tee $LOG_DIR/test4.log
                status=${PIPESTATUS[0]}
                [ $status -eq 0 ] && test_results+=("测试4:成功") || test_results+=("测试4:失败")
                ;;
            5)
                test_5_cluster_upgrade_env_password_fail 2>&1 | tee $LOG_DIR/test5.log
                status=${PIPESTATUS[0]}
                [ $status -eq 0 ] && test_results+=("测试5:成功") || test_results+=("测试5:失败")
                ;;
            6)
                test_6_single_node_env_password_fail_after_restart 2>&1 | tee $LOG_DIR/test6.log
                status=${PIPESTATUS[0]}
                [ $status -eq 0 ] && test_results+=("测试6:成功") || test_results+=("测试6:失败")
                ;;
            *)
                log_warn "未知用例编号: $case"
                ;;
        esac
    done

    echo ""
    log_info "========================================"
    log_info "  所有测试完成！"
    log_info "========================================"
    log_info "测试日志保存在: $LOG_DIR/"
    echo ""
    log_info "测试结果汇总："
    for result in "${test_results[@]}"; do
        case "$result" in
            *成功)
                log_info "$result"
                ;;
            *)
                log_error "$result"
                ;;
        esac
    done
}

# Parse edition argument
while [[ $# -gt 0 ]]; do
    case "$1" in
        --edition|-e)
            EDITION="$2"
            shift 2
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            break
            ;;
    esac
done

# Select image versions based on edition
if [ "$EDITION" = "oss" ]; then
    OLD_VERSION="$OLD_VERSION_OSS"
    NEW_VERSION="$NEW_VERSION_OSS"
    expected_processes=("taosd" "taosudf" "taosadapter" "taos-explorer" "taoskeeper")
else
    OLD_VERSION="$OLD_VERSION_EE"
    NEW_VERSION="$NEW_VERSION_EE"
    expected_processes=("taosd" "taosudf" "taosadapter" "taos-explorer" "taoskeeper" "taosx")

fi

main "$@"