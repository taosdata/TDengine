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

function collect_info_from_tests_single() {
    local test_log_dir="$1"
    
    if [ -z "$test_log_dir" ] || [ ! -d "$test_log_dir" ]; then
        echo "Test log directory does not exist or not specified, skipping info collection: $test_log_dir"
        return 0
    fi
    
    echo "=== 收集并合并所有测试 case 的覆盖率信息文件 === SINGLE"
    echo "源目录: $test_log_dir"
    
    # 查找所有 .info 文件
    echo "查找所有 .info 文件..."
    local info_files=$(find "$test_log_dir" -name "*.info" -type f 2>/dev/null)
    
    if [ -z "$info_files" ]; then
        echo "警告: 未找到任何 .info 文件"
        return 1
    fi
    
    local info_count=$(echo "$info_files" | wc -l)
    echo "找到 $info_count 个覆盖率信息文件"
    
    # 不使用 exclude.txt 过滤，直接使用所有文件
    filtered_info_files="$info_files"
    local filtered_count=$info_count
    echo "不使用 exclude.txt 过滤，直接使用所有 $filtered_count 个文件"
    
    # 检查是否有文件需要合并
    local final_info_count=$(echo "$filtered_info_files" | wc -l)
    if [ "$final_info_count" -eq 0 ]; then
        echo "错误: 没有有效的覆盖率信息文件需要处理"
        return 1
    fi
    
    echo "准备合并 $final_info_count 个覆盖率信息文件..."
    
    # 合并所有 .info 文件
    if [ "$final_info_count" -eq 1 ]; then
        # 只有一个文件，直接复制
        local single_file=$(echo "$filtered_info_files" | head -1)
        echo "只有一个覆盖率文件，直接使用: $(basename "$single_file")"
        cp "$single_file" "coverage_tdengine_raw.info"
    else
        # 多个文件，使用 lcov 合并 - 简单显示进度
        echo "使用 lcov 合并多个覆盖率文件..."
        
        # 创建临时文件列表
        local info_files_list=$(mktemp)
        echo "$filtered_info_files" > "$info_files_list"
        
        # 构建合并命令并显示文件列表
        local merge_cmd="lcov --quiet --rc lcov_branch_coverage=0"
        local file_index=0
        
        echo "合并文件列表:"
        while IFS= read -r info_file; do
            if [ -f "$info_file" ]; then
                ((file_index++))
                local file_size=$(stat -c%s "$info_file" 2>/dev/null || echo "0")
                echo "  [$file_index/$final_info_count] $(basename "$info_file") ($file_size 字节)"
                merge_cmd="$merge_cmd --add-tracefile '$info_file'"
            fi
        done < "$info_files_list"
        
        merge_cmd="$merge_cmd -o coverage_tdengine_raw.info"
        
        echo ""
        echo "开始执行合并..."
        local start_time=$(date +%s)
        
        if eval "$merge_cmd" 2>/dev/null; then
            local end_time=$(date +%s)
            local duration=$((end_time - start_time))
            echo "✓ 成功合并覆盖率信息 (用时: ${duration}秒)"
        else
            echo "✗ 合并覆盖率信息失败"
            rm -f "$info_files_list"
            return 1
        fi
        
        rm -f "$info_files_list"
    fi
    
    # 检查生成的文件
    if [ -s "coverage_tdengine_raw.info" ]; then
        local final_size=$(stat -c%s "coverage_tdengine_raw.info")
        local final_lines=$(wc -l < "coverage_tdengine_raw.info")
        local source_files=$(grep "^SF:" "coverage_tdengine_raw.info" | wc -l || echo "0")
        
        echo "✓ 成功生成合并后的覆盖率信息文件:"
        echo "  文件大小: $final_size 字节"
        echo "  文件行数: $final_lines 行"
        echo "  包含源文件数: $source_files 个"
        
        return 0
    else
        echo "✗ 生成的覆盖率信息文件为空或不存在"
        return 1
    fi
}

function collect_info_from_tests() {
    local test_log_dir="$1"
    
    if [ -z "$test_log_dir" ] || [ ! -d "$test_log_dir" ]; then
        echo "Test log directory does not exist or not specified, skipping info collection: $test_log_dir"
        return 0
    fi
    
    echo "=== 收集并合并所有测试 case 的覆盖率信息文件 === tests"
    echo "源目录: $test_log_dir"
    
    # 查找所有 .info 文件
    echo "查找所有 .info 文件..."
    local info_files=$(find "$test_log_dir" -name "*.info" -type f 2>/dev/null)
    
    if [ -z "$info_files" ]; then
        echo "警告: 未找到任何 .info 文件"
        return 1
    fi
    
    local info_count=$(echo "$info_files" | wc -l)
    echo "找到 $info_count 个覆盖率信息文件"
    
    # 不使用 exclude.txt 过滤，直接使用所有文件
    filtered_info_files="$info_files"
    local filtered_count=$info_count
    echo "不使用 exclude.txt 过滤，直接使用所有 $filtered_count 个文件"
    
    # 检查是否有文件需要合并
    local final_info_count=$(echo "$filtered_info_files" | wc -l)
    if [ "$final_info_count" -eq 0 ]; then
        echo "错误: 没有有效的覆盖率信息文件需要处理"
        return 1
    fi
    
    echo "准备合并 $final_info_count 个覆盖率信息文件..."
    
    # 统一使用小批次合并策略
    if [ "$final_info_count" -eq 1 ]; then
        # 只有一个文件，直接复制
        local single_file=$(echo "$filtered_info_files" | head -1)
        echo "只有一个覆盖率文件，直接使用: $(basename "$single_file")"
        cp "$single_file" "coverage_tdengine_raw.info"
    else
        # 使用统一的小批次合并策略
        echo "使用统一小批次合并策略..."
        
        # 确定并发数和批次大小
        local max_jobs=$(nproc 2>/dev/null || echo "4")
        
        # *** 可配置的批次大小参数 ***
        local MERGE_BATCH_SIZE=${MERGE_BATCH_SIZE:-5}  # 默认5个文件一批，可通过环境变量调整
        
        echo "并发配置: $max_jobs 个CPU核心，统一批次大小 $MERGE_BATCH_SIZE 个文件"
        merge_files_uniform_batch "$filtered_info_files" "$final_info_count" "$MERGE_BATCH_SIZE" "$max_jobs"
    fi
    
    # 检查生成的文件
    if [ -s "coverage_tdengine_raw.info" ]; then
        local final_size=$(stat -c%s "coverage_tdengine_raw.info")
        local final_lines=$(wc -l < "coverage_tdengine_raw.info")
        local source_files=$(grep "^SF:" "coverage_tdengine_raw.info" | wc -l || echo "0")
        
        echo "✓ 成功生成合并后的覆盖率信息文件:"
        echo "  文件大小: $final_size 字节"
        echo "  文件行数: $final_lines 行"
        echo "  包含源文件数: $source_files 个"
        
        return 0
    else
        echo "✗ 生成的覆盖率信息文件为空或不存在"
        return 1
    fi
}

# 统一小批次合并函数
function merge_files_uniform_batch() {
    local filtered_info_files="$1"
    local file_count="$2"
    local batch_size="$3"
    local max_jobs="$4"
    
    local overall_start=$(date +%s)
    
    # 创建临时目录
    local temp_dir=$(mktemp -d)
    local current_files_list="$temp_dir/current_files.list"
    echo "$filtered_info_files" > "$current_files_list"
    
    local current_count="$file_count"
    local round=1
    
    echo "开始统一小批次合并: $current_count 个文件，批次大小: $batch_size"
    
    # 循环合并：当文件数量大于1时继续合并
    while [ "$current_count" -gt 1 ]; do
        echo ""
        echo "=== 第 $round 轮统一批次合并 ==="
        echo "处理 $current_count 个文件，批次大小: $batch_size"
        
        # 分割文件列表
        split -l "$batch_size" -d "$current_files_list" "$temp_dir/round_${round}_batch_" --suffix-length=4
        
        # 统计批次数
        local batch_count=$(find "$temp_dir" -name "round_${round}_batch_*" -type f | wc -l)
        local expected_output_count=$(((current_count + batch_size - 1) / batch_size))
        echo "生成 $batch_count 个批次进行并发处理，预期输出: $expected_output_count 个文件"
        
        # 合并函数
        uniform_merge_batch() {
            local batch_file="$1"
            local batch_no="$2"
            local round="$3"
            local temp_dir="$4"
            
            local batch_output="$temp_dir/round_${round}_merged_$(printf "%04d" $batch_no).info"
            local batch_count=$(wc -l < "$batch_file")
            
            #echo "  [第${round}轮-批次${batch_no}] 合并 $batch_count 个文件..."
            
            if [ "$batch_count" -eq 1 ]; then
                # 单文件批次，直接复制
                local single_file=$(head -1 "$batch_file")
                if [ -f "$single_file" ]; then
                    cp "$single_file" "$batch_output"
                    local file_size=$(stat -c%s "$batch_output" 2>/dev/null || echo "0")
                    echo "  [第${round}轮-批次${batch_no}] ✓ 单文件复制 (大小: $file_size 字节)"
                else
                    echo "  [第${round}轮-批次${batch_no}] ✗ 单文件不存在"
                    return 1
                fi
            else
                # 多文件批次，使用lcov合并
                local merge_cmd="lcov --quiet --rc lcov_branch_coverage=0"
                local actual_files=0
                
                while IFS= read -r file_path; do
                    if [ -f "$file_path" ]; then
                        merge_cmd="$merge_cmd --add-tracefile '$file_path'"
                        ((actual_files++))
                    fi
                done < "$batch_file"
                merge_cmd="$merge_cmd -o '$batch_output'"
                
                local batch_start=$(date +%s)
                if eval "$merge_cmd" 2>/dev/null; then
                    local batch_end=$(date +%s)
                    local batch_duration=$((batch_end - batch_start))
                    
                    if [ -s "$batch_output" ]; then
                        local file_size=$(stat -c%s "$batch_output" 2>/dev/null || echo "0")
                        echo "  [第${round}轮-批次${batch_no}] ✓ 完成 ($actual_files个文件, 用时: ${batch_duration}秒, 大小: $file_size 字节)"
                    else
                        echo "  [第${round}轮-批次${batch_no}] ✗ 生成文件为空"
                        rm -f "$batch_output"
                        return 1
                    fi
                else
                    echo "  [第${round}轮-批次${batch_no}] ✗ 合并失败"
                    return 1
                fi
            fi
            
            # 记录成功的批次文件
            (
                flock -x 200
                echo "$batch_output" >> "$temp_dir/round_${round}_success.list"
            ) 200>"$temp_dir/round_${round}_success.lock"
        }
        
        # 导出函数
        export -f uniform_merge_batch
        
        # 清空成功列表
        rm -f "$temp_dir/round_${round}_success.list"
        
        # 并发执行当前轮合并
        local batch_no=1
        for batch_file in "$temp_dir"/round_${round}_batch_*; do
            if [ -f "$batch_file" ]; then
                if command -v parallel >/dev/null 2>&1; then
                    echo "$batch_file $batch_no $round $temp_dir"
                else
                    # 后台进程控制
                    uniform_merge_batch "$batch_file" "$batch_no" "$round" "$temp_dir" &
                    
                    # 控制并发数
                    local running_jobs=$(jobs -r | wc -l)
                    while [ "$running_jobs" -ge "$max_jobs" ]; do
                        sleep 0.1
                        running_jobs=$(jobs -r | wc -l)
                    done
                fi
                ((batch_no++))
            fi
        done
        
        # 使用parallel或等待后台任务
        if command -v parallel >/dev/null 2>&1; then
            echo "使用 GNU parallel 进行第${round}轮并发处理..."
            find "$temp_dir" -name "round_${round}_batch_*" -type f | \
            parallel -j "$max_jobs" uniform_merge_batch {} {#} "$round" "$temp_dir"
        else
            echo "使用后台进程进行第${round}轮并发处理，等待完成..."
            wait
        fi
        
        # 清理锁文件
        rm -f "$temp_dir/round_${round}_success.lock"
        
        # 检查当前轮结果
        if [ ! -f "$temp_dir/round_${round}_success.list" ]; then
            echo "✗ 第 $round 轮合并失败，没有成功文件"
            rm -rf "$temp_dir"
            return 1
        fi
        
        local success_count=$(wc -l < "$temp_dir/round_${round}_success.list")
        if [ "$success_count" -eq 0 ]; then
            echo "✗ 第 $round 轮合并失败，成功文件数为0"
            rm -rf "$temp_dir"
            return 1
        fi
        
        local round_end=$(date +%s)
        local round_duration=$((round_end - overall_start))
        echo "第 $round 轮完成: $current_count -> $success_count 个文件 (累计用时: ${round_duration}秒)"
        
        # 清理当前轮临时文件，但保留成功的输出文件
        rm -f "$temp_dir"/round_${round}_batch_*
        
        # 准备下一轮
        mv "$temp_dir/round_${round}_success.list" "$temp_dir/round_$((round + 1))_input.list"
        current_files_list="$temp_dir/round_$((round + 1))_input.list"
        current_count="$success_count"
        ((round++))
        
        # 安全检查：防止无限循环
        if [ "$round" -gt 20 ]; then
            echo "警告: 合并轮数超过20轮，检查是否需要强制处理剩余文件"
            break
        fi
        
        # *** 移除收敛检查，让循环自然进行到底 ***
        # 注释掉原来的收敛检查代码，避免提前退出进入串行合并
        # if [ "$success_count" -ge "$((current_count * 90 / 100))" ] && [ "$current_count" -gt 2 ]; then
        #     echo "检测到收敛缓慢，强制最终合并剩余 $success_count 个文件..."
        #     break
        # fi
    done
    
    # *** 修复：最终结果处理也保持并发 ***
    if [ "$current_count" -eq 1 ]; then
        local final_file=$(head -1 "$current_files_list")
        echo ""
        echo "✓ 统一小批次合并完成，最终文件: $(basename "$final_file")"
        mv "$final_file" "coverage_tdengine_raw.info"
    elif [ "$current_count" -gt 1 ]; then
        # *** 关键修复：剩余文件也使用并发合并，而不是串行 ***
        echo ""
        echo "剩余 $current_count 个文件，继续使用并发合并而不是串行..."
        
        # 检查是否因为轮数限制而退出
        if [ "$round" -gt 20 ]; then
            echo "由于轮数限制退出循环，强制并发处理剩余 $current_count 个文件"
            
            # 即使超过轮数限制，也使用小批次并发处理剩余文件
            echo "=== 强制并发处理剩余文件 ==="
            
            # 分割剩余文件
            split -l "$batch_size" -d "$current_files_list" "$temp_dir/final_batch_" --suffix-length=4
            
            local final_batch_count=$(find "$temp_dir" -name "final_batch_*" -type f | wc -l)
            echo "剩余文件分为 $final_batch_count 个批次进行最终并发处理"
            
            # 清空最终成功列表
            rm -f "$temp_dir/final_success.list"
            
            # 最终并发合并函数
            final_merge_batch() {
                local batch_file="$1"
                local batch_no="$2"
                local temp_dir="$3"
                
                local batch_output="$temp_dir/final_merged_$(printf "%04d" $batch_no).info"
                local batch_count=$(wc -l < "$batch_file")
                
                echo "  [最终-批次${batch_no}] 合并 $batch_count 个文件..."
                
                if [ "$batch_count" -eq 1 ]; then
                    local single_file=$(head -1 "$batch_file")
                    if [ -f "$single_file" ]; then
                        cp "$single_file" "$batch_output"
                        echo "  [最终-批次${batch_no}] ✓ 单文件复制"
                    fi
                else
                    local merge_cmd="lcov --quiet --rc lcov_branch_coverage=0"
                    while IFS= read -r file_path; do
                        if [ -f "$file_path" ]; then
                            merge_cmd="$merge_cmd --add-tracefile '$file_path'"
                        fi
                    done < "$batch_file"
                    merge_cmd="$merge_cmd -o '$batch_output'"
                    
                    if eval "$merge_cmd" 2>/dev/null && [ -s "$batch_output" ]; then
                        echo "  [最终-批次${batch_no}] ✓ 合并完成"
                    else
                        echo "  [最终-批次${batch_no}] ✗ 合并失败"
                        return 1
                    fi
                fi
                
                # 记录成功文件
                (
                    flock -x 200
                    echo "$batch_output" >> "$temp_dir/final_success.list"
                ) 200>"$temp_dir/final_success.lock"
            }
            
            export -f final_merge_batch
            
            # 执行最终并发合并
            local batch_no=1
            for batch_file in "$temp_dir"/final_batch_*; do
                if [ -f "$batch_file" ]; then
                    if command -v parallel >/dev/null 2>&1; then
                        echo "$batch_file $batch_no $temp_dir"
                    else
                        final_merge_batch "$batch_file" "$batch_no" "$temp_dir" &
                        
                        local running_jobs=$(jobs -r | wc -l)
                        while [ "$running_jobs" -ge "$max_jobs" ]; do
                            sleep 0.1
                            running_jobs=$(jobs -r | wc -l)
                        done
                    fi
                    ((batch_no++))
                fi
            done
            
            if command -v parallel >/dev/null 2>&1; then
                echo "使用 GNU parallel 进行最终并发处理..."
                find "$temp_dir" -name "final_batch_*" -type f | \
                parallel -j "$max_jobs" final_merge_batch {} {#} "$temp_dir"
            else
                echo "等待最终并发处理完成..."
                wait
            fi
            
            rm -f "$temp_dir/final_success.lock"
            
            # 检查最终结果
            if [ -f "$temp_dir/final_success.list" ]; then
                local final_count=$(wc -l < "$temp_dir/final_success.list")
                echo "最终并发处理完成: $current_count -> $final_count 个文件"
                
                if [ "$final_count" -eq 1 ]; then
                    local final_file=$(head -1 "$temp_dir/final_success.list")
                    mv "$final_file" "coverage_tdengine_raw.info"
                    echo "✓ 最终文件已生成"
                elif [ "$final_count" -gt 1 ]; then
                    # 如果最终并发处理后还有多个文件，递归调用自己继续处理
                    echo "最终并发处理后还有 $final_count 个文件，递归继续处理..."
                    local final_files=$(cat "$temp_dir/final_success.list")
                    
                    # 清理临时目录
                    rm -rf "$temp_dir"
                    
                    # 递归调用继续处理
                    merge_files_uniform_batch "$final_files" "$final_count" "$batch_size" "$max_jobs"
                    return $?
                fi
            else
                echo "✗ 最终并发处理失败"
                rm -rf "$temp_dir"
                return 1
            fi
        else
            echo "✗ 异常退出：剩余 $current_count 个文件但未达到轮数限制"
            rm -rf "$temp_dir"
            return 1
        fi
    else
        echo "✗ 统一小批次合并异常，剩余 $current_count 个文件"
        rm -rf "$temp_dir"
        return 1
    fi
    
    local overall_end=$(date +%s)
    local total_duration=$((overall_end - overall_start))
    
    echo "✓ 统一小批次合并完成:"
    echo "  总轮数: $((round - 1))"
    echo "  批次大小: $batch_size 个文件/批次"
    echo "  总用时: ${total_duration}秒"
    echo "  输入文件: $file_count 个"
    if [ "$file_count" -gt 0 ]; then
        echo "  处理效率: $(((file_count * 1000) / (total_duration > 0 ? total_duration : 1))) 文件/秒"
    fi
    
    # 清理临时目录
    rm -rf "$temp_dir"
}

function lcovFunc {
    echo "collect data by lcov func"
    cd $TDENGINE_DIR || exit

    # 收集并合并所有测试case的覆盖率信息文件
    if [ -n "$TEST_LOG_DIR" ]; then
        if ! collect_info_from_tests "$TEST_LOG_DIR"; then
            echo "错误: 收集覆盖率信息文件失败"
            exit 1
        fi
    else
        echo "警告: 未指定测试日志目录，无法收集覆盖率信息"
        exit 1
    fi

    # 检查生成的原始覆盖率文件
    if [ ! -s "coverage_tdengine_raw.info" ]; then
        echo "错误: coverage_tdengine_raw.info 文件不存在或为空"
        exit 1
    fi

    echo "=== 原始覆盖率信息统计 ==="
    local raw_size=$(stat -c%s "coverage_tdengine_raw.info")
    local raw_lines=$(wc -l < "coverage_tdengine_raw.info")
    local raw_sources=$(grep "^SF:" "coverage_tdengine_raw.info" | wc -l || echo "0")
    echo "原始文件大小: $raw_size 字节"
    echo "原始文件行数: $raw_lines 行"
    echo "原始源文件数: $raw_sources 个"

    # 使用 exclude.txt 文件来排除指定的文件
    if [ -f "$TDENGINE_DIR/test/ci/cov/exclude.txt" ]; then
        # echo "使用 exclude.txt 进行排除过滤..."
        
        # 先打印所有源文件
        # echo "=== 覆盖率文件中的所有源文件列表 ==="
        # grep "^SF:" coverage_tdengine_raw.info | sed 's/^SF://' | head -2000
        local total_sources=$(grep "^SF:" coverage_tdengine_raw.info | wc -l)
        # echo "总源文件数: $total_sources"
        
        local exclude_patterns=""
        local exclude_count=0
        # echo "扫描 exclude.txt 文件..."
        
        while IFS= read -r file_pattern; do
            # 跳过空行和注释行
            [[ -z "$file_pattern" || "$file_pattern" =~ ^[[:space:]]*# ]] && continue
            
            # 如果是相对路径，转换为绝对路径
            if [[ "$file_pattern" != /* ]]; then
                file_pattern="$TDENGINE_DIR/$file_pattern"
            fi
            
            # 打印当前处理的模式
            # echo "处理排除模式 $exclude_count: $file_pattern"
            exclude_patterns="$exclude_patterns '$file_pattern'"
            ((exclude_count++))
        done < "$TDENGINE_DIR/test/ci/cov/exclude.txt"
        
        # echo "加载了 $exclude_count 个排除模式"
        
        if [ -n "$exclude_patterns" ]; then
            # 使用 lcov --remove 排除指定的文件
            # echo "执行排除过滤..."
            # echo "排除命令: lcov --quiet --remove coverage_tdengine_raw.info $exclude_patterns --rc lcov_branch_coverage=0 -o coverage_tdengine.info"
            
            eval "lcov --quiet --remove coverage_tdengine_raw.info $exclude_patterns \
                --rc lcov_branch_coverage=0 \
                -o coverage_tdengine.info"
            
            if [ -s "coverage_tdengine.info" ]; then
                # echo "✓ 成功应用 exclude.txt 排除过滤"
                # 打印过滤后的文件统计
                local final_sources=$(grep "^SF:" "coverage_tdengine.info" | wc -l || echo "0")
                local excluded_count=$((raw_sources - final_sources))
                # echo "排除文件数: $excluded_count 个"
                # echo "剩余文件数: $final_sources 个"
                
                # 验证特定文件是否被过滤
                # echo "=== 过滤验证 ==="
                # for test_file in "get_db_name_test.c" "replay_test.c" "sml_test.c"; do
                #     if grep -q "SF:.*$test_file" coverage_tdengine.info; then
                #         echo "✗ 未过滤: $test_file"
                #     else
                #         echo "✓ 已过滤: $test_file"
                #     fi
                # done
            else
                echo "✗ 排除后文件为空，使用原始数据"
                cp coverage_tdengine_raw.info coverage_tdengine.info
            fi
        else
            echo "Warning: exclude.txt 中没有有效的文件模式，使用原始数据"
            cp coverage_tdengine_raw.info coverage_tdengine.info
        fi
    else
        echo "Warning: exclude.txt 文件不存在，使用原始数据"
        cp coverage_tdengine_raw.info coverage_tdengine.info
    fi

    # 生成最终结果统计
    echo "=== 最终覆盖率信息统计 ==="
    local final_size=$(stat -c%s "coverage_tdengine.info")
    local final_lines=$(wc -l < "coverage_tdengine.info")
    local final_sources=$(grep "^SF:" "coverage_tdengine.info" | wc -l || echo "0")
    echo "最终文件大小: $final_size 字节"
    echo "最终文件行数: $final_lines 行"
    echo "最终源文件数: $final_sources 个"

    # generate result
    echo "generate result"
    lcov --quiet -l --rc lcov_branch_coverage=0 coverage_tdengine.info 
    
    # 修正路径以确保与 TDengine 仓库根目录匹配    
    sed -i "s|SF:/home/TDinternal/community/|SF:|g" $TDENGINE_DIR/coverage_tdengine.info

    # 文件检查
    echo "=== 文件检查 ==="
    echo "当前目录: $(pwd)"
    echo "目标文件: $TDENGINE_DIR/coverage_tdengine.info"
    
    if [ -s "$TDENGINE_DIR/coverage_tdengine.info" ]; then
        local check_size=$(stat -c%s "$TDENGINE_DIR/coverage_tdengine.info")
        echo "✓ 最终文件: $TDENGINE_DIR/coverage_tdengine.info (大小: $check_size 字节)"
    else
        echo "✗ 最终文件不存在或为空: $TDENGINE_DIR/coverage_tdengine.info"
        exit 1
    fi

    # exit 0

    # 调试输出覆盖率文件内容样例
    echo "覆盖率文件包含的源文件 (前10个):"
    grep "^SF:" "$TDENGINE_DIR/coverage_tdengine.info" | head -10 | sed 's/^SF:/  /' || echo "  (无源文件信息)"

    # 上传到 Codecov
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

    echo "push result to coveralls.io"
    # push result to https://coveralls.io/
    # /usr/local/bin/coveralls-lcov -t WOjivt0JCvDfqHDpyBQXtqhYbOGANrrps -b $BRANCH $TDENGINE_DIR/coverage_tdengine.info

    # # 执行上传并捕获结果
    # local coveralls_output
    # coveralls_output=$(/usr/local/bin/coveralls-lcov -t WOjivt0JCvDfqHDpyBQXtqhYbOGANrrps -b $BRANCH $TDENGINE_DIR/coverage_tdengine.info 2>&1)
    
    # echo "$coveralls_output"

    # Coveralls 上传重试机制
    local max_upload_attempts=10
    local upload_attempt=1
    local upload_success=false
    local coveralls_output=""

    while [ $upload_attempt -le $max_upload_attempts ]; do
        echo ""
        echo "=== 第 $upload_attempt/$max_upload_attempts 次尝试上传到 Coveralls ==="
        
        # 执行上传并捕获结果
        coveralls_output=$(/usr/local/bin/coveralls-lcov -t WOjivt0JCvDfqHDpyBQXtqhYbOGANrrps -b $BRANCH $TDENGINE_DIR/coverage_tdengine.info 2>&1)
        local upload_exit_code=$?
        
        echo "上传输出:"
        echo "$coveralls_output"
        echo "上传退出码: $upload_exit_code"
        
        # 判断是否成功
        if [ $upload_exit_code -eq 0 ] && echo "$coveralls_output" | grep -q '"url"'; then
            echo "✓ 成功上传到 Coveralls"
            upload_success=true
            break
        else
            echo "✗ 上传失败"
            
            # 分析失败原因
            if echo "$coveralls_output" | grep -qi "Connection reset by peer\|ECONNRESET"; then
                echo "原因: 网络连接被重置"
            elif echo "$coveralls_output" | grep -qi "timeout\|timed out"; then
                echo "原因: 连接超时"
            elif echo "$coveralls_output" | grep -qi "No such file or directory"; then
                echo "原因: 文件不存在或路径错误"
            else
                echo "原因: 未知错误"
            fi
            
            if [ $upload_attempt -lt $max_upload_attempts ]; then
                # 计算等待时间：2分钟 = 120秒
                local wait_time=120
                echo "等待 ${wait_time} 秒后重试..."
                sleep $wait_time
            else
                echo "已达到最大重试次数 ($max_upload_attempts 次)"
            fi
        fi
        
        ((upload_attempt++))
    done

    echo ""
    echo "=== Coveralls 上传结果分析 ==="
    if [ "$upload_success" = true ]; then
        echo "✓ 覆盖率数据已成功上传到 Coveralls (尝试次数: $((upload_attempt - 1)))"
        
        
        # 提取 URL 并调用 Python 脚本
        local job_url=$(echo "$coveralls_output" | grep -o '"url":"[^"]*"' | sed 's/"url":"//;s/"//')
        
        if [ -n "$job_url" ]; then
            echo ""
            echo "=== 获取 Coveralls 详细信息 ==="
            echo "调用 Python 脚本获取覆盖率详情..."
            echo "Coveralls URL: $job_url"

            # 检查 Python 脚本是否存在
            local script_path="$TDENGINE_DIR/test/ci/tdengine_coveage_alarm.py"
            if [ ! -f "$script_path" ]; then
                echo "警告: Python 脚本不存在: $script_path"
                echo "📊 完整报告请访问: $job_url"
                return
            fi

            # 安装必要的 Python 依赖包
            echo "安装 Python 依赖包..."
            pip3 install bs4 requests lxml beautifulsoup4 -q
            local pip_exit_code=$?
            if [ $pip_exit_code -ne 0 ]; then
                echo "警告: 安装 Python 依赖包失败，退出码: $pip_exit_code"
                echo "尝试继续执行脚本..."
            else
                echo "✓ Python 依赖包安装完成"
            fi

            # 重试机制获取覆盖率详情
            local max_attempts=5  # 增加到5次
            local attempt=1
            local success=false

            while [ $attempt -le $max_attempts ]; do
                echo ""
                echo "第 $attempt/$max_attempts 次尝试获取覆盖率详情..."
                
                # 计算等待时间：递增等待 - 60s, 120s, 180s, 240s, 300s
                local wait_time=$((attempt * 60))
                echo "等待 Coveralls 处理数据（${wait_time}秒）..."
                sleep $wait_time
                
                echo "开始调用 Python 脚本..."
                echo "命令: python3 $script_path -url $job_url"
                
                # 调用 Python 脚本并捕获输出
                local script_output
                script_output=$(python3 "$script_path" -url "$job_url" 2>&1)
                local script_exit_code=$?
                
                echo "Python 脚本输出:"
                echo "$script_output"
                echo "Python 脚本执行完成，退出码: $script_exit_code"
                
                # 判断是否成功
                if [ $script_exit_code -eq 0 ]; then
                    # 提取 JSON 中的 status 字段
                    local status=$(echo "$script_output" | grep '"status"' | head -1 | grep -o '"status"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"status"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')
                    local coverage_change=$(echo "$script_output" | grep '"coverage_change"' | head -1 | grep -o '"coverage_change"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"coverage_change"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')
                    
                    echo ""
                    echo "=== 结果分析 ==="
                    echo "状态: $status"
                    echo "覆盖率变化: $coverage_change"
                    
                    # 只有 status 为 "success" 才认为成功
                    if [ "$status" = "success" ]; then
                        echo "✓ 成功获取完整的覆盖率详情（status=success）"
                        success=true
                        break
                    elif [ "$status" = "error" ]; then
                        echo "⚠ Coveralls 数据尚未完全处理（status=error）"
                        
                        # 检查是否有覆盖率变化信息
                        if [ -n "$coverage_change" ] && [ "$coverage_change" != "null" ]; then
                            echo "  已获取覆盖率变化: $coverage_change"
                            echo "  但详细信息尚未就绪，需要继续等待..."
                        else
                            echo "  详细信息尚未就绪..."
                        fi
                        
                        if [ $attempt -lt $max_attempts ]; then
                            echo "  将在 $((attempt * 30)) 秒后重试..."
                        else
                            echo "  已达到最大重试次数，但已获取基本覆盖率信息"
                        fi
                    else
                        echo "⚠ 未知状态: '$status'"
                        if [ $attempt -lt $max_attempts ]; then
                            echo "  将重试..."
                        fi
                    fi
                else
                    echo "✗ Python 脚本执行失败，退出码: $script_exit_code"
                    if [ $attempt -lt $max_attempts ]; then
                        echo "  将重试..."
                    fi
                fi
                
                ((attempt++))
            done
            
            # 最终结果总结
            echo ""
            echo "=== 最终结果 ==="
            if [ "$success" = true ]; then
                echo "✓ 成功获取 Coveralls 覆盖率详情"
                echo "  重试次数: $((attempt - 1))"
                echo "  总等待时间: $(( (attempt - 1) * attempt * 15 )) 秒"
            else
                echo "⚠ 未能获取完整的覆盖率详情"
                echo "可能的原因："
                echo "  1. Coveralls 数据处理需要更长时间"
                echo "  2. 上传需要更长的处理时间"
                echo ""
                echo "建议："
                echo "  • 稍后手动检查 Coveralls 页面"
            fi
            
            echo ""
            echo "📊 完整报告请访问: $job_url"
        fi
    
    else
        echo "✗ 覆盖率数据上传到 Coveralls 失败 (尝试次数: $upload_attempt)"
        echo "最后一次错误输出:"
        echo "$coveralls_output"
        echo ""
        echo "可能的解决方案："
        echo "  1. 检查网络连接是否稳定"
        echo "  2. 稍后手动上传或重新运行测试"
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

echo "For more details: https://app.codecov.io/github/taosdata/TDengine"\n