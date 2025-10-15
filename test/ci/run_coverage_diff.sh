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
    
    echo "=== 收集并合并所有测试case的覆盖率信息文件 ==="
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
    
    # 使用 coverage.txt 过滤需要处理的文件
    local filtered_info_files=""
    if [ -f "$TDENGINE_DIR/test/ci/coverage.txt" ]; then
        echo "使用 coverage.txt 过滤覆盖率信息文件..."
        
        # 创建临时过滤模式文件
        local coverage_patterns_file=$(mktemp)
        while IFS= read -r line; do
            # 跳过空行和注释行
            [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
            
            # 提取文件名（去掉路径）
            local filename=$(basename "$line")
            
            # 去掉扩展名，得到基础名称
            if [[ "$filename" == *.* ]]; then
                local base_name="${filename%%.*}"
                echo "$base_name" >> "$coverage_patterns_file"
            else
                echo "$filename" >> "$coverage_patterns_file"
            fi
        done < "$TDENGINE_DIR/test/ci/coverage.txt"
        
        # 去重并排序
        sort -u "$coverage_patterns_file" -o "$coverage_patterns_file"
        local pattern_count=$(wc -l < "$coverage_patterns_file")
        echo "加载了 $pattern_count 个过滤模式"
        
        # 过滤 .info 文件：只保留包含coverage.txt中文件的info文件
        local temp_filtered_list=$(mktemp)
        echo "$info_files" | while read -r info_file; do
            if [ -f "$info_file" ]; then
                # 检查 info 文件中是否包含我们关心的源文件
                local has_covered_files=false
                while IFS= read -r pattern; do
                    if grep -q "SF:.*${pattern}" "$info_file" 2>/dev/null; then
                        has_covered_files=true
                        break
                    fi
                done < "$coverage_patterns_file"
                
                if [ "$has_covered_files" = true ]; then
                    echo "$info_file" >> "$temp_filtered_list"
                fi
            fi
        done
        
        if [ -s "$temp_filtered_list" ]; then
            filtered_info_files=$(cat "$temp_filtered_list")
            local filtered_count=$(wc -l < "$temp_filtered_list")
            echo "过滤后剩余 $filtered_count 个有效的覆盖率信息文件"
        else
            echo "警告: 过滤后没有有效的覆盖率信息文件"
            rm -f "$coverage_patterns_file" "$temp_filtered_list"
            return 1
        fi
        
        rm -f "$coverage_patterns_file" "$temp_filtered_list"
    else
        echo "Warning: coverage.txt 不存在，使用所有找到的 .info 文件"
        filtered_info_files="$info_files"
    fi
    
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
        local merge_cmd="lcov --quiet --rc lcov_branch_coverage=1"
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
    
    echo "=== 收集并合并所有测试case的覆盖率信息文件 ==="
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
    
    # 使用 coverage.txt 过滤需要处理的文件
    local filtered_info_files=""
    if [ -f "$TDENGINE_DIR/test/ci/coverage.txt" ]; then
        echo "使用 coverage.txt 过滤覆盖率信息文件..."
        
        # 创建临时过滤模式文件
        local coverage_patterns_file=$(mktemp)
        while IFS= read -r line; do
            # 跳过空行和注释行
            [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
            
            # 提取文件名（去掉路径）
            local filename=$(basename "$line")
            
            # 去掉扩展名，得到基础名称
            if [[ "$filename" == *.* ]]; then
                local base_name="${filename%%.*}"
                echo "$base_name" >> "$coverage_patterns_file"
            else
                echo "$filename" >> "$coverage_patterns_file"
            fi
        done < "$TDENGINE_DIR/test/ci/coverage.txt"
        
        # 去重并排序
        sort -u "$coverage_patterns_file" -o "$coverage_patterns_file"
        local pattern_count=$(wc -l < "$coverage_patterns_file")
        echo "加载了 $pattern_count 个过滤模式"
        
        # 过滤 .info 文件：只保留包含coverage.txt中文件的info文件
        local temp_filtered_list=$(mktemp)
        echo "$info_files" | while read -r info_file; do
            if [ -f "$info_file" ]; then
                # 检查 info 文件中是否包含我们关心的源文件
                local has_covered_files=false
                while IFS= read -r pattern; do
                    if grep -q "SF:.*${pattern}" "$info_file" 2>/dev/null; then
                        has_covered_files=true
                        break
                    fi
                done < "$coverage_patterns_file"
                
                if [ "$has_covered_files" = true ]; then
                    echo "$info_file" >> "$temp_filtered_list"
                fi
            fi
        done
        
        if [ -s "$temp_filtered_list" ]; then
            filtered_info_files=$(cat "$temp_filtered_list")
            local filtered_count=$(wc -l < "$temp_filtered_list")
            echo "过滤后剩余 $filtered_count 个有效的覆盖率信息文件"
        else
            echo "警告: 过滤后没有有效的覆盖率信息文件"
            rm -f "$coverage_patterns_file" "$temp_filtered_list"
            return 1
        fi
        
        rm -f "$coverage_patterns_file" "$temp_filtered_list"
    else
        echo "Warning: coverage.txt 不存在，使用所有找到的 .info 文件"
        filtered_info_files="$info_files"
    fi
    
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
        local MERGE_BATCH_SIZE=${MERGE_BATCH_SIZE:-4}  # 默认2个文件一批，可通过环境变量调整
        
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
                local merge_cmd="lcov --quiet --rc lcov_branch_coverage=1"
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
                    local merge_cmd="lcov --quiet --rc lcov_branch_coverage=1"
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
    echo "collect data by lcov"
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

    # 使用 coverage.txt 文件来进一步过滤覆盖率数据
    if [ -f "$TDENGINE_DIR/test/ci/coverage.txt" ]; then
        echo "使用 coverage.txt 进行最终过滤..."
        
        local include_patterns=""
        while IFS= read -r file_pattern; do
            # 跳过空行和注释行
            [[ -z "$file_pattern" || "$file_pattern" =~ ^[[:space:]]*# ]] && continue
            
            # 添加到 include 列表
            include_patterns="$include_patterns '*/$file_pattern'"
        done < "$TDENGINE_DIR/test/ci/coverage.txt"
        
        if [ -n "$include_patterns" ]; then
            # 使用 lcov --extract 提取指定的文件
            eval "lcov --quiet --extract coverage_tdengine_raw.info $include_patterns \
                --rc lcov_branch_coverage=1 \
                -o coverage_tdengine.info"
            
            if [ -s "coverage_tdengine.info" ]; then
                echo "✓ 成功应用 coverage.txt 过滤"
            else
                echo "✗ 过滤后文件为空，使用原始数据"
                cp coverage_tdengine_raw.info coverage_tdengine.info
            fi
        else
            echo "Warning: coverage.txt 中没有有效的文件模式，使用原始数据"
            cp coverage_tdengine_raw.info coverage_tdengine.info
        fi
    else
        echo "Warning: coverage.txt 文件不存在，使用原始数据"
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
    lcov --quiet -l --rc lcov_branch_coverage=1 coverage_tdengine.info 
    
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
    /usr/local/bin/coveralls-lcov -t WOjivt0JCvDfqHDpyBQXtqhYbOGANrrps -b $BRANCH $TDENGINE_DIR/coverage_tdengine.info 
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