#!/bin/bash

# TDengine 增量位图插件 - 修复版测试脚本
# 作者：章子渝
# 版本：1.1

echo "🚀 TDengine 增量位图插件测试开始"
echo "=================================="

# 检查当前目录
if [ ! -f "CMakeLists.txt" ]; then
    echo "❌ 错误：请在插件根目录运行此脚本"
    echo "   正确路径：/home/hp/TDengine/plugins/incremental_bitmap"
    exit 1
fi

# 1. 环境检查
echo "1️⃣ 检查环境..."
if ! command -v cmake &> /dev/null; then
    echo "❌ 错误：未找到cmake，请安装：sudo apt-get install cmake"
    exit 1
fi

if ! command -v make &> /dev/null; then
    echo "❌ 错误：未找到make，请安装：sudo apt-get install build-essential"
    exit 1
fi

echo "   ✅ 环境检查通过"

# 2. 构建项目
echo ""
echo "2️⃣ 构建项目..."
if [ -d "build" ]; then
    echo "   清理旧构建..."
    rm -rf build
fi

mkdir -p build
cd build

echo "   配置CMake..."
cmake -DUSE_MOCK=ON -DBUILD_TESTING=ON .. > cmake.log 2>&1
if [ $? -ne 0 ]; then
    echo "   ❌ CMake配置失败！"
    echo "   错误日志："
    cat cmake.log
    exit 1
fi

echo "   编译代码..."
make -j$(nproc) > make.log 2>&1
if [ $? -ne 0 ]; then
    echo "   ❌ 编译失败！"
    echo "   错误日志："
    cat make.log
    exit 1
fi

echo "   ✅ 构建成功"

# 3. 运行核心测试
echo ""
echo "3️⃣ 运行核心测试..."
echo "   📋 运行单元测试..."

test_results=0
passed=0
failed=0
mem_issues=0

# 实际存在的测试列表
tests=(
    "test_bitmap_engine_core:位图引擎核心测试"
    "test_state_transitions:状态转换测试"
    "test_abstraction_layer:抽象层测试"
    "test_backup_coordinator:备份协调器测试"
    "test_event_interceptor:事件拦截器测试"
    "test_ring_buffer:环形缓冲区测试"
    "test_skiplist:跳表测试"
    "test_roaring_bitmap_specific:RoaringBitmap特定测试"
    "test_fault_injection:故障注入测试"
    "test_offset_semantics:偏移量语义测试"
    "test_observability_interface:可观测性接口测试"
    "test_observability_enhanced:可观测性增强测试"
    "test_observability_comprehensive:可观测性综合测试"
)

for test_info in "${tests[@]}"; do
    test_name=$(echo "$test_info" | cut -d: -f1)
    test_desc=$(echo "$test_info" | cut -d: -f2)
    
    echo "      - $test_desc..."
    if timeout 30s ./$test_name > /dev/null 2>&1; then
        echo "        ✅ 通过"
        ((passed++))
    else
        echo "        ❌ 失败"
        ((failed++))
        test_results=1
    fi
done

# 4. 运行集成测试
echo ""
echo "4️⃣ 运行集成测试..."
echo "   📋 运行PITR端到端测试（简化版）..."
if timeout 60s ./test_pitr_e2e_simple > /dev/null 2>&1; then
    echo "      ✅ 通过"
    ((passed++))
else
    echo "      ❌ 失败"
    ((failed++))
    test_results=1
fi

echo "   📋 运行一致性测试（最小化）..."
if timeout 30s ./test_consistency_minimal > /dev/null 2>&1; then
    echo "      ✅ 通过"
    ((passed++))
else
    echo "      ❌ 失败"
    ((failed++))
    test_results=1
fi

# 5. 运行完整功能测试（可选）
echo ""
echo "5️⃣ 运行完整功能测试（可选）..."
echo "   ⚠️  注意：完整测试需要约10-15分钟"
read -p "   是否运行完整测试？(y/N): " run_full_test

if [[ $run_full_test =~ ^[Yy]$ ]]; then
    echo "   📋 运行完整PITR端到端测试..."
    if timeout 300s ./test_pitr_e2e > /dev/null 2>&1; then
        echo "      ✅ 通过"
        ((passed++))
    else
        echo "      ❌ 失败"
        ((failed++))
        test_results=1
    fi
fi

# 6. 内存检查（可选）
echo ""
echo "6️⃣ 内存检查（可选）..."
read -p "   是否运行内存检查？(y/N): " run_memory_check

if [[ $run_memory_check =~ ^[Yy]$ ]]; then
    echo "   📋 运行Valgrind内存检查..."
    declare -a vg_targets=(
        "test_bitmap_engine_core"
        "test_backup_coordinator"
        "test_skiplist"
        "test_ring_buffer"
    )
    mkdir -p valgrind_logs
    vg_failed=()
    for t in "${vg_targets[@]}"; do
        log_file="valgrind_logs/${t}.log"
        echo "      ▶ $t..."
        if timeout 90s valgrind --leak-check=full --error-exitcode=1 ./$t > "$log_file" 2>&1; then
            echo "        ✅ 通过 ($t)"
        else
            echo "        ❌ 发现内存问题 ($t)"
            echo "        ⤷ 日志：$log_file"
            mem_issues=1
            vg_failed+=("$t")
        fi
    done
fi

# 7. 测试结果总结
echo ""
echo "=================================="
if [ $test_results -eq 0 ]; then
    echo "✅ 所有测试通过！"
    echo "   通过: $passed"
    echo "   失败: $failed"
    echo "   总计: $((passed + failed))"
    if [ $mem_issues -eq 1 ]; then
        echo "   ⚠️  注意：可选内存检查发现问题，详见 valgrind_logs/*.log"
        if [ ${#vg_failed[@]} -gt 0 ]; then
            echo "   受影响的目标：${vg_failed[*]}"
        fi
    fi
else
    echo "❌ 部分测试失败，请检查上述错误信息"
    echo "   通过: $passed"
    echo "   失败: $failed"
    echo "   总计: $((passed + failed))"
    if [ $mem_issues -eq 1 ]; then
        echo "   ⚠️  同时存在内存检查问题，详见 valgrind_logs/*.log"
        if [ ${#vg_failed[@]} -gt 0 ]; then
            echo "   受影响的目标：${vg_failed[*]}"
        fi
    fi
fi

echo ""
echo "🔧 故障排除："
echo "   - 检查编译日志：build/cmake.log, build/make.log"
echo "   - 检查测试日志：build/pitr_simple.log, build/pitr_full.log"
echo "   - 检查内存日志：build/valgrind.log"

echo "=================================="

exit $test_results
