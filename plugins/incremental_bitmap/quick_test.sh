#!/bin/bash

# 快速测试脚本 - 只运行核心测试
echo "🚀 快速测试开始"
echo "=================="

cd /home/hp/TDengine/plugins/incremental_bitmap/build

echo "1. 测试位图引擎核心..."
if timeout 30s ./test_bitmap_engine_core > /dev/null 2>&1; then
    echo "   ✅ 通过"
else
    echo "   ❌ 失败"
fi

echo "2. 测试状态转换..."
if timeout 30s ./test_state_transitions > /dev/null 2>&1; then
    echo "   ✅ 通过"
else
    echo "   ❌ 失败"
fi

echo "3. 测试抽象层..."
if timeout 30s ./test_abstraction_layer > /dev/null 2>&1; then
    echo "   ✅ 通过"
else
    echo "   ❌ 失败"
fi

echo "4. 测试备份协调器..."
if timeout 30s ./test_backup_coordinator > /dev/null 2>&1; then
    echo "   ✅ 通过"
else
    echo "   ❌ 失败"
fi

echo "=================="
echo "✅ 快速测试完成"
