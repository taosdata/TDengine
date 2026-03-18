#!/bin/bash
# 测试滚动升级任务脚本

# 设置测试用的绿色版本路径
export TD_GREEN_VERSIONS_PATH="/root/TDinternal/community/test/tools/CompatCheck/test"

# 进入脚本所在目录
cd "$(dirname "$0")"

echo "=========================================="
echo "测试滚动升级任务"
echo "=========================================="
echo ""

# 运行滚动升级任务（使用测试目录）
python3 hot_upgrade_task.py \
    --green-versions-path "$TD_GREEN_VERSIONS_PATH" \
    --build-dir "/root/TDinternal/community/debug" \
    "$@"

exit_code=$?

echo ""
echo "=========================================="
echo "测试完成，退出码: $exit_code"
echo "=========================================="

exit $exit_code
