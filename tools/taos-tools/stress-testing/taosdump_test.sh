#!/bin/bash

# 多种数据类型测试脚本
# 功能：自动测试不同数据类型的导入导出性能

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # 恢复默认颜色

# 检查Python脚本是否存在
if [ ! -f "taosdump_test.py" ]; then
    echo -e "${RED}错误: 未找到Python脚本 taosdump_test.py${NC}"
    exit 1
fi

# 清理之前的测试结果
echo -e "${YELLOW}清理之前的测试文件...${NC}"
rm -rf ./dump_data
rm -rf ./result
rm -rf ./logs

# 定义多种数据类型
TYPES=(
    "double"
    "int"
    "binary"
    "nchar"
    "varbinary"
    "all"
)

# 定义测试方式
TEST_MODES=("native")

# 定义子表数和每张子表写入行数的组合
declare -a SUBTABLE_COUNTS=(10000 100000 1000000)
declare -a ROWS_PER_SUBTABLE=(5000 1000 500 10000)

# declare -a SUBTABLE_COUNTS=(100)
# declare -a ROWS_PER_SUBTABLE=(5000)

# 执行所有类型测试
echo -e "${GREEN}开始执行多类型测试...${NC}"
for mode in "${TEST_MODES[@]}"; do
    for subtable_count in "${SUBTABLE_COUNTS[@]}"; do
        for rows_per_subtable in "${ROWS_PER_SUBTABLE[@]}"; do
            for type in "${TYPES[@]}"; do
                # 生成测试用例名称
                CASE_NAME="test_${type}_${subtable_count}_${rows_per_subtable}_${mode}"

                # 根据不同类型构造标签和列定义
                if [ "$type" == "double" ]; then
                    TAG_DEF='{"name":"tag_value", "type":"DOUBLE", "min":0, "max":100}'
                    COL_DEF='{"name":"column_value", "type":"DOUBLE", "min":0, "max":100}'
                elif [ "$type" == "int" ]; then
                    TAG_DEF='{"name":"tag_value", "type":"INT", "min":0, "max":100}'
                    COL_DEF='{"name":"column_value", "type":"INT", "min":0, "max":100}'
                elif [ "$type" == "binary" ]; then
                    TAG_DEF='{"name":"tag_value", "type":"BINARY", "len":32}'
                    COL_DEF='{"name":"column_value", "type":"BINARY", "len":32}'
                elif [ "$type" == "nchar" ]; then
                    TAG_DEF='{"name":"tag_value", "type":"NCHAR", "len":32}'
                    COL_DEF='{"name":"column_value", "type":"NCHAR", "len":32}'
                elif [ "$type" == "varbinary" ]; then
                    TAG_DEF='{"name":"tag_value", "type":"VARBINARY", "len":32}'
                    COL_DEF='{"name":"column_value", "type":"VARBINARY", "len":32}'
                elif [ "$type" == "all" ]; then
                    COL_DEF='all'
                fi

                echo -e "${YELLOW}执行 ${type} 类型测试...${NC}"

                # 调用Python脚本（添加了常用参数，可根据需要调整）
                python3 ./taosdump_test.py \
                    -name "${CASE_NAME}" \
                    -t "${subtable_count}" \
                    -n "${rows_per_subtable}" \
                    -A "${TAG_DEF}" \
                    -C "${COL_DEF}" \
                    -Z "${mode}"             # 连接模式

                # 检查测试结果
                RESULT_FILE="./result/${CASE_NAME}_result.json"
                if [ -f "$RESULT_FILE" ]; then
                    echo -e "${GREEN}${type} 类型测试完成，结果已保存至 ${RESULT_FILE}${NC}"
                else
                    echo -e "${RED}${type} 类型测试失败，未生成结果文件${NC}"
                fi

                echo "----------------------------------------"
            done
        done
    done
done


