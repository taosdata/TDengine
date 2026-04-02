#!/bin/bash

# 配置参数
SEARCH_DIR="/root/TDinternal/debug/community/tools"          # 要扫描的根目录
OUTPUT_FILE="merged.info"     # 最终合并的 .info 文件
TEMP_DIR="/root/testmerge/temp_info"        # 临时文件存放目录

# 创建 lcov 配置文件
cat > lcov_tdengine.config << EOF
# # 设置要忽略的文件和目录
# exclude_patterns=/root/TDinternal/enterprise/*
# /root/TDinternal/community/contrib/*
# /root/TDinternal/community/deps/*
# /root/TDinternal/community/test/*
# /root/TDinternal/community/tests/*
# /root/TDinternal/community/utils/*
# /root/TDinternal/community/examples/*
# /root/TDinternal/community/include/*
# /root/TDinternal/community/packaging/*
# /root/TDinternal/community/source/*
# /root/TDinternal/community/Testing/*
# /root/TDinternal/community/Testicmakeng/*
# /root/TDinternal/community/docs/*
# /root/TDinternal/community/debug/*
# /root/TDinternal/community/tools/taos-tools/deps/*
# /root/TDinternal/community/tools/tdgpt/*
# /root/TDinternal/community/tools/taosadapter/*
# /root/TDinternal/community/tools/src/*
# /root/TDinternal/community/tools/shell/*
# /root/TDinternal/community/tools/scripts/*
# /root/TDinternal/community/tools/keeper/*
# /root/TDinternal/community/tools/inc/*
# /root/TDinternal/community/tools/auto/*
# /root/TDinternal/community/tools/shell/*
EOF

# 创建临时目录
mkdir -p "$TEMP_DIR"
#trap 'rm -rf "$TEMP_DIR"' EXIT # 退出时自动清理临时文件

export TEMP_DIR  # 导出临时目录变量，供子进程使用

# 步骤1：遍历所有 .gcda 文件并生成临时 .info 文件
find "$SEARCH_DIR" -name "*.gcda" | xargs -P 24 -I {} bash -c '
  gcda_file="{}"
  temp_info="$TEMP_DIR/$(basename "$gcda_file")_$(echo "$gcda_file" | md5sum | cut -d" " -f1).info"
  
  echo "处理文件：$gcda_file"

  # #old
  # # 打印完整的 lcov 命令
  # echo "执行命令：lcov --capture --initial --directory \"$gcda_file\" --output-file \"$temp_info\"" >> debug_lcov.log
  
  # lcov --capture --initial \
  #   --directory "$gcda_file" \
  #   --output-file "$temp_info" 2>> lcov_errors.log
  
  # 打印完整的 lcov 命令
  echo "执行命令：lcov --quiet --capture --directory \"$gcda_file\" --output-file \"$temp_info\" --rc lcov_branch_coverage=1 --rc genhtml_branch_coverage=1 --no-external --config-file lcov_tdengine.config" >> debug_lcov.log
  
  # lcov --quiet --capture --initial \
  #   --directory "$gcda_file" \
  #   --output-file "$temp_info" \
  #   --rc lcov_branch_coverage=1 \
  #   --rc genhtml_branch_coverage=1 \
  #   --no-external \
  #   --config-file lcov_tdengine.config 2>> lcov_errors.log

  lcov --capture --initial \
    --directory "$gcda_file" \
    --output-file "$temp_info" \
    --rc lcov_branch_coverage=1 \
    --rc genhtml_branch_coverage=1 2>> lcov_errors.log
  
  if [ $? -ne 0 ]; then
    echo "错误：处理 $gcda_file 失败" >> lcov_errors.log
    exit 1
  fi
  
  if [ ! -s "$temp_info" ]; then
    echo "警告：临时文件 $temp_info 为空" >> lcov_errors.log
  fi
'

# 步骤2：合并所有临时 .info 文件
echo "合并所有 .info 文件..."
temp_merged_file="$TEMP_DIR/temp_merged.info" # 临时合并文件
> "$temp_merged_file" # 初始化临时合并文件

for temp_info in "$TEMP_DIR"/*.info; do
  echo "正在合并文件：$temp_info"
  
  if [ ! -s "$temp_merged_file" ]; then
    # 如果临时合并文件为空，直接初始化为当前文件
    cp "$temp_info" "$temp_merged_file"
  else
    # 合并当前文件到临时合并文件
    lcov --add-tracefile "$temp_info" \
         --add-tracefile "$temp_merged_file" \
         --output-file "$temp_merged_file" \
         --rc lcov_branch_coverage=1 \
         --rc genhtml_branch_coverage=1 2>> lcov_errors.log
    if [ $? -ne 0 ]; then
      echo "错误：合并 $temp_info 失败" >> lcov_errors.log
      exit 1
    fi
  fi
  
  # 打印完整的 lcov 命令
  echo "执行命令：lcov --add-tracefile \"$temp_info\" --add-tracefile \"$temp_merged_file\" --output-file \"$temp_merged_file\" --rc lcov_branch_coverage=1 --rc genhtml_branch_coverage=1" >> debug_merge.log
done

# 将最终合并的文件复制到 OUTPUT_FILE
cp "$temp_merged_file" "$OUTPUT_FILE"

# 检查合并结果
if [ ! -s "$OUTPUT_FILE" ]; then
  echo "错误：合并后的 .info 文件为空"
  exit 1
fi

echo "✅ 成功生成最终报告：$OUTPUT_FILE"