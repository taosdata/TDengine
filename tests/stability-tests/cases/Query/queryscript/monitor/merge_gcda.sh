#!/bin/bash

# 配置参数
SEARCH_DIR="/root/TDinternal/debug"               # 查找 .gcda 文件的根目录
OUTPUT_DIR="/root/merge_report"                  # 生成报告的目录
INFO_FILE="/root/merge_report/merged_info.info"  # 合并后的 .info 文件名
FILTER_DIRS=("community/source" "community/tools" "community/util" "community/source")                      # 需要包含的目录（相对于 SEARCH_DIR）

# 清理旧文件
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# 步骤1：查找所有 .gcda 文件，按文件名分组
declare -A gcda_files
while IFS= read -r file; do
  filename=$(basename "$file")
  gcda_files["$filename"]+="$file "
done < <(find "$SEARCH_DIR" -name "*gcda*" \
  $(for dir in "${FILTER_DIRS[@]}"; do echo "-path $SEARCH_DIR/$dir -o "; done) -false -prune -o -print 2>/dev/null)

# 检查是否有文件
if [ ${#gcda_files[@]} -eq 0 ]; then
  echo "未找到 .gcda 文件！"
  exit 1
fi

# 步骤2：合并同名 .gcda 文件的覆盖率数据
> "$INFO_FILE" # 初始化 .info 文件
for filename in "${!gcda_files[@]}"; do
  files=${gcda_files["$filename"]}
  echo "正在合并文件：$filename"
  
  # 临时 .info 文件，使用文件路径的哈希值确保唯一性
  temp_info="temp_$(echo "$filename" | md5sum | cut -d' ' -f1).info"
  
  # 收集覆盖率数据
  for file in $files; do
    echo "处理文件：$file"
    # 打印完整的 lcov 命令
    echo "执行命令：lcov --quiet --capture --directory \"$file\" --output-file \"$temp_info\" --rc lcov_branch_coverage=1 --rc genhtml_branch_coverage=1 --no-external --config-file lcov_tdengine.config" >> debug.log
    
    lcov --quiet --capture \
      --directory "$file" \
      --output-file "$temp_info" \
      --rc lcov_branch_coverage=1 \
      --rc genhtml_branch_coverage=1 \
      --no-external \
      --config-file lcov_tdengine.config 2>> lcov_errors.log
    
    if [ $? -ne 0 ]; then
      echo "lcov 捕获失败：$file" >> lcov_errors.log
      continue
    fi
    # 合并到总文件
    lcov --add-tracefile "$temp_info" --output-file "$INFO_FILE"
    # 删除临时文件
    rm "$temp_info"
  done
done

# 步骤3：生成 HTML 报告
genhtml "$INFO_FILE" --output-directory "$OUTPUT_DIR"

echo "覆盖率报告已生成：$OUTPUT_DIR/index.html"