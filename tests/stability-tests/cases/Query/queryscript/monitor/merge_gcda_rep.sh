#!/bin/bash

# 配置参数
SEARCH_DIR="/root/TDinternal/debug/"               # 查找 .gcda 文件的根目录
OUTPUT_DIR="/root/merge_report_rep/"                  # 生成报告的目录
INFO_FILE="/root/merge_report_rep/merged_info.info"  # 合并后的 .info 文件名
FILTER_DIRS=("community/source" "community/tools" "community/util") # 需要包含的目录（相对于 SEARCH_DIR）
EXCLUDE_DIRS=("community/contrib")                         # 需要忽略的目录（相对于 SEARCH_DIR）

# 清理旧文件
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# 步骤1：查找所有 .gcda 文件，按文件名分组
declare -A gcda_files
echo "正在查找 .gcda 文件..."

# 构建 find 命令
find_command="find \"$SEARCH_DIR\" -name \"*.gcda\""
for dir in "${EXCLUDE_DIRS[@]}"; do
  find_command+=" -not -path \"$SEARCH_DIR$dir/*\""
done
for dir in "${FILTER_DIRS[@]}"; do
  find_command+=" -path \"$SEARCH_DIR$dir\" -o"
done
find_command+=" -false -prune -o -print 2>/dev/null"

# 打印完整的 find 命令
echo "执行的 find 命令："
echo "$find_command"

# 执行 find 命令
while IFS= read -r file; do
  filename=$(basename "$file")
  gcda_files["$filename"]+="$file "
  echo "找到文件：$file" # 打印被处理的文件路径
done < <(eval "$find_command")

# 打印被忽略的路径
echo "忽略的路径："
for dir in "${EXCLUDE_DIRS[@]}"; do
  echo "$SEARCH_DIR$dir/*"
done

# 检查是否有文件
if [ ${#gcda_files[@]} -eq 0 ]; then
  echo "未找到 .gcda 文件！"
  exit 1
fi

# 步骤2：合并同名 .gcda 文件的覆盖率数据
> "$INFO_FILE" # 初始化 .info 文件

merge_coverage() {
  local filename="$1"
  local files="$2"
  echo "正在合并文件：$filename"
  
  # 临时 .info 文件，使用文件路径的哈希值确保唯一性
  local temp_info="temp_$(echo "$filename" | md5sum | cut -d' ' -f1).info"
  
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
    # 检查临时 .info 文件是否为空
    if [ ! -s "$temp_info" ]; then
      echo "警告：临时文件 $temp_info 为空，跳过合并。" >> lcov_errors.log
      continue
    fi
  done

  # 返回临时 .info 文件路径
  echo "$temp_info"
}

export -f merge_coverage

# 使用 xargs 并发执行 merge_coverage 函数，并收集所有临时 .info 文件
temp_info_files=$(printf "%s\n" "${!gcda_files[@]}" | xargs -P 8 -I {} bash -c 'merge_coverage "$@"' _ {} "${gcda_files[{}]}")

# 合并所有临时 .info 文件到最终的 INFO_FILE
for temp_info in $temp_info_files; do
  if [ -s "$temp_info" ]; then
    lcov --add-tracefile "$temp_info" --output-file "$INFO_FILE" 2>> lcov_errors.log
    rm "$temp_info" # 删除临时文件
  fi
done

# 步骤3：生成 HTML 报告
genhtml "$INFO_FILE" --output-directory "$OUTPUT_DIR"

echo "覆盖率报告已生成：$OUTPUT_DIR/index.html"