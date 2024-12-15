#!/bin/bash
set -e 
set -x
# 检查是否传入了两个参数
# echo "参数个数: $#"
# if [ "$#" -ne 4 ]; then
#     echo "使用方法: $0 -i <sqlfile> -o <query_result_file>"
#     exit 1
# fi

# 读取传入的参数
while getopts "i:o:" opt; do
  case $opt in
    i)
      sqlfile="$OPTARG"
      ;;
    o)
      query_result_file="$OPTARG"
      ;;
    \?)
      echo "无效选项: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "选项 -$OPTARG 需要一个参数." >&2
      exit 1
      ;;
  esac
done

# 删除sqlfile文件中每一行末尾的分号和空格
sed -i 's/;\s*$//' "$sqlfile"

# 执行SQL文件并生成query_result_file文件
taos -f "$sqlfile" | grep -v 'Query OK' | grep -v 'Copyright' | grep -v 'Welcome to the TDengine Command' > "$query_result_file"
# echo  $(cat "$query_result_file")
# echo "1"
# sed -i 's/ ([^()]*)$//' "$query_result_file"
# echo "1"
# 打印输入的文件名
echo "输入的文件: $sqlfile"

# 打印输出的文件名
echo "输出的文件: $query_result_file"