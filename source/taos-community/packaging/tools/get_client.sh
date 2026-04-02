#!/bin/bash
#

log_dir=$1
result_file=$2

if [ ! -n "$1" ];then
  echo "Pleas input the director of taosdlog."
  echo "usage: ./get_client.sh <taosdlog directory> <result file>"
  exit 1
else
  log_dir=$1
fi

if [ ! -n "$2" ];then
  result_file=clientInfo.txt
else
  result_file=$2
fi

grep "new TCP connection" ${log_dir}/taosdlog.* | sed -e "s/0x.* from / /"|sed -e "s/,.*$//"|sed -e "s/:[0-9]*$//"|sort -r|uniq -f 2|sort -k 3 -r|uniq -f 2 > ${result_file}
