#!/bin/sh

#指定AppName
APP_NAME=consume-to-tdengine.jar

#找到包含AppName的进程
PROCESS=$(ps -ef | grep $APP_NAME | grep -v grep | awk '{ print $2}')

#循环停用进程直到成功
while :; do
  kill -9 $PROCESS >/dev/null 2>&1
  if [ $? -ne 0 ]; then
    break
  else
    continue
  fi
done

echo 'Stop Successed'
