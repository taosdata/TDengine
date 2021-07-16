#!/bin/sh

#指定JDK目录&AppName
APP_NAME=consume-to-tdengine

#nohup命令后台启动jar包并写入日志
nohup java -Dlog4j.configuration=./config/log4j.properties -jar $APP_NAME.jar --config config/$APP_NAME.json 1> logs/$APP_NAME.log 2> logs/$APP_NAME.error &

#sleep等待5秒后，判断包含AppName的线程是否存在
sleep 5

if test $(pgrep -f $APP_NAME | wc -l) -eq 0; then
  echo "Start Failed"
else
  echo "Start $APP_NAME Succeed"
fi