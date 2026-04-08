#!/bin/sh

#指定JDK目录&AppName
APP_NAME=consume-to-tdengine

#nohup命令后台启动jar包并写入日志
nohup java -Dlog4j.configuration=file:./config/log4j.properties -jar $APP_NAME.jar --config config/$APP_NAME.json 1> logs/$APP_NAME.log 2> logs/$APP_NAME.error &
