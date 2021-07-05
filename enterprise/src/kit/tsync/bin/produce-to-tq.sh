#!/bin/sh

#指定JDK目录&AppName
APP_NAME=produce-to-tq.jar
echo $APP_NAME

java -jar $APP_NAME --config config/produce-to-tq.json
exit 0
