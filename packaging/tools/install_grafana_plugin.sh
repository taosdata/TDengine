#!/bin/bash
target=$1
tmp=/tmp

if [ "$target" != "" ]; then
  [ -d "$target" ] || mkdir "$target"
  cd $target
  target=$(pwd)
  cd $tmp
  wget -c -T 20 -t 2 https://github.com/taosdata/grafanaplugin/releases/download/v3.1.1/tdengine-datasource-3.1.1.zip
  cp tdengine-datasource-3.1.1.zip $target
fi