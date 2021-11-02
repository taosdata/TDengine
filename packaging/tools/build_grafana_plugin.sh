#!/bin/bash
plugin=$1
target=$2

set -e

if [ -z "$(ls -A $plugin)" ]; then
   echo "warning: grafana plugin directory is empty"
   exit
fi

function _real_path() {
  dir="$@"
  if [ "$(command -v realpath)" != "" ]; then
    dir=$(realpath "$dir")
  elif [ `command -v readlink` != "" ]; then
    dir=$(readlink -f "$dir")
  else
    cc=$PWD
    cd $dir
    dir=$(pwd)
    cd $cc
  fi
  echo $dir
}

target2=$(_real_path $target)
echo $target2
if [ "$target" != "" ]; then
  if [ `command -v realpath` != "" ]; then
    target=$(realpath "$target")
  fi
fi

cd $plugin
rm -rf *-datasource-*.zip
yarn

wget -c -T 20 -t 2 https://github.com/magefile/mage/releases/download/v1.11.0/mage_1.11.0_Linux-64bit.tar.gz
tar -xvf mage_1.11.0_Linux-64bit.tar.gz -C node_modules/.bin/ mage

yarn build:all

if [ "$target" != "" ]; then
  [ -d "$target" ] || mkdir "$target"
  cp *-datasource-*.zip $target
fi

