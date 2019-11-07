#!/bin/bash

##################################################
# 
# Do release test 
#
##################################################

set +e

FILE_NAME=
RESTART=0
while getopts "f:r" arg 
do
  case $arg in
    f)
      FILE_NAME=$OPTARG
      ;;
    r)
      RESTART=1
      ;;
    ?)
      echo "unkonw argument"
      ;;
  esac
done


TOOL_DIR=`dirname $0`
cd $TOOL_DIR
TOOL_DIR=`pwd`


if [ $RESTART -eq 0 ]; then
  echo ========== make a new service environment
  source $TOOL_DIR/remove.sh
  source $TOOL_DIR/pre_build.sh
  source $TOOL_DIR/release.sh test
  source $TOOL_DIR/pre_install.sh
  source $TOOL_DIR/install.sh
else
  echo ========== use the exist service environment
fi

sudo systemctl start taosd
sudo systemctl start taosm

if [ -n "$FILE_NAME" ]; then
  source $TOOL_DIR/test_dev.sh -r -f $FILE_NAME
else
  source $TOOL_DIR/test_dev.sh -r
fi

#source $TOOL_DIR/remove.sh > /dev/null 2>&1

