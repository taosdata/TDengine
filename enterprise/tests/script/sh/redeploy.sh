#!/bin/sh

if [ $# != 6 ]; then 
  echo "argument list need input : "
  echo "  -n nodeName"
  echo "  -f fileName"
  echo "  -z deCompressMethod"
  exit 1
fi

NODE_NAME=
FILE_NAME=
FILE_TYPE=
while getopts "n:f:t:" arg 
do
  case $arg in
    n)
      NODE_NAME=$OPTARG
      ;;
    f)
      FILE_NAME=$OPTARG
      ;;
    t)
      FILE_TYPE=$OPTARG
      ;;
    ?)
      echo "unkonw argument"
      ;;
  esac
done

SCRIPT_DIR=`dirname $0`
cd $SCRIPT_DIR
SCRIPT_DIR=`pwd`

cd ../../
TAOS_DIR=`pwd`

cd ../
PARENT_DIR=`pwd`

BUILD_DIR=$PARENT_DIR/build
cd ../
TOP_TOP_DIR=`pwd`
SIM_DIR=$TOP_TOP_DIR/sim
SCRIPT_DIR=$TAOS_DIR/script
NODE_DIR=$SIM_DIR/$NODE_NAME
EXE_DIR=$BUILD_DIR/bin
CFG_DIR=$NODE_DIR/cfg
LOG_DIR=$NODE_DIR/log
DATA_DIR=$NODE_DIR/data

#echo ============ deploy $NODE_NAME
#echo === masterIp : $MASTER_IP
#echo === nodeIp : $NODE_IP
#echo === nodePath : $EXE_DIR
#echo === cfgPath : $CFG_DIR
#echo === logPath : $LOG_DIR
#echo === dataPath : $DATA_DIR

rm -rf $NODE_DIR
mkdir -p $SIM_DIR
FULL_FILE_NAME=$SCRIPT_DIR/$FILE_NAME

echo ============ re-deploy $NODE_NAME from $FULL_FILE_NAME
echo === fileName : $FULL_FILE_NAME
echo === decompress_path : $SIM_DIR

if [ "$FILE_TYPE" = "rar" ]; then 
  echo unrar x $FULL_FILE_NAME $SIM_DIR
  unrar x $FULL_FILE_NAME $SIM_DIR
else 
  echo tar -xzvf $FULL_FILE_NAME -C  $SIM_DIR
  tar -xzvf $FULL_FILE_NAME -C  $SIM_DIR
fi
	
#allow normal user to read/write log
chmod -R 777 $NODE_DIR

