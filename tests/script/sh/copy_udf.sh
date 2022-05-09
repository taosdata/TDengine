#!/bin/bash

set +e
#set -x

echo "Executing copy_udf.sh"

SCRIPT_DIR=`pwd`
cd $SCRIPT_DIR/../

IN_TDINTERNAL="community"
if [[ "$SCRIPT_DIR" == *"$IN_TDINTERNAL"* ]]; then
  cd ../../..
else
  cd ../../
fi

TAOS_DIR=`pwd`
UDF1_DIR=`find $TAOS_DIR -name "libudf1.so"|grep lib|head -n1`
UDF2_DIR=`find $TAOS_DIR -name "libudf2.so"|grep lib|head -n1`

echo $UDF1_DIR
echo $UDF2_DIR

UDF_TMP=/tmp/udf
rm -rf $UDF_TMP
mkdir $UDF_TMP

echo "Copy udf shared library files to $UDF_TMP"

cp $UDF1_DIR $UDF_TMP
echo "copy udf1 result: $?" 
cp $UDF2_DIR $UDF_TMP
echo "copy udf2 result: $?"
