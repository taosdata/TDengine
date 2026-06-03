#!/bin/bash
IN_TDINTERNAL="community"
TDIR=`pwd`
if [[ "$tests_dir" == *"$IN_TDINTERNAL"* ]]; then
  cd ../..
else
  cd ../../..
fi

TOP_DIR=`pwd`
TAOSLIB_DIR=`find . -name "libtaos.so"|grep -w lib|head -n1`
if [[ "$TAOSLIB_DIR" == *"$IN_TDINTERNAL"* ]]; then
  LIB_DIR=`find . -name "libtaos.so"|grep -w lib|head -n1|cut -d '/' --fields=2,3,4,5`
else
  LIB_DIR=`find . -name "libtaos.so"|grep -w lib|head -n1|cut -d '/' --fields=2,3,4`
fi
if [ ! $LD_LIBRARY_PATH ]; then
        export LD_LIBRARY_PATH=$TOP_DIR/$LIB_DIR
else
        export LD_LIBRARY_PATH=$TOP_DIR/$LIB_DIR:$LD_LIBRARY_PATH
fi

cd $TDIR
# client
PYTHONMALLOC=malloc python3 ./test.py -g -f client/client.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1

# insert
PYTHONMALLOC=malloc python3 ./test.py -g -f insert/basic.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1
PYTHONMALLOC=malloc python3 ./test.py -g -f insert/int.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1
PYTHONMALLOC=malloc python3 ./test.py -g -f insert/float.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1
PYTHONMALLOC=malloc python3 ./test.py -g -f insert/bigint.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1
PYTHONMALLOC=malloc python3 ./test.py -g -f insert/bool.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1
PYTHONMALLOC=malloc python3 ./test.py -g -f insert/double.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1
PYTHONMALLOC=malloc python3 ./test.py -g -f insert/smallint.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1
PYTHONMALLOC=malloc python3 ./test.py -g -f insert/tinyint.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1
PYTHONMALLOC=malloc python3 ./test.py -g -f insert/binary.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1
PYTHONMALLOC=malloc python3 ./test.py -g -f insert/date.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1
PYTHONMALLOC=malloc python3 ./test.py -g -f insert/nchar.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1
PYTHONMALLOC=malloc python3 ./test.py -g -f insert/multi.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1

# table
PYTHONMALLOC=malloc python3 ./test.py -g -f table/column_name.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1
PYTHONMALLOC=malloc python3 ./test.py -g -f table/column_num.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1
PYTHONMALLOC=malloc python3 ./test.py -g -f table/db_table.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1

# import
PYTHONMALLOC=malloc python3 ./test.py -g -f import_merge/importDataLastSub.py
PYTHONMALLOC=malloc python3 ./test.py -g -s && sleep 1

#tag 
PYTHONMALLOC=malloc python3 ./test.py $1 -f tag_lite/filter.py
PYTHONMALLOC=malloc python3 ./test.py $1 -s && sleep 1
