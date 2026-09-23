#!/bin/bash

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.


# Due to the heavy path name assumptions/usage, let us require that the user be in the current directory
EXEC_DIR=`dirname "$0"`
if [[ $EXEC_DIR != "." ]]
then
    echo "ERROR: Please execute `basename "$0"` in its own directory (for now anyway, pardon the dust)"
    exit -1
fi

CURR_DIR=`pwd`
if [[ "$CURR_DIR" == *"taos-community"* ]]; then
  TAOS_DIR=$CURR_DIR/..
  TAOSD_DIR=`find $TAOS_DIR -name "taosd"|grep bin|grep -v packaging|head -n1`
  LIB_DIR=`dirname $(dirname $TAOSD_DIR)`/lib
elif [[ "$CURR_DIR" == *"community"* ]]; then
  TAOS_DIR=$CURR_DIR/../../..
  TAOSD_DIR=`find $TAOS_DIR -name "taosd"|grep bin|head -n1`
  LIB_DIR=`echo $TAOSD_DIR|rev|cut -d '/' -f 3,4,5,6,7|rev`/lib
else
  TAOS_DIR=$CURR_DIR/../..
  TAOSD_DIR=`find $TAOS_DIR -name "taosd"|grep bin|head -n1`
  LIB_DIR=`echo $TAOSD_DIR|rev|cut -d '/' -f 3,4,5,6|rev`/lib
fi

# Now getting ready to execute Python
# The following is the default of our standard dev env (Ubuntu 20.04), modify/adjust at your own risk
PYTHON_EXEC=python3

# First we need to set up a path for Python to find our own TAOS modules, so that "import" can work.
# export PYTHONPATH=$(pwd)/../../src/connector/python:$(pwd)
# NOTE: we are now pointing outside the current github, per Wade on 7/7/2022, we'll be keeping connectors outside
# and there does not seem to be a module to reference that.
if [[ "$CURR_DIR" == *"taos-community"* ]]; then
  PROJECT_PARENT=$(pwd)/../../..
  TAOS_PYTHON_PROJECT_DIR=$PROJECT_PARENT/source/taos-connector-python
else
  PROJECT_PARENT=$(pwd)/../../..
  TAOS_PYTHON_PROJECT_DIR=$PROJECT_PARENT/taos-connector-python
fi
export PYTHONPATH=$TAOS_PYTHON_PROJECT_DIR:$(pwd)

# Then let us set up the library path so that our compiled SO file can be loaded by Python
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LIB_DIR

# Now we are all let, and let's see if we can find a crash. Note we pass all params
CRASH_GEN_EXEC=crash_gen_bootstrap.py
if [[ $1 == '--valgrind' ]]; then
  shift
  export PYTHONMALLOC=malloc
  VALGRIND_OUT=valgrind.out 
  VALGRIND_ERR=valgrind.err
  # How to generate valgrind suppression file: https://stackoverflow.com/questions/17159578/generating-suppressions-for-memory-leaks
  # valgrind --leak-check=full --gen-suppressions=all --log-fd=9 python3.8 ./crash_gen.py $@ 9>>memcheck.log
  echo Executing under VALGRIND, with STDOUT/ERR going to $VALGRIND_OUT and $VALGRIND_ERR, please watch them from a different terminal.
  valgrind  \
    --leak-check=yes \
    --suppressions=crash_gen/valgrind_taos.supp \
    $PYTHON_EXEC \
    $CRASH_GEN_EXEC $@ > $VALGRIND_OUT 2> $VALGRIND_ERR 
elif [[ $1 == '--helgrind' ]]; then
  shift
  HELGRIND_OUT=helgrind.out 
  HELGRIND_ERR=helgrind.err
  valgrind  \
    --tool=helgrind \
    $PYTHON_EXEC \
    $CRASH_GEN_EXEC $@ > $HELGRIND_OUT 2> $HELGRIND_ERR
else
  $PYTHON_EXEC $CRASH_GEN_EXEC $@
fi

