#!/bin/bash

# This is the script for us to try to cause the TDengine server or client to crash
#    
# PREPARATION
#
# 1. Build an compile the TDengine source code that comes with this script, in the same directory tree
# 2. Please follow the direction in our README.md, and build TDengine in the build/ directory
# 3. Adjust the configuration file if needed under build/test/cfg/taos.cfg
# 4. Run the TDengine server instance: cd build; ./build/bin/taosd -c test/cfg
# 5. Make sure you have a working Python3 environment: run /usr/bin/python3 --version, and you should get 3.6 or above
# 6. Make sure you have the proper Python packages: # sudo apt install python3-setuptools python3-pip python3-distutils
#
# RUNNING THIS SCRIPT
# 
# This script assumes the source code directory is intact, and that the binaries has been built in the
# build/ directory, as such, will will load the Python libraries in the directory tree, and also load
# the TDengine client shared library (so) file, in the build/directory, as evidenced in the env
# variables below.
#
# Running the script is simple, no parameter is needed (for now, but will change in the future).
#
# Happy Crashing...


# Due to the heavy path name assumptions/usage, let us require that the user be in the current directory
EXEC_DIR=`dirname "$0"`
if [[ $EXEC_DIR != "." ]]
then
    echo "ERROR: Please execute `basename "$0"` in its own directory (for now anyway, pardon the dust)"
    exit -1
fi

CURR_DIR=`pwd`
IN_TDINTERNAL="community"
if [[ "$CURR_DIR" == *"$IN_TDINTERNAL"* ]]; then
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
PYTHON_EXEC=python3.8

# First we need to set up a path for Python to find our own TAOS modules, so that "import" can work.
export PYTHONPATH=$(pwd)/../../src/connector/python/linux/python3:$(pwd)

# Then let us set up the library path so that our compiled SO file can be loaded by Python
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LIB_DIR

# Now we are all let, and let's see if we can find a crash. Note we pass all params
CONCURRENT_INQUIRY=concurrent_inquiry.py
if [[ $1 == '--valgrind' ]]; then
  shift
  export PYTHONMALLOC=malloc
  VALGRIND_OUT=valgrind.out 
  VALGRIND_ERR=valgrind.err
  # How to generate valgrind suppression file: https://stackoverflow.com/questions/17159578/generating-suppressions-for-memory-leaks
  # valgrind --leak-check=full --gen-suppressions=all --log-fd=9 python3.8 ./concurrent_inquiry.py $@ 9>>memcheck.log
  echo Executing under VALGRIND, with STDOUT/ERR going to $VALGRIND_OUT and $VALGRIND_ERR, please watch them from a different terminal.
  valgrind  \
    --leak-check=yes \
    --suppressions=crash_gen/valgrind_taos.supp \
    $PYTHON_EXEC \
    $CONCURRENT_INQUIRY $@ > $VALGRIND_OUT 2> $VALGRIND_ERR 
elif [[ $1 == '--helgrind' ]]; then
  shift
  HELGRIND_OUT=helgrind.out 
  HELGRIND_ERR=helgrind.err
  valgrind  \
    --tool=helgrind \
    $PYTHON_EXEC \
    $CONCURRENT_INQUIRY $@ > $HELGRIND_OUT 2> $HELGRIND_ERR
else
  $PYTHON_EXEC $CONCURRENT_INQUIRY $@
fi

