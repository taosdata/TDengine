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

# First we need to set up a path for Python to find our own TAOS modules, so that "import" can work.
export PYTHONPATH=$(pwd)/../../src/connector/python/linux/python3

# Then let us set up the library path so that our compiled SO file can be loaded by Python
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/../../build/build/lib

# Now we are all let, and let's see if we can find a crash. Note we pass all params
./crash_gen.py $@
