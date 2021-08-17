# This is the script for us to run our Python test cases with 2 important constraints:
#    
#	1. No root/sudo special rights is needed.
#	2. No files are needed outside the development tree, everything is done in the local source code directory

# First we need to set up a path for Python to find our own TAOS modules, so that "import" can work.
export PYTHONPATH=$(pwd)/../../src/connector/python

# Then let us set up the library path so that our compiled SO file can be loaded by Python
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/../../build/build/lib

# Now we are all let, and let's run our cases!
python3 ./test.py -m 127.0.0.1 -f insert/basic.py
