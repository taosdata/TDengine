#!/bin/bash

# set LD_LIBRARY_PATH
export PATH=$PATH:/home/TDengine/debug/build/bin
export LD_LIBRARY_PATH=/home/TDengine/debug/build/lib
ln -s /home/TDengine/debug/build/lib/libtaos.so /usr/lib/libtaos.so 2>/dev/null
ln -s /home/TDengine/debug/build/lib/libtaos.so /usr/lib/libtaos.so.1 2>/dev/null
ln -s /home/TDengine/include/client/taos.h /usr/include/taos.h 2>/dev/null

# run crash_gen auto script
python3 /home/TDengine/tests/pytest/auto_crash_gen_valgrind.py