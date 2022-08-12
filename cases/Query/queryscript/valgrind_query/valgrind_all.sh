#!/bin/bash

cd /root/
chmod 777 *
mkdir -p valgrind/1/
mkdir -p valgrind/2/
mkdir -p valgrind/3/
mkdir -p valgrind/4/
mkdir -p valgrind/5/
mkdir -p valgrind/6/
mkdir -p valgrind/7/
mkdir -p valgrind/8/
mkdir -p valgrind/9/
mkdir -p valgrind/0/
chmod 777 *

./valgrind_0.sh >valgrind/0/00.log 2>&1 &
./valgrind_1.sh >valgrind/1/11.log 2>&1 &
./valgrind_2.sh >valgrind/2/22.log 2>&1 &
./valgrind_3.sh >valgrind/3/33.log 2>&1 &
./valgrind_4.sh >valgrind/4/44.log 2>&1 &
./valgrind_5.sh >valgrind/5/55.log 2>&1 &
./valgrind_6.sh >valgrind/6/66.log 2>&1 &
./valgrind_7.sh >valgrind/7/77.log 2>&1 &
./valgrind_8.sh >valgrind/8/88.log 2>&1 &
./valgrind_9.sh >valgrind/9/99.log 2>&1 &

wait