#!/bin/sh

echo -n "hello" > /tmp/normal
echo -n "" > /tmp/empty
dd if=/dev/zero bs=3584 of=/tmp/big count=1

rm -rf /tmp/sum_double.so /tmp/add_one.so
touch /tmp/normal

gcc -g -O0 -fPIC -shared sh/sum_double.c -o /tmp/sum_double.so
gcc -g -O0 -fPIC -shared sh/add_one.c -o /tmp/add_one.so
gcc -g -O0 -fPIC -shared sh/demo.c -o /tmp/demo.so
gcc -g -O0 -fPIC -shared sh/abs_max.c -o /tmp/abs_max.so
