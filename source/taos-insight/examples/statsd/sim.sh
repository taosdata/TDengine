#!/bin/bash
inter=0.1
while [ 1 ]; do
  echo "measurement:$(( ( RANDOM % 100 ) ))|c" | nc -u -w0 127.0.0.1 8125
  sleep $inter
done
