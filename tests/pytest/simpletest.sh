#!/bin/bash
python3 ./test.py -f insert/basic.py $1
python3 ./test.py -s $1
sleep 1
python3 ./test.py -f insert/int.py $1
python3 ./test.py -s $1
sleep 1
python3 ./test.py -f insert/float.py $1
python3 ./test.py -s $1
