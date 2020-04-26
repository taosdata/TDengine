#!/bin/bash
python3 ./test.py $1 -f insert/basic.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/int.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/float.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/bigint.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/bool.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/double.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/smallint.py
python3 ./test.py -s $1
sleep 1
python3 ./test.py $1 -f insert/tinyint.py
python3 ./test.py -s $1
sleep 1
