#!/bin/bash
ulimit -c unlimited
#======================p1-start===============
#======================p1-end===============

# restful test for python
# python3 test.py -f restful/restful_bind_db1.py
# python3 test.py -f restful/restful_bind_db2.py
python3 ./test.py -f client/nettest.py