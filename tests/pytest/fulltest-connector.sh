#!/bin/bash
ulimit -c unlimited
#======================p1-start===============

# restful test for python
# python3 test.py -f restful/restful_bind_db1.py
# python3 test.py -f restful/restful_bind_db2.py
python3 ./test.py -f client/nettest.py

python3 ./test.py -f ../system-test/4-taosAdapter/taosAdapter_query.py
python3 ./test.py -f ../system-test/4-taosAdapter/taosAdapter_insert.py

#======================p1-end===============
