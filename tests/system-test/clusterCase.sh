#!/bin/bash
set -e
set -x

python3 ./test.py -f 6-cluster/5dnode1mnode.py
#python3 ./test.py -f 6-cluster/5dnode2mnode.py  -N 5 -M 3
#python3 ./test.py -f 6-cluster/5dnode3mnodeStop.py -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeStopLoop.py -N 5 -M 3
# BUG python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateDb.py -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateDb.py -N 5 -M 3
# BUG python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopVnodeCreateDb.py  -N 5 -M 3
# BUG python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateStb.py -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateStb.py  -N 5 -M 3
# python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopVnodeCreateStb.py  -N 5 -M 3
# BUG python3 ./test.py -f 6-cluster/5dnode3mnodeStopInsert.py
# python3 ./test.py -f 6-cluster/5dnode3mnodeDrop.py -N 5
# python3 test.py -f 6-cluster/5dnode3mnodeStopConnect.py -N 5 -M 3
# BUG Redict python3 ./test.py -f 6-cluster/5dnode3mnodeAdd1Ddnoe.py -N 6 -M 3 -C 5
# python3 ./test.py -f 6-cluster/5dnode3mnodeRestartDnodeInsertData.py -N 5 -M 3
 python3 ./test.py -f 6-cluster/5dnode3mnodeAdd1Ddnoe.py -N 6 -M 3 -C 5 

 
