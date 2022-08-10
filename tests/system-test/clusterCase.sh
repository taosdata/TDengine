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

# test case of vnode 
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_createDb_replica1.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica1_insertdatas.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_vgroups.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_vgroups_stopOne.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_stop_follower_sync.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_stop_follower_unsync.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_stop_follower_unsync_force_stop.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_stop_leader.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_stop_leader_forece_stop.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_querydatas_stop_follower.py  -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_querydatas_stop_follower_force_stop.py  -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_querydatas_stop_leader.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_querydatas_stop_leader_force_stop.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica1_insertdatas.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica1_insertdatas_querys.py -N 4 -M 1 
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_querys.py -N 4 -M 1 
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_querys_loop_restart_follower.py -N 4 -M 1 
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_querys_loop_restart_leader.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_querys_loop_restart_all_vnode.py   -N 4 -M 1 
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_stop_all_dnodes.py -N 4 -M 1 
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_force_stop_all_dnodes.py -N 4 -M 1 
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_mnode3_insertdatas_querys.py -N 4 -M 1

