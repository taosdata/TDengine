#!/bin/bash
set -e
set -x

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
