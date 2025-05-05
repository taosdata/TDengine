import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDropDnodeHasMultiVnodeReplica3:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_drop_dnode_has_multi_vnode_replica3(self):
        """drop dnode has multi vnode replica3

        1. -

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated to new test framework, from tsim/dnode/drop_dnode_has_multi_vnode_replica3.sim

        """

        clusterComCheck.checkDnodes(5)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

        sc.dnodeStop(5)
        clusterComCheck.checkDnodes(4)

#system sh/stop_dnodes.sh
#system sh/deploy.sh -n dnode1 -i 1
#system sh/deploy.sh -n dnode2 -i 2
#system sh/deploy.sh -n dnode3 -i 3
#system sh/deploy.sh -n dnode4 -i 4
#system sh/deploy.sh -n dnode5 -i 5
#system sh/cfg.sh -n dnode1 -c supportVnodes -v 0
#system sh/exec.sh -n dnode1 -s start
#system sh/exec.sh -n dnode2 -s start
#system sh/exec.sh -n dnode3 -s start
#system sh/exec.sh -n dnode4 -s start
        tdSql.connect('root')

        tdLog.info(f'=============== step1 create dnode2 dnode3 dnode4 dnode 5')
        tdSql.execute(f"create dnode {hostname} port 7200")
        tdSql.execute(f"create dnode {hostname} port 7300")
        tdSql.execute(f"create dnode {hostname} port 7400")
        tdSql.execute(f"create dnode {hostname} port 7500")

        x = 0
step1:
	 = x + 1
	sleep 1000
	if x == 10 "":
	          tdLog.info(f'====> dnode not online!')
        #return -1

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}')
        tdSql.checkRows(5)

        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")
        tdSql.checkKeyData(5, 4, "offline")
        tdLog.info(f'=============== step3 create database')
        tdSql.execute(f"create database d1 vgroups 4 replica 3")

        tdLog.info(f'=============== step32 wait vgroup2')
        x = 0
step32:
	x = x + 1
	sleep 1000
	if x == 60 "":
	          tdLog.info(f'====> db not ready!')
        #return -1

        tdSql.query(f"show d1.vgroups")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)} {tdSql.getData(1,6)} {tdSql.getData(1,7)} {tdSql.getData(1,8)} {tdSql.getData(1,9)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)} {tdSql.getData(2,6)} {tdSql.getData(2,7)} {tdSql.getData(2,8)} {tdSql.getData(2,9)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)} {tdSql.getData(3,6)} {tdSql.getData(3,7)} {tdSql.getData(3,8)} {tdSql.getData(3,9)}')
        tdSql.checkRows(4)

        if data(2)[4] == "leader" "":
  leaderExist = 1

        if data(2)[7] == "leader" "":
  leaderExist = 1

        if data(2)[10] == "leader" "":
  leaderExist = 1

        if  leaderExist != 1 "":
  goto step32

        tdLog.info(f'=============== step33 wait vgroup3')
        x = 0
step33:
	x = x + 1
	sleep 1000
	if x == 60 "":
	          tdLog.info(f'====> db not ready!')
        #return -1

        tdSql.query(f"show d1.vgroups")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)} {tdSql.getData(1,6)} {tdSql.getData(1,7)} {tdSql.getData(1,8)} {tdSql.getData(1,9)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)} {tdSql.getData(2,6)} {tdSql.getData(2,7)} {tdSql.getData(2,8)} {tdSql.getData(2,9)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)} {tdSql.getData(3,6)} {tdSql.getData(3,7)} {tdSql.getData(3,8)} {tdSql.getData(3,9)}')
        tdSql.checkRows(4)

        if data(3)[4] == "leader" "":
  leaderExist = 1

        if data(3)[7] == "leader" "":
  leaderExist = 1

        if data(3)[10] == "leader" "":
  leaderExist = 1

        if  leaderExist != 1 "":
  goto step33

        tdLog.info(f'=============== step34 wait vgroup4')
        x = 0
step34:
	x = x + 1
	sleep 1000
	if x == 60 "":
	          tdLog.info(f'====> db not ready!')
        #return -1

        tdSql.query(f"show d1.vgroups")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)} {tdSql.getData(1,6)} {tdSql.getData(1,7)} {tdSql.getData(1,8)} {tdSql.getData(1,9)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)} {tdSql.getData(2,6)} {tdSql.getData(2,7)} {tdSql.getData(2,8)} {tdSql.getData(2,9)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)} {tdSql.getData(3,6)} {tdSql.getData(3,7)} {tdSql.getData(3,8)} {tdSql.getData(3,9)}')
        tdSql.checkRows(4)

        if data(4)[4] == "leader" "":
  leaderExist = 1

        if data(4)[7] == "leader" "":
  leaderExist = 1

        if data(4)[10] == "leader" "":
  leaderExist = 1

        if  leaderExist != 1 "":
  goto step34

        tdLog.info(f'=============== step35 wait vgroup5')
        x = 0
step35:
	x = x + 1
	sleep 1000
	if x == 60 "":
	          tdLog.info(f'====> db not ready!')
        #return -1

        tdSql.query(f"show d1.vgroups")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)} {tdSql.getData(1,6)} {tdSql.getData(1,7)} {tdSql.getData(1,8)} {tdSql.getData(1,9)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)} {tdSql.getData(2,6)} {tdSql.getData(2,7)} {tdSql.getData(2,8)} {tdSql.getData(2,9)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)} {tdSql.getData(3,6)} {tdSql.getData(3,7)} {tdSql.getData(3,8)} {tdSql.getData(3,9)}')
        tdSql.checkRows(4)

        if data(4)[4] == "leader" "":
  leaderExist = 1

        if data(4)[7] == "leader" "":
  leaderExist = 1

        if data(4)[10] == "leader" "":
  leaderExist = 1

        if  leaderExist != 1 "":
  goto step35

        wt = 0
stepwt1:
 	wt = wt + 1
 	sleep 1000
 	if wt == 200 "":
 	          tdLog.info(f'====> dnode not ready!')
        #return -1

        tdSql.query(f"show transactions")
        tdSql.checkRows(0)
          tdLog.info(f'wait 1 seconds to alter')
  goto stepwt1

        tdLog.info(f'=============== step36: create table')
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table d1.st (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d1.c1 using st tags(1)")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdLog.info(f'=============== step4: drop dnode 2')
#system sh/exec.sh -n dnode5 -s start
        x = 0
step4:
	 = x + 1
	sleep 1000
	if x == 10 "":
	          tdLog.info(f'====> dnode not online!')
        #return -1

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}')
        tdSql.checkRows(5)

        tdSql.checkKeyData(1, 4, "ready")
  goto step4

        tdSql.checkKeyData(2, 4, "ready")
  goto step4

        tdSql.checkKeyData(3, 4, "ready")
  goto step4

        tdSql.checkKeyData(4, 4, "ready")
  goto step4

        tdSql.checkKeyData(5, 4, "ready")
  goto step4

        tdLog.info(f'=============== step5: drop dnode2')
        tdSql.execute(f"drop dnode 2")

        tdLog.info(f'select * from information_schema.ins_dnodes;')
        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
        tdSql.checkRows(4)

        tdLog.info(f'=============== step62 wait vgroup2')
        x = 0
step62:
	x = x + 1
	sleep 1000
	if x == 60 "":
	          tdLog.info(f'====> db not ready!')
        #return -1

        tdSql.query(f"show d1.vgroups")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)} {tdSql.getData(1,6)} {tdSql.getData(1,7)} {tdSql.getData(1,8)} {tdSql.getData(1,9)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)} {tdSql.getData(2,6)} {tdSql.getData(2,7)} {tdSql.getData(2,8)} {tdSql.getData(2,9)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)} {tdSql.getData(3,6)} {tdSql.getData(3,7)} {tdSql.getData(3,8)} {tdSql.getData(3,9)}')
        tdSql.checkRows(4)

        if data(2)[4] == "leader" "":
  leaderExist = 1

        if data(2)[7] == "leader" "":
  leaderExist = 1

        if data(2)[10] == "leader" "":
  leaderExist = 1

        if  leaderExist != 1 "":
  goto step62

        tdLog.info(f'=============== step63 wait vgroup3')
        x = 0
step63:
	x = x + 1
	sleep 1000
	if x == 60 "":
	          tdLog.info(f'====> db not ready!')
        #return -1

        tdSql.query(f"show d1.vgroups")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)} {tdSql.getData(1,6)} {tdSql.getData(1,7)} {tdSql.getData(1,8)} {tdSql.getData(1,9)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)} {tdSql.getData(2,6)} {tdSql.getData(2,7)} {tdSql.getData(2,8)} {tdSql.getData(2,9)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)} {tdSql.getData(3,6)} {tdSql.getData(3,7)} {tdSql.getData(3,8)} {tdSql.getData(3,9)}')
        tdSql.checkRows(4)

        if data(3)[4] == "leader" "":
  leaderExist = 1

        if data(3)[7] == "leader" "":
  leaderExist = 1

        if data(3)[10] == "leader" "":
  leaderExist = 1

        if  leaderExist != 1 "":
  goto step63

        tdLog.info(f'=============== step64 wait vgroup4')
        x = 0
step64:
	x = x + 1
	sleep 1000
	if x == 60 "":
	          tdLog.info(f'====> db not ready!')
        #return -1

        tdSql.query(f"show d1.vgroups")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)} {tdSql.getData(1,6)} {tdSql.getData(1,7)} {tdSql.getData(1,8)} {tdSql.getData(1,9)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)} {tdSql.getData(2,6)} {tdSql.getData(2,7)} {tdSql.getData(2,8)} {tdSql.getData(2,9)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)} {tdSql.getData(3,6)} {tdSql.getData(3,7)} {tdSql.getData(3,8)} {tdSql.getData(3,9)}')
        tdSql.checkRows(4)

        if data(4)[4] == "leader" "":
  leaderExist = 1

        if data(4)[7] == "leader" "":
  leaderExist = 1

        if data(4)[10] == "leader" "":
  leaderExist = 1

        if  leaderExist != 1 "":
  goto step64

        tdLog.info(f'=============== step35 wait vgroup5')
        x = 0
step65:
	x = x + 1
	sleep 1000
	if x == 60 "":
	          tdLog.info(f'====> db not ready!')
        #return -1

        tdSql.query(f"show d1.vgroups")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)} {tdSql.getData(1,6)} {tdSql.getData(1,7)} {tdSql.getData(1,8)} {tdSql.getData(1,9)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)} {tdSql.getData(2,6)} {tdSql.getData(2,7)} {tdSql.getData(2,8)} {tdSql.getData(2,9)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)} {tdSql.getData(3,6)} {tdSql.getData(3,7)} {tdSql.getData(3,8)} {tdSql.getData(3,9)}')
        tdSql.checkRows(4)

        if data(4)[4] == "leader" "":
  leaderExist = 1

        if data(4)[7] == "leader" "":
  leaderExist = 1

        if data(4)[10] == "leader" "":
  leaderExist = 1

        if  leaderExist != 1 "":
  goto step65

        tdLog.info(f'=============== step7: select data')
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)}')
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

#system sh/exec.sh -n dnode1 -s stop -x SIGINT
#system sh/exec.sh -n dnode2 -s stop -x SIGINT
#system sh/exec.sh -n dnode3 -s stop -x SIGINT
