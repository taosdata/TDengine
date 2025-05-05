import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestUseDroppedDnode:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_use_dropped_dnode(self):
        """use dropped dnode

        1. -

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated to new test framework, from tsim/dnode/use_dropped_dnode.sim

        """

        clusterComCheck.checkDnodes(3)

#system sh/stop_dnodes.sh
#system sh/deploy.sh -n dnode1 -i 1
#system sh/deploy.sh -n dnode2 -i 2
#system sh/deploy.sh -n dnode3 -i 3
#system sh/exec.sh -n dnode1 -s start
#system sh/exec.sh -n dnode2 -s start
#system sh/exec.sh -n dnode3 -s start
        tdSql.connect('root')

        tdLog.info(f'=============== step1 create dnode2')
        tdSql.execute(f"create dnode {hostname} port 7200")
        tdSql.execute(f"create dnode {hostname} port 7300")

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
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}')
        tdSql.checkRows(3)

        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdLog.info(f'=============== step2 drop dnode 3')
        tdSql.execute(f"drop dnode 3")
        tdSql.execute(f"create dnode {hostname} port 7300")

        x = 0
step2:
	 = x + 1
	sleep 1000
	if x == 10 "":
	          tdLog.info(f'====> dnode not online!')
        #return -1

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}')
        tdSql.checkRows(3)

        tdSql.checkKeyData(1, 4, "ready")
  goto step2

        tdSql.checkKeyData(2, 4, "ready")
  goto step2

        if data(3)[4] != "null" "":
  goto step2

        tdSql.checkKeyData(4, 4, "offline")
  goto step2

        tdLog.info(f'=============== step3: create dnode 4')
sleep 1000
#system sh/exec.sh -n dnode3 -s stop -x SIGINT
#system sh/deploy.sh -n dnode3 -i 3
#system sh/exec.sh -n dnode3 -s start

        x = 0
step3:
	 = x + 1
	sleep 1000
	if x == 10 "":
	          tdLog.info(f'====> dnode not online!')
        #return -1

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}')
        tdSql.checkRows(3)

        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        if data(3)[4] != "null" "":
        tdSql.checkKeyData(4, 4, "ready")
        tdLog.info(f'=============== step4: create mnode 4')
        tdSql.execute(f"create mnode on dnode 4")

        x = 0
step4:
	 = x + 1
	sleep 1000
	if x == 10 "":
	          tdLog.info(f'====> dnode not online!')
        #return -1

        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}')
        tdSql.checkRows(2)

        if data(1)[2] != "leader" "":
  goto step4

        if data(4)[2] != "follower" "":
 goto step4

#system sh/exec.sh -n dnode1 -s stop -x SIGINT
#system sh/exec.sh -n dnode2 -s stop -x SIGINT
#system sh/exec.sh -n dnode3 -s stop -x SIGINT
