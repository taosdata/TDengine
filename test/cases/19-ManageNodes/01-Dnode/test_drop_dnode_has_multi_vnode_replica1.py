import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDropDnodeHasMultiVnodeReplica1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_drop_dnode_has_multi_vnode_replica1(self):
        """drop dnode has multi vnode replica 1

        1. -

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated to new test framework, from tsim/dnode/drop_dnode_has_multi_vnode_replica1.sim

        """

        clusterComCheck.checkDnodes(4)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

        sc.dnodeStop(3)
        clusterComCheck.checkDnodes(2)

#system sh/stop_dnodes.sh
#system sh/deploy.sh -n dnode1 -i 1
#system sh/deploy.sh -n dnode2 -i 2
#system sh/deploy.sh -n dnode3 -i 3
#system sh/cfg.sh -n dnode1 -c supportVnodes -v 0
#system sh/exec.sh -n dnode1 -s start
#system sh/exec.sh -n dnode2 -s start
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
        tdSql.checkRows(3)

        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "offline")
        tdLog.info(f'=============== step2 create database')
        tdSql.execute(f"create database d1 vgroups 4")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table d1.st (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d1.c1 using st tags(1)")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

        tdSql.query(f"show d1.vgroups")
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdSql.checkRows(4)

        tdSql.checkKeyData(2, 3, 2)
        tdSql.checkKeyData(3, 3, 2)
        if data(4)[3] != 2 "":

        if data(5)[3] != 2 "":

        tdLog.info(f'=============== step3: start dnode 3')
#system sh/exec.sh -n dnode3 -s start
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
        tdSql.checkRows(3)

        tdSql.checkKeyData(1, 4, "ready")
  goto step4

        tdSql.checkKeyData(2, 4, "ready")
  goto step4

        tdSql.checkKeyData(3, 4, "ready")
  goto step4

        tdLog.info(f'=============== step3: drop dnode2')
        tdSql.execute(f"drop dnode 2")

        tdLog.info(f'select * from information_schema.ins_dnodes;')
        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
        tdSql.checkRows(2)

        tdSql.query(f"show d1.vgroups")
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdSql.checkRows(4)

        tdSql.checkKeyData(2, 3, 3)
        if data(3)[3] != 3 "":

        if data(4)[3] != 3 "":

        if data(5)[3] != 3 "":

        tdSql.execute(f"reset query cache")

        tdLog.info(f'=============== step4: select data')
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(1)

#system sh/exec.sh -n dnode1 -s stop -x SIGINT
#system sh/exec.sh -n dnode2 -s stop -x SIGINT
#system sh/exec.sh -n dnode3 -s stop -x SIGINT
