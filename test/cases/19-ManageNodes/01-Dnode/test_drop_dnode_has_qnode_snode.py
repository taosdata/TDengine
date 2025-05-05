import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDropDnodeHasQnodeSnode:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_drop_dnode_has_qnode_snode(self):
        """drop dnode has qnode snode

        1. -

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated to new test framework, from tsim/dnode/drop_dnode_has_qnode_snode.sim

        """

        clusterComCheck.checkDnodes(2)

#system sh/stop_dnodes.sh
#system sh/deploy.sh -n dnode1 -i 1
#system sh/deploy.sh -n dnode2 -i 2
#system sh/exec.sh -n dnode1 -s start
#system sh/exec.sh -n dnode2 -s start
        tdSql.connect('root')

        tdLog.info(f'=============== step1 create dnode2')
        tdSql.execute(f"create dnode {hostname} port 7200")

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
        tdSql.checkRows(2)

        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdLog.info(f'=============== step2: create qnode snode on dnode 2')
        tdSql.execute(f"create qnode on dnode 2")
        tdSql.execute(f"create snode on dnode 2")

        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(1)

        tdSql.query(f"show snodes")
        tdSql.checkRows(1)

        tdLog.info(f'=============== step3: drop dnode 2')
        tdSql.execute(f"drop dnode 2")

        tdLog.info(f'select * from information_schema.ins_dnodes;')
        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(0)

        tdSql.query(f"show snodes")
        tdSql.checkRows(0)

#system sh/exec.sh -n dnode1 -s stop -x SIGINT
#system sh/exec.sh -n dnode2 -s stop -x SIGINT
