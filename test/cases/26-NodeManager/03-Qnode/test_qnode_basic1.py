import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestQnodeBasic1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_qnode_basic1(self):
        """Qnode basic

        1. Repeated create and drop qnodes
        2. Check the results of ins_qnodes
        3. Restart the dnode and check the results of the qnode


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/qnode/basic1.sim

        """

        clusterComCheck.checkDnodes(2)

        tdLog.info(f"=============== select * from information_schema.ins_dnodes")
        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(1, 2, 0)
        tdSql.checkData(0, 4, "ready")
        tdSql.checkData(1, 4, "ready")

        tdSql.query(f"select * from information_schema.ins_mnodes;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 2, "leader")

        tdLog.info(f"=============== create drop qnode 1")
        tdSql.execute(f"create qnode on dnode 1")
        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.error(f"create qnode on dnode 1")
        tdSql.execute(f"drop qnode on dnode 1")
        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(0)

        tdSql.error(f"drop qnode on dnode 1")
        tdLog.info(f"=============== create drop qnode 2")
        tdSql.execute(f"create qnode on dnode 2")
        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.error(f"create qnode on dnode 2")

        tdSql.execute(f"drop qnode on dnode 2")
        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(0)

        tdSql.error(f"drop qnode on dnode 2")

        tdLog.info(f"=============== create drop qnodes")
        tdSql.execute(f"create qnode on dnode 1")
        tdSql.execute(f"create qnode on dnode 2")
        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(2)

        tdLog.info(f"=============== restart")
        sc.dnodeStop(1)
        sc.dnodeStop(2)
        sc.dnodeStart(1)
        sc.dnodeStart(2)

        clusterComCheck.checkDnodes(2)

        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(2)
