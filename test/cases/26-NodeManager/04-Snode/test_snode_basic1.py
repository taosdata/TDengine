import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSnodeBasic1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_snode_basic1(self):
        """Snode basic

        1. Repeated create and drop snodes
        2. Check the results of ins_snodes
        3. Restart the dnode and check the results of the snode

        Catalog:
            - ManageNodes:Snode

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/snode/basic1.sim

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

        tdLog.info(f"=============== create drop snode 1")
        tdSql.execute(f"create snode on dnode 1")
        tdSql.query(f"show snodes")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.error(f"create snode on dnode 1")
        tdSql.execute(f"drop snode on dnode 1")
        tdSql.query(f"show snodes")
        tdSql.checkRows(0)

        tdSql.error(f"drop snode on dnode 1")

        tdLog.info(f"=============== create drop snode 2")
        tdSql.execute(f"create snode on dnode 2")
        tdSql.query(f"show snodes")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.error(f"create snode on dnode 2")

        tdSql.execute(f"drop snode on dnode 2")
        tdSql.query(f"show snodes")
        tdSql.checkRows(0)

        tdSql.error(f"drop snode on dnode 2")

        tdLog.info(f"=============== create drop snodes")
        tdSql.execute(f"create snode on dnode 1")
        tdSql.execute(f"create snode on dnode 2")

        tdLog.info(f"=============== restart")
        sc.dnodeStop(1)
        sc.dnodeStop(2)
        sc.dnodeStart(1)
        sc.dnodeStart(2)

        tdSql.query(f"show snodes")
        tdSql.checkRows(2)
