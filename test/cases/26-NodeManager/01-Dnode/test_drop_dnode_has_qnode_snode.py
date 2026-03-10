import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDropDnodeHasQnodeSnode:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_drop_dnode_has_qnode_snode(self):
        """Drop drop with qnode and snode

        Drop the dnode containing the mnode and snode

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/drop_dnode_has_qnode_snode.sim

        """

        clusterComCheck.checkDnodes(2)

        tdLog.info(f"=============== step1 create dnode2")

        tdLog.info(f"=============== step2: create qnode snode on dnode 2")
        tdSql.execute(f"create qnode on dnode 2")
        tdSql.execute(f"create snode on dnode 2")

        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(1)

        tdSql.query(f"show snodes")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step3: drop dnode 2")
        tdSql.execute(f"drop dnode 2")

        tdLog.info(f"select * from information_schema.ins_dnodes;")
        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(0)

        tdSql.query(f"show snodes")
        tdSql.checkRows(0)
