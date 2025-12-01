import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDropDnodeHasMnode:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_drop_dnode_has_mnode(self):
        """Drop drop with mnode

        Drop the dnode containing the mnode

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/drop_dnode_has_mnode.sim

        """

        clusterComCheck.checkDnodes(2)

        tdLog.info(f"=============== step1 create dnode2")
        tdSql.execute(f"create dnode localhost port 6230")

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "offline")

        tdLog.info(f"=============== step2 drop dnode 3")
        tdSql.error(f"drop dnode 1")
        tdSql.execute(f"drop dnode 3 force")

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(2)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")

        tdLog.info(f"=============== step3: create mnode on dnode 2")
        tdSql.execute(f"create mnode on dnode 2")
        clusterComCheck.checkMnodeStatus(2)

        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkKeyData(1, 2, "leader")
        tdSql.checkKeyData(2, 2, "follower")

        tdLog.info(f"=============== step4: drop dnode 2")
        tdSql.error(f"drop dnode 1")
        tdSql.execute(f"drop dnode 2")

        tdLog.info(f"select * from information_schema.ins_dnodes;")
        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdLog.info(f"select * from information_schema.ins_mnodes;")
        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)
        tdSql.checkKeyData(1, 2, "leader")
