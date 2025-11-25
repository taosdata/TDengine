import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestMnodeBasic3:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_mnode_basic3(self):
        """Mnode kill -9 then restart

        1. Create mnodes on the second and third dnodes
        2. Create a user (update mnode)
        3. Kill dnode1 with kill -9
        4. Check if the user exists
        5. Stop dnode2 and dnode3 sequentially
        6. Check service availability

        Catalog:
            - ManageNodes:Mnode

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/mnode/basic3.sim

        """

        clusterComCheck.checkDnodes(4)
        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)
        tdSql.checkKeyData(1, 0, 1)
        tdSql.checkKeyData(1, 2, "leader")

        tdLog.info(f"=============== step2: create mnode 2 3")
        tdSql.execute(f"create mnode on dnode 2")
        tdSql.execute(f"create mnode on dnode 3")
        tdSql.error(f"create mnode on dnode 4")

        clusterComCheck.checkMnodeStatus(3)

        tdLog.info(f"=============== step3: create user")
        tdSql.execute(f"create user user1 PASS 'user121$*'")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)

        # wait mnode2 mnode3 recv data finish
        time.sleep(5)

        tdLog.info(f"=============== step4: stop dnode1")
        sc.dnodeForceStop(1)
        time.sleep(1)
        clusterComCheck.checkDnodes(3)

        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step5: stop dnode2")
        sc.dnodeStart(1)
        sc.dnodeStop(2)

        time.sleep(1)
        clusterComCheck.checkDnodes(3)

        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step6: stop dnode3")
        sc.dnodeStart(2)
        sc.dnodeStop(3)

        time.sleep(1)
        clusterComCheck.checkDnodes(3)

        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)
