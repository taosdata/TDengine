import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestOfflineReason:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_offline_reason(self):
        """Dnode check offline reason

        Check whether the offline_reason field of the offline dnode is correct.

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/offline_reason.sim

        """

        clusterComCheck.checkDnodes(1)

        tdLog.info(f"========== step1")
        tdSql.execute(f"create dnode localhost port 6130")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkData(1, 7, "status not received")

        tdLog.info(f"========== step2")
        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(2)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdSql.checkRows(2)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")

        tdLog.info(f"========== step3")
        time.sleep(5)
        sc.dnodeStop(2)
        clusterComCheck.checkDnodes(1)

        sql = "select * from information_schema.ins_dnodes"
        tdSql.checkDataLoop(1, 7, "status msg timeout", sql, loopCount = 20)

        tdLog.info(f"========== step4")
        tdSql.execute(f"drop dnode 2 force")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(1)

        tdLog.info(f"========== step5")
        sc.dnodeStart(2)
        tdSql.execute(f"create dnode localhost port 6130")
