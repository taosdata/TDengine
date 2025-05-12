from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestImportLarge:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_import_large(self):
        """import data large

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/import/large.sim

        """

        tdSql.execute(f"create database ir1db duration 7")
        tdSql.execute(f"use ir1db")
        tdSql.execute(f"create table tb(ts timestamp, i bigint)")

        tdLog.info(f"================= step1")
        tdSql.execute(f"import into tb values(1520000010000, 1520000010000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(1)

        tdLog.info(f"================= step2")
        tdSql.execute(f"insert into tb values(1520000008000, 1520000008000)")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(2)

        tdLog.info(f"================= step3")
        tdSql.execute(f"insert into tb values(1520000020000, 1520000020000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(3)

        tdLog.info(f"================= step4")
        tdSql.execute(f"import into tb values(1520000009000, 1520000009000)")
        tdSql.execute(f"import into tb values(1520000015000, 1520000015000)")
        tdSql.execute(f"import into tb values(1520000030000, 1520000030000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(6)

        tdLog.info(f"================= step5")
        tdSql.execute(f"insert into tb values(1520000008000, 1520000008000)")
        tdSql.execute(f"insert into tb values(1520000014000, 1520000014000)")
        tdSql.execute(f"insert into tb values(1520000025000, 1520000025000)")
        tdSql.execute(f"insert into tb values(1520000040000, 1520000040000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(9)

        tdLog.info(f"================= step6")
        tdSql.execute(f"import into tb values(1520000007000, 1520000007000)")
        tdSql.execute(f"import into tb values(1520000012000, 1520000012000)")
        tdSql.execute(f"import into tb values(1520000023000, 1520000023000)")
        tdSql.execute(f"import into tb values(1520000034000, 1520000034000)")
        tdSql.execute(f"import into tb values(1520000050000, 1520000050000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(14)

        tdLog.info(f"================== dnode restart")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.execute(f"use ir1db")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(14)

        tdLog.info(f"================= step7")
        tdSql.execute(f"import into tb values(1520000007001, 1520000007001)")
        tdSql.execute(f"import into tb values(1520000012001, 1520000012001)")
        tdSql.execute(f"import into tb values(1520000023001, 1520000023001)")
        tdSql.execute(f"import into tb values(1520000034001, 1520000034001)")
        tdSql.execute(f"import into tb values(1520000050001, 1520000050001)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(19)

        tdLog.info(f"================= step8")
        tdSql.execute(f"insert into tb values(1520000008002, 1520000008002)")
        tdSql.execute(f"insert into tb values(1520000014002, 1520000014002)")
        tdSql.execute(f"insert into tb values(1520000025002, 1520000025002)")
        tdSql.execute(f"insert into tb values(1520000060000, 1520000060000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(23)

        tdLog.info(f"================= step9")
        # 1520000000000
        # sql import into tb values(now-30d, 7003)
        # sql import into tb values(now-20d, 34003)
        # sql import into tb values(now-10d, 34003)
        # sql import into tb values(now-5d, 34003)
        # sql import into tb values(now+1d, 50001)
        # sql import into tb values(now+2d, 50001)
        # sql import into tb values(now+6d, 50001)
        # sql import into tb values(now+8d, 50002)
        # sql import into tb values(now+10d, 50003)
        # sql import into tb values(now+12d, 50004)
        # sql import into tb values(now+14d, 50001)
        # sql import into tb values(now+16d, 500051)

        tdSql.execute(f"import into tb values(1517408000000, 1517408000000)")
        tdSql.execute(f"import into tb values(1518272000000, 1518272000000)")
        tdSql.execute(f"import into tb values(1519136000000, 1519136000000)")
        tdSql.execute(f"import into tb values(1519568000000, 1519568000000)")
        tdSql.execute(f"import into tb values(1519654400000, 1519654400000)")
        tdSql.execute(f"import into tb values(1519827200000, 1519827200000)")
        tdSql.execute(f"import into tb values(1520345600000, 1520345600000)")
        tdSql.execute(f"import into tb values(1520691200000, 1520691200000)")
        tdSql.execute(f"import into tb values(1520864000000, 1520864000000)")
        tdSql.execute(f"import into tb values(1521900800000, 1521900800000)")
        tdSql.execute(f"import into tb values(1523110400000, 1523110400000)")
        tdSql.execute(f"import into tb values(1521382400000, 1521382400000)")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(35)

        tdLog.info(f"================= step10")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.execute(f"use ir1db")
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(35)

        tdLog.info(f"================= step11")

        # sql import into tb values(now-50d, 7003) (now-48d, 7003) (now-46d, 7003) (now-44d, 7003) (now-42d, 7003)
        tdSql.execute(
            f"import into tb values(1515680000000, 1) (1515852800000, 2) (1516025600000, 3) (1516198400000, 4) (1516371200000, 5)"
        )
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(40)

        tdLog.info(f"================= step12")
        # 1520000000000
        # sql import into tb values(now-19d, -19) (now-18d, -18) (now-17d, -17) (now-16d, -16) (now-15d, -15) (now-14d, -14) (now-13d, -13) (now-12d, -12) (now-11d, -11)
        tdSql.execute(
            f"import into tb values(1518358400000, 6) (1518444800000, 7) (1518531200000, 8) (1518617600000, 9) (1518704000000, 10) (1518790400000, 11) (1518876800000, 12) (1518963200000, 13) (1519049600000, 14)"
        )
        tdSql.query(f"select * from tb;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(49)

        tdLog.info(f"================= step14")
        # 1520000000000
        # sql import into tb values(now-48d, -48)
        # sql import into tb values(now-38d, -38)
        # sql import into tb values(now-28d, -28)

        tdSql.execute(f"import into tb values(1515852800001, -48)")
        tdSql.execute(f"import into tb values(1516716800000, -38)")
        tdSql.execute(f"import into tb values(1517580800000, -28)")

        tdSql.query(f"select * from tb;")
        tdSql.checkRows(52)
