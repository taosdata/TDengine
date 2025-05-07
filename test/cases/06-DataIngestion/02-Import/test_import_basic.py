from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestImportBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_import_basic(self):
        """import data basic

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/import/basic.sim

        """

        tdSql.execute(f"create database ibadb")
        tdSql.execute(f"use ibadb")
        tdSql.execute(f"create table tb(ts timestamp, i int)")

        tdLog.info(f"================= step1")

        tdSql.execute(f"import into tb values(1564641710000, 10000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(1)

        tdLog.info(f"================= step2")
        tdSql.execute(f"insert into tb values(1564641708000, 8000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(2)

        tdLog.info(f"================= step3")
        tdSql.execute(f"insert into tb values(1564641720000, 20000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(3)

        tdLog.info(f"================= step4")
        tdSql.execute(f"import into tb values(1564641708000, 8000)")
        tdSql.execute(f"import into tb values(1564641715000, 15000)")
        tdSql.execute(f"import into tb values(1564641730000, 30000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(5)

        tdLog.info(f"================= step5")
        tdSql.execute(f"insert into tb values(1564641708000, 8000)")
        tdSql.execute(f"insert into tb values(1564641714000, 14000)")
        tdSql.execute(f"insert into tb values(1564641725000, 25000)")
        tdSql.execute(f"insert into tb values(1564641740000, 40000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(8)

        tdLog.info(f"================= step6")
        tdSql.execute(f"import into tb values(1564641707000, 7000)")
        tdSql.execute(f"import into tb values(1564641712000, 12000)")
        tdSql.execute(f"import into tb values(1564641723000, 23000)")
        tdSql.execute(f"import into tb values(1564641734000, 34000)")
        tdSql.execute(f"import into tb values(1564641750000, 50000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(13)

        tdLog.info(f"================= step7")
        tdSql.execute(f"import into tb values(1564641707001, 7001)")
        tdSql.execute(f"import into tb values(1564641712001, 12001)")
        tdSql.execute(f"import into tb values(1564641723001, 23001)")
        tdSql.execute(f"import into tb values(1564641734001, 34001)")
        tdSql.execute(f"import into tb values(1564641750001, 50001)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(18)

        tdLog.info(f"================= step8")
        tdSql.execute(f"insert into tb values(1564641708002, 8002)")
        tdSql.execute(f"insert into tb values(1564641714002, 14002)")
        tdSql.execute(f"insert into tb values(1564641725002, 25002)")
        tdSql.execute(f"insert into tb values(1564641900000, 200000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(22)

        tdLog.info(f"================= step9 only insert last one")
        tdSql.execute(
            f"import into tb values(1564641705000, 5000)(1564641718000, 18000)(1564642400000, 700000)"
        )
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(25)

        tdLog.info(f"================= step10")
        tdSql.execute(
            f"import into tb values(1564641705000, 5000)(1564641718000, 18000)(1564642400000, 70000)"
        )
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(25)

        tdLog.info(f"================= step11")
        tdSql.execute(f"import into tb values(1564642400000, 700000)")
        tdSql.query(f"select * from tb;")
        tdSql.checkRows(25)

        tdLog.info(f"================= step12")
        tdSql.execute(
            f"import into tb values(1564641709527, 9527)(1564641709527, 9528)"
        )
        tdSql.query(f"select * from tb;")
        tdLog.info(f"rows=> {tdSql.getRows()})")
        tdSql.checkRows(26)

        tdLog.info(f"================= step13")
        tdSql.execute(
            f"import into tb values(1564641709898, 9898)(1564641709897, 9897)"
        )
        tdSql.query(f"select * from tb;")
        tdLog.info(f"rows=> {tdSql.getRows()})")
        tdSql.checkRows(28)
