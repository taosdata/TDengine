from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableAlter5:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_alter_5(self):
        """Alter: repeatedly drop

        1. Drop column
        2. Insert data
        3. Project query
        4. Loop for 7 times
        5. Kill then restart

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/stable/alter_insert2.sim

        """

        tdLog.info(f"======== step1")
        tdSql.prepare("d4", drop=True)
        tdSql.execute(f"use d4")
        tdSql.execute(
            f"create table tb (ts timestamp, a int, b smallint, c tinyint, d int, e bigint, f float, g double, h binary(10))"
        )
        tdSql.execute(f"insert into tb values(now-28d, 1, 2, 3, 4, 5, 6, 7, 8)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(1)
        tdSql.checkCols(9)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 2)

        tdSql.checkData(0, 3, 3)

        tdSql.checkData(0, 4, 4)

        tdSql.checkData(0, 5, 5)

        tdSql.checkData(0, 6, 6.00000)

        tdSql.checkData(0, 7, 7.000000000)

        tdSql.checkData(0, 8, 8)

        tdLog.info(f"======== step2")
        tdSql.error(f"alter table tb add column b smallint")
        tdSql.error(f"alter table tb add column b int")
        tdSql.execute(f"alter table tb drop column b")
        tdSql.execute(f"insert into tb values(now-25d, 2, 3, 4, 5, 6, 7, 8)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(2)
        tdSql.checkCols(8)

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(0, 2, 3)

        tdSql.checkData(0, 3, 4)

        tdSql.checkData(0, 4, 5)

        tdSql.checkData(0, 5, 6.00000)

        tdSql.checkData(0, 6, 7.000000000)

        tdSql.checkData(0, 7, 8)

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(1, 2, 3)

        tdSql.checkData(1, 3, 4)

        tdSql.checkData(1, 4, 5)

        tdSql.checkData(1, 5, 6.00000)

        tdSql.checkData(1, 6, 7.000000000)

        tdSql.checkData(1, 7, 8)

        tdLog.info(f"======== step3")
        tdSql.execute(f"alter table tb drop column c")
        tdSql.execute(f"insert into tb values(now-22d, 3, 4, 5, 6, 7, 8)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(3)
        tdSql.checkCols(7)

        tdSql.checkData(0, 1, 3)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 6.00000)

        tdSql.checkData(0, 5, 7.000000000)

        tdSql.checkData(0, 6, 8)

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(1, 2, 4)

        tdSql.checkData(1, 3, 5)

        tdSql.checkData(1, 4, 6.00000)

        tdSql.checkData(1, 5, 7.000000000)

        tdSql.checkData(1, 6, 8)

        tdSql.checkData(2, 1, 1)

        tdSql.checkData(2, 2, 4)

        tdSql.checkData(2, 3, 5)

        tdSql.checkData(2, 4, 6.00000)

        tdSql.checkData(2, 5, 7.000000000)

        tdSql.checkData(2, 6, 8)

        tdLog.info(f"======== step4")
        tdSql.execute(f"alter table tb drop column d")
        tdSql.execute(f"alter table tb drop column e")
        tdSql.execute(f"insert into tb values(now-19d, -19, 6, 3, 0)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(4)
        tdSql.checkCols(5)

        tdSql.checkData(0, 1, -19)

        tdSql.checkData(0, 2, 6.00000)

        tdSql.checkData(0, 3, 3.000000000)

        tdSql.checkData(0, 4, 0)

        tdSql.checkData(1, 1, 3)

        tdSql.checkData(1, 2, 6.00000)

        tdSql.checkData(1, 3, 7.000000000)

        tdSql.checkData(1, 4, 8)

        tdSql.checkData(2, 1, 2)

        tdSql.checkData(2, 2, 6.00000)

        tdSql.checkData(2, 3, 7.000000000)

        tdSql.checkData(2, 4, 8)

        tdSql.checkData(3, 1, 1)

        tdSql.checkData(3, 2, 6.00000)

        tdSql.checkData(3, 3, 7.000000000)

        tdSql.checkData(3, 4, 8)

        tdLog.info(f"======== step5")
        tdSql.execute(f"alter table tb drop column g")
        tdSql.execute(f"insert into tb values(now-16d, -16, 9, 5)")
        tdSql.query(f"select count(f) from tb")
        tdSql.checkData(0, 0, 5)

        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(5)
        tdSql.checkCols(4)

        tdSql.checkData(0, 1, -16)

        tdSql.checkData(0, 2, 9.00000)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(1, 1, -19)

        tdSql.checkData(1, 2, 6.00000)

        tdSql.checkData(1, 3, 0)

        tdSql.checkData(2, 1, 3)

        tdSql.checkData(2, 2, 6.00000)

        tdSql.checkData(2, 3, 8)

        tdSql.checkData(3, 1, 2)

        tdSql.checkData(3, 2, 6.00000)

        tdSql.checkData(3, 3, 8)

        tdSql.checkData(4, 1, 1)

        tdSql.checkData(4, 2, 6.00000)

        tdSql.checkData(4, 3, 8)

        tdLog.info(f"======== step6")
        tdSql.execute(f"alter table tb drop column f")
        tdSql.execute(f"insert into tb values(now-13d, -13, 7)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(6)
        tdSql.checkCols(3)

        tdSql.checkData(0, 1, -13)

        tdSql.checkData(1, 1, -16)

        tdSql.checkData(1, 2, 5)

        tdSql.checkData(2, 1, -19)

        tdSql.checkData(2, 2, 0)

        tdSql.checkData(3, 1, 3)

        tdSql.checkData(3, 2, 8)

        tdSql.checkData(4, 1, 2)

        tdSql.checkData(4, 2, 8)

        tdSql.checkData(5, 1, 1)

        tdSql.checkData(5, 2, 8)

        tdLog.info(f"======== step7")
        tdSql.execute(f"alter table tb drop column h")
        tdSql.execute(f"insert into tb values(now-10d, -10)")
        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(7)
        tdSql.checkCols(2)

        tdLog.info(f"tdSql.getData(0,1) = {tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, -10)

        tdSql.checkData(1, 1, -13)

        tdSql.checkData(2, 1, -16)

        tdSql.checkData(3, 1, -19)

        tdSql.checkData(4, 1, 3)

        tdSql.checkData(5, 1, 2)

        tdSql.checkData(6, 1, 1)

        tdSql.error(f"alter table tb drop column a")

        tdLog.info(f"======== step9")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"select * from tb order by ts desc")
        tdSql.checkRows(7)
        tdSql.checkCols(2)

        tdSql.checkData(0, 1, -10)

        tdSql.checkData(1, 1, -13)

        tdSql.checkData(2, 1, -16)

        tdSql.checkData(3, 1, -19)

        tdSql.checkData(4, 1, 3)

        tdSql.checkData(5, 1, 2)

        tdSql.checkData(6, 1, 1)
