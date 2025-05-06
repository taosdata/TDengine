from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSTableAlter5:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_alter_5(self):
        """alter super table 5

        1. modify column
        2. insert data
        3. project query
        4. kill then restart
        5. project query

        Catalog:
            - SuperTables:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/stable/column_modify.sim

        """

        tdLog.info(f"========== prepare stb and ctb")
        tdSql.prepare("db", drop=True, vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 float, t3 binary(16)) comment "abd"'
        )
        tdSql.execute(f'create table db.ctb using db.stb tags(101, 102, "103")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "1234")')

        tdSql.error(f"alter table db.stb MODIFY column c2 binary(3)")
        tdSql.error(f"alter table db.stb MODIFY column c2 int")
        tdSql.error(f"alter table db.stb MODIFY column c1 int")
        tdSql.error(f"alter table db.stb MODIFY column ts int")
        tdSql.error(f'insert into db.ctb values(now, 1, "12345")')

        tdLog.info(f"========== step1 modify column")
        tdSql.execute(f"alter table db.stb MODIFY column c2 binary(5)")
        tdSql.execute(f'insert into db.ctb values(now, 1, "12345")')

        tdSql.query(f"select * from db.stb")

        tdSql.checkRows(2)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1234)

        tdSql.checkData(0, 3, 101)

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(1, 2, 12345)

        tdSql.checkData(1, 3, 101)

        tdLog.info(f"========== step2 describe")
        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(6)

        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(1, 0, "c1")

        tdSql.checkData(2, 0, "c2")

        tdSql.checkData(2, 1, "VARCHAR")

        tdSql.checkData(2, 2, 5)

        tdSql.checkData(3, 0, "t1")

        tdSql.checkData(4, 0, "t2")

        tdSql.checkData(5, 0, "t3")

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"select * from db.stb")

        tdSql.checkRows(2)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1234)

        tdSql.checkData(0, 3, 101)

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(1, 2, 12345)

        tdSql.checkData(1, 3, 101)
