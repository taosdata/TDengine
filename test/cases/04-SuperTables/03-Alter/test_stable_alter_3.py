from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSTableAlter3:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_alter_3(self):
        """alter super table 3

        1. add column
        2. insert data
        3. project query
        4. loop for 7 times

        Catalog:
            - SuperTables:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/stable/column_add.sim

        """

        tdLog.info(f"========== prepare stb and ctb")
        tdSql.prepare("db", drop=True, vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 float, t3 binary(16)) comment "abd"'
        )
        tdSql.execute(f'create table db.ctb using db.stb tags(101, 102, "103")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "2")')

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "stb")

        tdSql.checkData(0, 1, "db")

        tdSql.checkData(0, 3, 3)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 6, "abd")

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "ctb")

        tdSql.checkData(0, 1, "db")

        tdSql.checkData(0, 3, 3)

        tdSql.checkData(0, 4, "stb")

        tdSql.checkData(0, 6, 2)

        tdSql.checkData(0, 9, "CHILD_TABLE")

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 2)

        tdSql.checkData(0, 3, 101)

        tdSql.error(f"alter table db.stb add column ts int")
        tdSql.error(f"alter table db.stb add column t1 int")
        tdSql.error(f"alter table db.stb add column t2 int")
        tdSql.error(f"alter table db.stb add column t3 int")
        tdSql.error(f"alter table db.stb add column c1 int")

        tdLog.info(f"========== step1 add column c3")
        tdSql.execute(f"alter table db.stb add column c3 int")
        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkData(0, 3, 4)

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkData(0, 3, 4)

        tdSql.query(f"select * from db.stb")
        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 2)

        tdSql.checkData(0, 3, None)

        tdSql.checkData(0, 4, 101)

        tdSql.execute(f"insert into db.ctb values(now+1s, 1, 2, 3)")
        tdSql.query(f"select * from db.stb")

        tdSql.checkRows(2)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 2)

        tdSql.checkData(0, 3, None)

        tdSql.checkData(0, 4, 101)

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(1, 2, 2)

        tdSql.checkData(1, 3, 3)

        tdSql.checkData(1, 4, 101)

        tdLog.info(f"========== step2 add column c4")
        tdSql.execute(f"alter table db.stb add column c4 bigint")
        tdSql.query(f"select * from db.stb")
        tdSql.execute(f"insert into db.ctb values(now+2s, 1, 2, 3, 4)")
        tdSql.query(f"select * from db.stb")

        tdSql.checkRows(3)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 2)

        tdSql.checkData(0, 3, None)

        tdSql.checkData(0, 4, None)

        tdSql.checkData(0, 5, 101)

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(1, 2, 2)

        tdSql.checkData(1, 3, 3)

        tdSql.checkData(1, 4, None)

        tdSql.checkData(1, 5, 101)

        tdSql.checkData(2, 1, 1)

        tdSql.checkData(2, 2, 2)

        tdSql.checkData(2, 3, 3)

        tdSql.checkData(2, 4, 4)

        tdSql.checkData(2, 5, 101)

        tdLog.info(f"========== step3 add column c5")
        tdSql.execute(f"alter table db.stb add column c5 int")
        tdSql.execute(f"insert into db.ctb values(now+3s, 1, 2, 3, 4, 5)")
        tdSql.query(f"select * from db.stb")

        tdSql.checkRows(4)

        tdSql.checkData(2, 1, 1)

        tdSql.checkData(2, 2, 2)

        tdSql.checkData(2, 3, 3)

        tdSql.checkData(2, 4, 4)

        tdSql.checkData(2, 5, None)

        tdSql.checkData(2, 6, 101)

        tdSql.checkData(3, 1, 1)

        tdSql.checkData(3, 2, 2)

        tdSql.checkData(3, 3, 3)

        tdSql.checkData(3, 4, 4)

        tdSql.checkData(3, 5, 5)

        tdSql.checkData(3, 6, 101)

        tdLog.info(f"========== step4 add column c6")
        tdSql.execute(f"alter table db.stb add column c6 int")
        tdSql.execute(f"insert into db.ctb values(now+4s, 1, 2, 3, 4, 5, 6)")
        tdSql.query(f"select * from db.stb")

        tdSql.checkRows(5)

        tdSql.checkData(3, 1, 1)

        tdSql.checkData(3, 2, 2)

        tdSql.checkData(3, 3, 3)

        tdSql.checkData(3, 4, 4)

        tdSql.checkData(3, 5, 5)

        tdSql.checkData(3, 6, None)

        tdSql.checkData(3, 7, 101)

        tdSql.checkData(4, 1, 1)

        tdSql.checkData(4, 2, 2)

        tdSql.checkData(4, 3, 3)

        tdSql.checkData(4, 4, 4)

        tdSql.checkData(4, 5, 5)

        tdSql.checkData(4, 6, 6)

        tdSql.checkData(4, 7, 101)

        tdLog.info(f"========== step5 describe")
        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(10)
