from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSTableAlter4:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_alter_4(self):
        """alter super table 4

        1. drop column
        2. insert data
        3. project query
        4. loop for 7 times

        Catalog:
            - SuperTables:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/stable/column_drop.sim

        """

        tdLog.info(f"========== prepare stb and ctb")
        tdSql.prepare("db", drop=True, vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4), c3 int, c4 bigint, c5 int, c6 int) tags(t1 int, t2 float, t3 binary(16)) comment "abd"'
        )
        tdSql.execute(f'create table db.ctb using db.stb tags(101, 102, "103")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "2", 3, 4, 5, 6)')

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "stb")

        tdSql.checkData(0, 1, "db")

        tdSql.checkData(0, 3, 7)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 6, "abd")

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "ctb")

        tdSql.checkData(0, 1, "db")

        tdSql.checkData(0, 3, 7)

        tdSql.checkData(0, 4, "stb")

        tdSql.checkData(0, 6, 2)

        tdSql.checkData(0, 9, "CHILD_TABLE")

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 2)

        tdSql.checkData(0, 3, 3)

        tdSql.checkData(0, 4, 4)

        tdSql.checkData(0, 5, 5)

        tdSql.checkData(0, 6, 6)

        tdSql.checkData(0, 7, 101)

        tdSql.error(f"alter table db.stb drop column ts")
        tdSql.error(f"alter table db.stb drop column t1")
        tdSql.error(f"alter table db.stb drop column t2")
        tdSql.error(f"alter table db.stb drop column t3")
        tdSql.error(f"alter table db.stb drop column c9")

        tdLog.info(f"========== step1 drop column c6")
        tdSql.execute(f"alter table db.stb drop column c6")
        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkData(0, 3, 6)

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkData(0, 3, 6)

        tdSql.query(f"select * from db.stb")
        tdSql.query(f"select * from db.stb")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 2)

        tdSql.checkData(0, 3, 3)

        tdSql.checkData(0, 4, 4)

        tdSql.checkData(0, 5, 5)

        tdSql.checkData(0, 6, 101)

        tdSql.execute(f"insert into db.ctb values(now+1s, 1, 2, 3, 4, 5)")
        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(2)

        tdLog.info(f"========== step2 drop column c5")
        tdSql.execute(f"alter table db.stb drop column c5")
        tdSql.error(f"insert into db.ctb values(now+2s, 1, 2, 3, 4, 5)")
        tdSql.execute(f"insert into db.ctb values(now+2s, 1, 2, 3, 4)")
        tdSql.execute(f"insert into db.ctb values(now+3s, 1, 2, 3, 4)")
        tdSql.error(f"insert into db.ctb values(now+2s, 1, 2, 3, 4, 5)")

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(4)

        tdLog.info(f"========== step3 drop column c4")
        tdSql.execute(f"alter table db.stb drop column c4")
        tdSql.query(f"select * from db.stb")
        tdSql.error(f"insert into db.ctb values(now+2s, 1, 2, 3, 4, 5)")
        tdSql.error(f"insert into db.ctb values(now+2s, 1, 2, 3, 4)")
        tdSql.execute(f"insert into db.ctb values(now+3s, 1, 2, 3)")

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(5)

        tdLog.info(f"========== step4 add column c4")
        tdSql.execute(f"alter table db.stb add column c4 binary(13)")
        tdSql.execute(f"insert into db.ctb values(now+4s, 1, 2, 3, '4')")
        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(6)

        tdSql.checkData(1, 4, None)

        tdSql.checkData(2, 4, None)

        tdSql.checkData(3, 4, None)

        tdSql.checkData(5, 4, 4)

        tdLog.info(f"========== step5 describe")
        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(1, 0, "c1")

        tdSql.checkData(2, 0, "c2")

        tdSql.checkData(3, 0, "c3")

        tdSql.checkData(4, 0, "c4")

        tdSql.checkData(4, 1, "VARCHAR")

        tdSql.checkData(4, 2, 13)

        tdSql.checkData(5, 0, "t1")

        tdSql.checkData(6, 0, "t2")

        tdSql.checkData(7, 0, "t3")
