from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStableAlter1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_alter_1(self):
        """alter super table 1

        1. add column
        2. drop column
        3. modify column
        4. rename column
        5. add tag
        6. drop tag
        7. modify tag
        8. rename tag

        Catalog:
            - SuperTables:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/stable/alter_comment.sim

        """

        tdLog.info(f"========== create stable")
        tdSql.prepare("db", drop=True)
        tdSql.execute(f"use db")
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 float, t3 binary(16)) comment "abd"'
        )

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 3, 3)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 6, "abd")

        tdLog.info(f"========== add column")
        tdSql.error(f"alter table db.stb add column ts int")
        tdSql.error(f"alter table db.stb add column c1 int")
        tdSql.error(f"alter table db.stb add column c2 int")
        tdSql.error(f"alter table db.stb add column t1 int")
        tdSql.error(f"alter table db.stb add column t2 int")
        tdSql.error(f"alter table db.stb add column t3 int")
        tdSql.execute(f"alter table db.stb add column c3 int")
        tdSql.execute(f"alter table db.stb add column c4 bigint")
        tdSql.execute(f"alter table db.stb add column c5 binary(12)")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 3, 6)

        tdSql.checkData(0, 4, 3)

        tdLog.info(f"========== drop column")
        tdSql.error(f"alter table db.stb drop column ts")
        tdSql.error(f"alter table db.stb drop column c6")
        tdSql.error(f"alter table db.stb drop column c7")
        tdSql.error(f"alter table db.stb drop column t1")
        tdSql.error(f"alter table db.stb drop column t2")
        tdSql.error(f"alter table db.stb drop column t3")
        tdSql.execute(f"alter table db.stb drop column c1")
        tdSql.execute(f"alter table db.stb drop column c4")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkData(0, 4, 3)

        tdLog.info(f"========== update column")
        tdSql.error(f"alter table db.stb MODIFY column ts binary(20)")
        tdSql.error(f"alter table db.stb MODIFY column c6 binary(20)")
        tdSql.error(f"alter table db.stb MODIFY column t1 binary(20)")
        tdSql.error(f"alter table db.stb MODIFY column t3 binary(20)")
        tdSql.error(f"alter table db.stb MODIFY column c2 binary(3)")
        tdSql.execute(f"alter table db.stb MODIFY column c2 binary(32)")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkData(0, 4, 3)

        tdLog.info(f"========== rename column")
        tdSql.error(f"alter table db.stb rename column ts tx")
        tdSql.error(f"alter table db.stb rename column c2 cx")

        tdLog.info(f"========== add tag")
        tdSql.error(f"alter table db.stb add tag ts int")
        tdSql.error(f"alter table db.stb add tag c2 int")
        tdSql.error(f"alter table db.stb add tag t1 int")
        tdSql.error(f"alter table db.stb add tag t2 int")
        tdSql.error(f"alter table db.stb add tag t3 int")
        tdSql.execute(f"alter table db.stb add tag t4 bigint")
        tdSql.execute(f"alter table db.stb add tag c1 int")
        tdSql.execute(f"alter table db.stb add tag t5 binary(12)")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)

        # ts c2 c3 c5
        tdSql.checkData(0, 3, 4)

        # t1 t2 t3 t4 c1 t5
        tdSql.checkData(0, 4, 6)

        tdLog.info(f"========== drop tag")
        tdSql.error(f"alter table db.stb drop tag ts")
        tdSql.error(f"alter table db.stb drop tag c2")
        tdSql.error(f"alter table db.stb drop tag c3")
        tdSql.error(f"alter table db.stb drop tag tx")
        tdSql.execute(f"alter table db.stb drop tag c1")
        tdSql.execute(f"alter table db.stb drop tag t5")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)

        # ts c2 c3 c5
        tdSql.checkData(0, 3, 4)

        # t1 t2 t3 t4
        tdSql.checkData(0, 4, 4)

        tdLog.info(f"========== update tag")
        tdSql.error(f"alter table db.stb MODIFY tag ts binary(20)")
        tdSql.error(f"alter table db.stb MODIFY tag c2 binary(20)")
        tdSql.error(f"alter table db.stb MODIFY tag t1 binary(20)")
        tdSql.error(f"alter table db.stb MODIFY tag tx binary(20)")
        tdSql.execute(f"alter table db.stb MODIFY tag t3 binary(32)")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkData(0, 4, 4)

        tdLog.info(f"========== rename tag")
        # t1 t2 t3 t4

        tdSql.error(f"alter table db.stb rename tag ts tx")
        tdSql.error(f"alter table db.stb rename tag c2 cx")
        tdSql.execute(f"alter table db.stb rename tag t1 tx")

        tdLog.info(f"========== alter common")
        tdSql.execute(f"alter table db.stb comment 'abcde' ;")
        tdSql.error(f"alter table db.stb ttl 10 ;")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db';"
        )
        tdSql.checkData(0, 6, "abcde")
