from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestStableAlterTag:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_alter_tag(self):
        """Alter tags

        1. Create a super table
        2. Create a child table and insert data
        3. Add a tag column and verify that it takes effect
        4. Query using the newly added tag value
        5. Repeat the same operations for Drop, Modify, and Rename Tag
        6. Restart and verify that all modifications remain effective

        Catalog:
            - SuperTable:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/stable/tag_add.sim
            - 2025-8-11 Simon Guan Migrated from tsim/stable/tag_drop.sim
            - 2025-8-11 Simon Guan Migrated from tsim/stable/tag_modify.sim
            - 2025-8-11 Simon Guan Migrated from tsim/stable/tag_rename.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/add.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/change.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/delete.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/commit.sim


        """

        self.AddTag()
        tdStream.dropAllStreamsAndDbs()
        self.DropTag()
        tdStream.dropAllStreamsAndDbs()
        self.ModifyTag()
        tdStream.dropAllStreamsAndDbs()
        self.RenameTag()
        tdStream.dropAllStreamsAndDbs()
        self.TagAdd()
        tdStream.dropAllStreamsAndDbs()
        self.TagChange()
        tdStream.dropAllStreamsAndDbs()
        self.TagDelete()
        tdStream.dropAllStreamsAndDbs()
        self.TagCommit()
        tdStream.dropAllStreamsAndDbs()

    def AddTag(self):
        tdLog.info(f"========== tsim/stable/tag_add.sim")
        tdSql.prepare(dbname="db", vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 binary(16)) comment "abd"'
        )
        tdSql.execute(f'create table db.ctb using db.stb tags(101, "102")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "2")')

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb")
        tdSql.checkData(0, 1, "db")
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, 2)
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
        tdSql.checkData(0, 4, 102)

        tdSql.error(f"alter table db.stb add tag ts int")
        tdSql.error(f"alter table db.stb add tag t1 int")
        tdSql.error(f"alter table db.stb add tag t2 int")
        tdSql.error(f"alter table db.stb add tag c1 int")
        tdSql.error(f"alter table db.stb add tag c2 int")

        tdLog.info(f"========== step1 add tag t3")
        tdSql.execute(f"alter table db.stb add tag t3 int")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkData(0, 3, 3)

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkData(0, 3, 3)

        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(6)
        tdSql.checkData(5, 0, "t3")
        tdSql.checkData(5, 1, "INT")
        tdSql.checkData(5, 2, 4)

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 102)
        tdSql.checkData(0, 5, None)

        tdLog.info(f"========== step2 add tag t4")
        tdSql.execute(f"alter table db.stb add tag t4 bigint")
        tdSql.query(f"select * from db.stb")
        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 102)
        tdSql.checkData(0, 5, None)
        tdSql.checkData(0, 6, None)

        tdSql.error(f'create table db.ctb2 using db.stb tags(101, "102")')
        tdSql.execute(f'create table db.ctb2 using db.stb tags(101, "102", 103, 104)')
        tdSql.execute(f'insert into db.ctb2 values(now, 1, "2")')

        tdSql.query(f"select * from db.stb where tbname = 'ctb2';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 102)
        tdSql.checkData(0, 5, 103)
        tdSql.checkData(0, 6, 104)

        tdLog.info(f"========== step3 describe")
        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(7)

    def DropTag(self):
        tdLog.info(f"========== tsim/stable/tag_drop.sim")
        tdSql.prepare(dbname="db", vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 binary(16)) comment "abd"'
        )
        tdSql.execute(f'create table db.ctb using db.stb tags(101, "102")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "2")')

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb")
        tdSql.checkData(0, 1, "db")
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, 2)
        tdSql.checkData(0, 6, "abd")

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "ctb")
        tdSql.checkData(0, 1, "db")
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, "stb")
        tdSql.checkData(0, 6, 3)
        tdSql.checkData(0, 9, "CHILD_TABLE")

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 102)

        tdSql.error(f"alter table db.stb drop tag ts int")
        tdSql.error(f"alter table db.stb drop tag t3 int")
        tdSql.error(f"alter table db.stb drop tag t4 int")
        tdSql.error(f"alter table db.stb drop tag c1 int")
        tdSql.error(f"alter table db.stb drop tag c2 int")

        tdLog.info(f"========== step1 drop tag t2")
        tdSql.execute(f"alter table db.stb drop tag t2")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkData(0, 4, 1)

        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(4)

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)
        tdSql.checkCols(4)

        tdLog.info(f"========== step2 add tag t3")
        tdSql.execute(f"alter table db.stb add tag t3 int")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkData(0, 4, 2)

        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(5)
        tdSql.checkData(4, 0, "t3")
        tdSql.checkData(4, 1, "INT")
        tdSql.checkData(4, 2, 4)

        tdSql.query(f"select * from db.stb")

        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, None)

        tdLog.info(f"========== step3 add tag t4")
        tdSql.execute(f"alter table db.stb add tag t4 bigint")
        tdSql.query(f"select * from db.stb")
        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, None)
        tdSql.checkData(0, 5, None)
        tdSql.checkCols(6)

        tdSql.error(f'create table db.ctb2 using db.stb tags(101, "102")')
        tdSql.execute(f"create table db.ctb2 using db.stb tags(201, 202, 203)")
        tdSql.execute(f'insert into db.ctb2 values(now, 1, "2")')

        tdSql.query(f"select * from db.stb where tbname = 'ctb2';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 201)
        tdSql.checkData(0, 4, 202)
        tdSql.checkData(0, 5, 203)

        tdLog.info(f"========== step4 describe")
        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(6)

        tdLog.info(f"========== step5 add tag2")
        tdSql.execute(f"alter table db.stb add tag t2 bigint")
        tdSql.query(f"select * from db.stb where tbname = 'ctb2';")
        tdSql.query(f"select * from db.stb where tbname = 'ctb2';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 201)
        tdSql.checkData(0, 4, 202)
        tdSql.checkData(0, 5, 203)
        tdSql.checkData(0, 6, None)

        tdSql.error(f'create table db.ctb2 using db.stb tags(101, "102")')
        tdSql.error(f"create table db.ctb2 using db.stb tags(201, 202, 203)")
        tdSql.execute(f"create table db.ctb3 using db.stb tags(301, 302, 303, 304)")
        tdSql.execute(f'insert into db.ctb3 values(now, 1, "2")')

        tdSql.query(f"select * from db.stb where tbname = 'ctb3';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 301)
        tdSql.checkData(0, 4, 302)
        tdSql.checkData(0, 5, 303)
        tdSql.checkData(0, 6, 304)

        tdLog.info(f"========== step6 describe")
        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(7)
        tdSql.checkData(3, 0, "t1")
        tdSql.checkData(4, 0, "t3")
        tdSql.checkData(5, 0, "t4")
        tdSql.checkData(6, 0, "t2")
        tdSql.checkData(6, 1, "BIGINT")

        tdLog.info(f"========== step7 drop tag t1")
        tdSql.execute(f"alter table db.stb drop tag t1")

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkData(0, 4, 3)

        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(6)

        tdSql.query(f"select * from db.stb where tbname = 'ctb3';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 302)
        tdSql.checkData(0, 4, 303)
        tdSql.checkData(0, 5, 304)

    def ModifyTag(self):
        tdLog.info(f"========== tsim/stable/tag_modify.sim")
        tdSql.prepare(dbname="db", vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 binary(4)) comment "abd"'
        )

        tdSql.error(f"alter table db.stb MODIFY tag c2 binary(3)")
        tdSql.error(f"alter table db.stb MODIFY tag c2 int")
        tdSql.error(f"alter table db.stb MODIFY tag c1 int")
        tdSql.error(f"alter table db.stb MODIFY tag ts int")
        tdSql.error(f"alter table db.stb MODIFY tag t2 binary(3)")
        tdSql.error(f"alter table db.stb MODIFY tag t2 int")
        tdSql.error(f"alter table db.stb MODIFY tag t1 int")
        tdSql.execute(f'create table db.ctb using db.stb tags(101, "123")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "1234")')

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1234)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 123)

        tdLog.info(f"========== step1 modify tag")
        tdSql.execute(f"alter table db.stb MODIFY tag t2 binary(5)")
        tdSql.query(f"select * from db.stb")

        tdSql.execute(f'create table db.ctb2 using db.stb tags(101, "12345")')
        tdSql.execute(f'insert into db.ctb2 values(now, 1, "1234")')

        tdSql.query(f"select * from db.stb where tbname = 'ctb2';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1234)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 12345)

        tdLog.info(f"========== step2 describe")
        tdSql.query(f"describe db.ctb2")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "c1")
        tdSql.checkData(2, 0, "c2")
        tdSql.checkData(3, 0, "t1")
        tdSql.checkData(4, 0, "t2")
        tdSql.checkData(4, 1, "VARCHAR")
        tdSql.checkData(4, 2, 5)

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"describe db.ctb2")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "c1")
        tdSql.checkData(2, 0, "c2")
        tdSql.checkData(3, 0, "t1")
        tdSql.checkData(4, 0, "t2")
        tdSql.checkData(4, 1, "VARCHAR")
        tdSql.checkData(4, 2, 5)

    def RenameTag(self):
        tdLog.info(f"========== tsim/stable/tag_rename.sim")
        tdSql.prepare(dbname="db", vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 binary(4)) comment "abd"'
        )

        tdSql.error(f"alter table db.stb rename tag c2 c3")
        tdSql.error(f"alter table db.stb rename tag c2 c3")
        tdSql.error(f"alter table db.stb rename tag c1 c3")
        tdSql.error(f"alter table db.stb rename tag ts c3")
        tdSql.error(f"alter table db.stb rename tag t2 t1")
        tdSql.error(f"alter table db.stb rename tag t2 t2")
        tdSql.error(f"alter table db.stb rename tag t1 t2")
        tdSql.execute(f'create table db.ctb using db.stb tags(101, "123")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "1234")')

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1234)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 123)

        tdLog.info(f"========== step1 rename tag")
        tdSql.execute(f"alter table db.stb rename tag t1 t3")
        tdSql.query(f"select * from db.stb")
        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1234)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(0, 4, 123)

        tdLog.info(f"========== step2 describe")
        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "c1")
        tdSql.checkData(2, 0, "c2")
        tdSql.checkData(3, 0, "t3")
        tdSql.checkData(4, 0, "t2")
        tdSql.checkData(4, 1, "VARCHAR")
        tdSql.checkData(4, 2, 4)

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "c1")
        tdSql.checkData(2, 0, "c2")
        tdSql.checkData(3, 0, "t3")
        tdSql.checkData(4, 0, "t2")
        tdSql.checkData(4, 1, "VARCHAR")
        tdSql.checkData(4, 2, 4)

    def TagAdd(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_ad_db"
        tbPrefix = "ta_ad_tb"
        mtPrefix = "ta_ad_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        tdLog.info(f"=============== step2")
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} add tag tgcol4 int")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4 =4")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step3")
        i = 3
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} add tag tgcol4 tinyint")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step4")
        i = 4
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.query(f"describe {tb}")
        tdLog.info(f"sql describe {tb}")
        tdSql.checkData(2, 1, "BIGINT")

        tdSql.checkData(3, 1, "FLOAT")

        tdSql.checkData(2, 3, "TAG")

        tdSql.checkData(3, 3, "TAG")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} add tag tgcol4 float")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step5")
        i = 5
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, '2' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = '2'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} add tag tgcol4 smallint")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.error(f"select * from {mt} where tgcol3 = '1'")

        tdLog.info(f"=============== step6")
        i = 6
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} add tag tgcol5 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol6 binary(10)")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=false")
        tdSql.execute(f"alter table {tb} set tag tgcol5='5'")
        tdSql.execute(f"alter table {tb} set tag tgcol6='6'")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol5 = '5'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 6)

        tdSql.query(f"select * from {mt} where tgcol6 = '6'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 6)

        tdSql.query(f"select * from {mt} where tgcol4 = 1")
        tdSql.checkRows(0)

        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step7")
        i = 7
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint, tgcol3 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, '3' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol3 = '3'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} add tag tgcol5 bigint")
        tdSql.execute(f"alter table {mt} add tag tgcol6 tinyint")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"alter table {tb} set tag tgcol5=5")
        tdSql.execute(f"alter table {tb} set tag tgcol6=6")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol6 = 6")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 6)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step8")
        i = 8
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float, tgcol3 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, '3' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol3 = '3'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} add tag tgcol5 binary(17)")
        tdSql.execute(f"alter table {mt} add tag tgcol6 bool")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"alter table {tb} set tag tgcol5='5'")
        tdSql.execute(f"alter table {tb} set tag tgcol6='1'")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol5 = '5'")
        tdLog.info(f"select * from {mt} where tgcol5 = 5")
        tdLog.info(
            f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 1)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step9")
        i = 9
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10), tgcol3 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, '2', '3' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = '2'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} add tag tgcol5 bool")
        tdSql.execute(f"alter table {mt} add tag tgcol6 float")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"alter table {tb} set tag tgcol5=1")
        tdSql.execute(f"alter table {tb} set tag tgcol6=6")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol5 = 1")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(0, 4, 6)

        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step10")
        i = 10
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 binary(10), tgcol3 binary(10), tgcol4 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( '1', '2', '3', '4' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol4 = '4'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.error(f"alter table {mt} rename tag tgcol1 tgcol4")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} add tag tgcol4 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol5 bool")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4='4'")
        tdSql.execute(f"alter table {tb} set tag tgcol5=false")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = '4'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkData(0, 4, 0)

        tdSql.checkCols(5)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step11")
        i = 11
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 smallint, tgcol4 float, tgcol5 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3, 4, '5' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 5)

        tdSql.error(f"alter table {mt} rename tag tgcol1 tgcol4")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol5")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} add tag tgcol4 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol5 int")
        tdSql.execute(f"alter table {mt} add tag tgcol6 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol7 bigint")
        tdSql.execute(f"alter table {mt} add tag tgcol8 smallint")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4='4'")
        tdSql.execute(f"alter table {tb} set tag tgcol5=5")
        tdSql.execute(f"alter table {tb} set tag tgcol6='6'")
        tdSql.execute(f"alter table {tb} set tag tgcol7=7")
        tdSql.execute(f"alter table {tb} set tag tgcol8=8")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol5 =5")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkData(0, 4, 5)

        tdSql.checkData(0, 5, 6)

        tdSql.checkData(0, 6, 7)

        tdSql.checkData(0, 7, 8)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol9 = 1")

        tdLog.info(f"=============== step12")
        i = 12
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 smallint, tgcol3 float, tgcol4 double, tgcol5 binary(10), tgcol6 binary(20))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3, 4, '5', '6' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 5)

        tdSql.checkData(0, 7, 6)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol5")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} add tag tgcol2 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol3 int")
        tdSql.execute(f"alter table {mt} add tag tgcol4 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol5 bigint")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol1=false")
        tdSql.execute(f"alter table {tb} set tag tgcol2='5'")
        tdSql.execute(f"alter table {tb} set tag tgcol3=4")
        tdSql.execute(f"alter table {tb} set tag tgcol4='3'")
        tdSql.execute(f"alter table {tb} set tag tgcol5=2")
        tdSql.execute(f"alter table {tb} set tag tgcol6='1'")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = '3'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(0, 4, 5)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 3)

        tdSql.checkData(0, 7, 2)

        tdSql.query(f"select * from {mt} where tgcol2 = '5'")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tgcol3 = 4")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tgcol5 = 2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tgcol6 = '1'")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step13")
        i = 13
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 int, tgcol3 smallint, tgcol4 binary(11), tgcol5 double, tgcol6 binary(20))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( '1', 2, 3, '4', 5, '6' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = '1'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 5)

        tdSql.checkData(0, 7, 6)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol6")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} add tag tgcol2 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol4 int")
        tdSql.execute(f"alter table {mt} add tag tgcol6 bigint")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol1='7'")
        tdSql.execute(f"alter table {tb} set tag tgcol2='8'")
        tdSql.execute(f"alter table {tb} set tag tgcol3=9")
        tdSql.execute(f"alter table {tb} set tag tgcol4=10")
        tdSql.execute(f"alter table {tb} set tag tgcol5=11")
        tdSql.execute(f"alter table {tb} set tag tgcol6=12")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol2 = '8'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 7)

        tdSql.checkData(0, 3, 9)

        tdSql.checkData(0, 4, 11)

        tdSql.checkData(0, 5, 8)

        tdSql.checkData(0, 6, 10)

        tdSql.checkData(0, 7, 12)

        tdLog.info(f"=============== step14")
        i = 14
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 bigint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")

        tdSql.execute(f"alter table {mt} add tag tgcol3 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol4 int")
        tdSql.execute(f"alter table {mt} add tag tgcol5 bigint")
        tdSql.execute(f"alter table {mt} add tag tgcol6 bigint")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} drop tag tgcol6")
        tdSql.execute(f"alter table {mt} add tag tgcol7 bigint")
        tdSql.execute(f"alter table {mt} add tag tgcol8 bigint")

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
 
        
    def TagChange(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_ch_db"
        tbPrefix = "ta_ch_tb"
        mtPrefix = "ta_ch_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        tdLog.info(f"=============== step2")
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.error(f"alter table {mt} rename tag tagcx tgcol3")
        tdSql.error(f"alter table {mt} rename tag tgcol1 tgcol2")

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol3")
        tdSql.execute(f"alter table {mt} rename tag tgcol2 tgcol4")
        tdSql.error(f"alter table {mt} rename tag tgcol4 tgcol3")

        tdLog.info(f"=============== step3")
        i = 3
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol3")
        tdSql.execute(f"alter table {mt} rename tag tgcol2 tgcol4")

        tdLog.info(f"=============== step4")
        i = 4
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol3")
        tdSql.execute(f"alter table {mt} rename tag tgcol2 tgcol4")

        tdLog.info(f"=============== step5")
        i = 5
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, '2' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = '2'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol3")
        tdSql.execute(f"alter table {mt} rename tag tgcol2 tgcol4")

        tdLog.info(f"=============== step6")
        i = 6
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 int, tgcol3 smallint, tgcol4 binary(11), tgcol5 double, tgcol6 binary(20))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( '1', 2, 3, '4', 5, '6' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = '1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(0, 5, 4)
        tdSql.checkData(0, 6, 5)
        tdSql.checkData(0, 7, 6)

        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} rename tag tgcol4 tgcol3")
        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol7")
        tdSql.execute(f"alter table {mt} rename tag tgcol2 tgcol8")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} rename tag tgcol3 tgcol9")
        tdSql.execute(f"alter table {mt} rename tag tgcol5 tgcol10")
        tdSql.execute(f"alter table {mt} rename tag tgcol6 tgcol11")

        tdSql.execute(f"reset query cache")

        tdLog.info(f"=============== step2")
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.error(f"select * from {mt} where tgcol1 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdSql.query(f"select * from {mt} where tgcol3 = 1")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.query(f"select * from {mt} where tgcol4 = 2")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdLog.info(f"=============== step3")
        i = 3
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.error(f"select * from {mt} where tgcol1 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdSql.query(f"select * from {mt} where tgcol3 = 1")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.query(f"select * from {mt} where tgcol4 = 2")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdLog.info(f"=============== step4")
        i = 4
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.error(f"select * from {mt} where tgcol1 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdSql.query(f"select * from {mt} where tgcol3 = 1")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.query(f"select * from {mt} where tgcol4 = 2")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdLog.info(f"=============== step5")
        i = 5
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.error(f"select * from {mt} where tgcol1 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdSql.query(f"select * from {mt} where tgcol3 < 2")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdSql.query(f"select * from {mt} where tgcol4 = '2'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)

        tdLog.info(f"=============== step6")
        i = 6
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.error(f"select * from {mt} where tgcol1 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol4 = 1")
        tdSql.error(f"select * from {mt} where tgcol5 = 1")
        tdSql.error(f"select * from {mt} where tgcol6 = 1")

        tdSql.query(f"select * from {mt} where tgcol7 = '1'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 4)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 6)
        tdSql.checkCols(7)

        tdSql.query(f"select * from {mt} where tgcol8 = 2")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 4)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 6)
        tdSql.checkCols(7)

        tdSql.query(f"select * from {mt} where tgcol9 = '4'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 4)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 6)
        tdSql.checkCols(7)

        tdSql.query(f"select * from {mt} where tgcol10 = 5")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 4)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 6)
        tdSql.checkCols(7)

        tdSql.query(f"select * from {mt} where tgcol11 = '6'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 4)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 6)
        tdSql.checkCols(7)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        
    def TagDelete(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_de_db"
        tbPrefix = "ta_de_tb"
        mtPrefix = "ta_de_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        tdLog.info(f"=============== step2")
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")

        tdLog.info(f"=============== step3")
        i = 3
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")

        tdLog.info(f"=============== step4")
        i = 4
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 < 3")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.query(f"describe {tb}")
        tdSql.checkData(2, 1, "BIGINT")

        tdSql.checkData(3, 1, "FLOAT")

        tdSql.checkData(2, 3, "TAG")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.error(f"alter table {mt} drop tag tgcol1")

        tdLog.info(f"=============== step5")
        i = 5
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, '2' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = '2'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.error(f"alter table {mt} drop tag tgcol1")

        tdLog.info(f"=============== step6")
        i = 6
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")

        tdLog.info(f"=============== step7")
        i = 7
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint, tgcol3 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, '3' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol3 = '3'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.query(f"describe {tb}")
        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 1, "VARCHAR")

        tdSql.checkData(2, 2, 2)

        tdSql.checkData(3, 2, 1)

        tdSql.checkData(4, 2, 10)

        tdSql.checkData(2, 3, "TAG")

        tdSql.checkData(3, 3, "TAG")

        tdSql.checkData(4, 3, "TAG")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")

        tdLog.info(f"=============== step8")
        i = 8
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float, tgcol3 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, '3' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol3 = '3'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")

        tdLog.info(f"=============== step9")
        i = 9
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10), tgcol3 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, '2', '3' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")

        tdLog.info(f"=============== step10")
        i = 10
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 binary(10), tgcol3 binary(10), tgcol4 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( '1', '2', '3', '4' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol4 = '4'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")

        tdLog.info(f"=============== step11")
        i = 11
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 smallint, tgcol4 float, tgcol5 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3, 4, '5' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 5)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol5")

        tdLog.info(f"=============== step12")
        i = 12
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 smallint, tgcol3 float, tgcol4 double, tgcol5 binary(10), tgcol6 binary(20))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3, 4, '5', '6' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 5)

        tdSql.checkData(0, 7, 6)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol5")
        tdSql.execute(f"alter table {mt} drop tag tgcol6")

        tdLog.info(f"=============== step13")
        i = 13
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 int, tgcol3 smallint, tgcol4 binary(11), tgcol5 double, tgcol6 binary(20))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( '1', 2, 3, '4', 5, '6' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = '1'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 5)

        tdSql.checkData(0, 7, 6)

        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol6")

        tdLog.info(f"=============== step2")
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step3")
        i = 3
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step4")
        i = 4
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step5")
        i = 5
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = '1'")

        tdLog.info(f"=============== step6")
        i = 6
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step7")
        i = 7
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step8")
        i = 8
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step9")
        i = 9
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = 1")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step10")
        i = 10
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol1 = '1'")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkCols(3)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol4 = 1")

        tdLog.info(f"=============== step11")
        i = 11
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol4=4")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkCols(4)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol5 = 1")

        tdLog.info(f"=============== step12")
        i = 12
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol4 = 4")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkCols(4)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol5 = 1")
        tdSql.error(f"select * from {mt} where tgcol6 = 1")

        tdLog.info(f"=============== step13")
        i = 13
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.execute(f"reset query cache")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 5)

        tdSql.checkCols(5)

        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol4 = 1")
        tdSql.error(f"select * from {mt} where tgcol6 = 1")

        tdLog.info(f"=============== step14")
        i = 14
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 bigint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")

        tdSql.error(f"alter table xxmt drop tag tag1")
        tdSql.error(f"alter table {tb} drop tag tag1")
        tdSql.error(f"alter table {mt} drop tag tag1")
        tdSql.error(f"alter table {mt} drop tag tagcol1")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.error(f"alter table {mt} drop tag tgcol1")

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        
    def TagCommit(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "db"
        tbPrefix = "tb"
        mtPrefix = "mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        tdLog.info(f"=============== step2")
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} add tag tgcol4 int")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4 =4")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step3")
        i = 3
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} add tag tgcol4 tinyint")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step4")
        i = 4
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.query(f"describe {tb}")
        tdSql.checkData(2, 1, "BIGINT")

        tdSql.checkData(3, 1, "FLOAT")

        tdSql.checkData(2, 3, "TAG")

        tdSql.checkData(3, 3, "TAG")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} add tag tgcol4 float")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step5")
        i = 5
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, '2' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = '2'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} add tag tgcol4 smallint")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.error(f"select * from {mt} where tgcol3 = '1'")

        tdLog.info(f"=============== step6")
        i = 6
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} add tag tgcol5 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol6 binary(10)")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=false")
        tdSql.execute(f"alter table {tb} set tag tgcol5='5'")
        tdSql.execute(f"alter table {tb} set tag tgcol6='6'")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol5 = '5'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 6)

        tdSql.query(f"select * from {mt} where tgcol6 = '6'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 6)

        tdSql.query(f"select * from {mt} where tgcol4 = 1")
        tdSql.checkRows(0)

        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step7")
        i = 7
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint, tgcol3 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, '3' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol3 = '3'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} add tag tgcol5 bigint")
        tdSql.execute(f"alter table {mt} add tag tgcol6 tinyint")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"alter table {tb} set tag tgcol5=5")
        tdSql.execute(f"alter table {tb} set tag tgcol6=6")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol6 = 6")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 6)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step8")
        i = 8
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float, tgcol3 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, '3' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol3 = '3'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} add tag tgcol5 binary(17)")
        tdSql.execute(f"alter table {mt} add tag tgcol6 bool")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"alter table {tb} set tag tgcol5='5'")
        tdSql.execute(f"alter table {tb} set tag tgcol6=1")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol5 = '5'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 1)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step9")
        i = 9
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10), tgcol3 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, '2', '3' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = '2'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.execute(f"alter table {mt} rename tag tgcol1 tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} add tag tgcol5 bool")
        tdSql.execute(f"alter table {mt} add tag tgcol6 float")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4=4")
        tdSql.execute(f"alter table {tb} set tag tgcol5=1")
        tdSql.execute(f"alter table {tb} set tag tgcol6=6")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol5 = 1")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(0, 4, 6)

        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol2 = 1")

        tdLog.info(f"=============== step10")
        i = 10
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 binary(10), tgcol3 binary(10), tgcol4 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( '1', '2', '3', '4' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol4 = '4'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.error(f"alter table {mt} rename tag tgcol1 tgcol4")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} add tag tgcol4 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol5 bool")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4='4'")
        tdSql.execute(f"alter table {tb} set tag tgcol5=false")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = '4'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkData(0, 4, 0)

        tdSql.checkCols(5)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")

        tdLog.info(f"=============== step11")
        i = 11
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 smallint, tgcol4 float, tgcol5 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3, 4, '5' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 5)

        tdSql.error(f"alter table {mt} rename tag tgcol1 tgcol4")

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol5")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} add tag tgcol4 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol5 int")
        tdSql.execute(f"alter table {mt} add tag tgcol6 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol7 bigint")
        tdSql.execute(f"alter table {mt} add tag tgcol8 smallint")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol4='4'")
        tdSql.execute(f"alter table {tb} set tag tgcol5=5")
        tdSql.execute(f"alter table {tb} set tag tgcol6='6'")
        tdSql.execute(f"alter table {tb} set tag tgcol7=7")
        tdSql.execute(f"alter table {tb} set tag tgcol8=8")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol5 =5")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkData(0, 4, 5)

        tdSql.checkData(0, 5, 6)

        tdSql.checkData(0, 6, 7)

        tdSql.checkData(0, 7, 8)

        tdSql.error(f"select * from {mt} where tgcol2 = 1")
        tdSql.error(f"select * from {mt} where tgcol3 = 1")
        tdSql.error(f"select * from {mt} where tgcol9 = 1")

        tdLog.info(f"=============== step12")
        i = 12
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 smallint, tgcol3 float, tgcol4 double, tgcol5 binary(10), tgcol6 binary(20))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3, 4, '5', '6' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 5)

        tdSql.checkData(0, 7, 6)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol3")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol5")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} add tag tgcol2 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol3 int")
        tdSql.execute(f"alter table {mt} add tag tgcol4 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol5 bigint")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol1=false")
        tdSql.execute(f"alter table {tb} set tag tgcol2='5'")
        tdSql.execute(f"alter table {tb} set tag tgcol3=4")
        tdSql.execute(f"alter table {tb} set tag tgcol4='3'")
        tdSql.execute(f"alter table {tb} set tag tgcol5=2")
        tdSql.execute(f"alter table {tb} set tag tgcol6='1'")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol4 = '3'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(0, 4, 5)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 3)

        tdSql.checkData(0, 7, 2)

        tdSql.query(f"select * from {mt} where tgcol2 = '5'")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tgcol3 = 4")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tgcol5 = 2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tgcol6 = '1'")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step13")
        i = 13
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 int, tgcol3 smallint, tgcol4 binary(11), tgcol5 double, tgcol6 binary(20))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( '1', 2, 3, '4', 5, '6' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = '1'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 2)

        tdSql.checkData(0, 4, 3)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 5)

        tdSql.checkData(0, 7, 6)

        tdSql.execute(f"alter table {mt} drop tag tgcol2")
        tdSql.execute(f"alter table {mt} drop tag tgcol4")
        tdSql.execute(f"alter table {mt} drop tag tgcol6")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {mt} add tag tgcol2 binary(10)")
        tdSql.execute(f"alter table {mt} add tag tgcol4 int")
        tdSql.execute(f"alter table {mt} add tag tgcol6 bigint")

        tdSql.execute(f"reset query cache")
        tdSql.execute(f"alter table {tb} set tag tgcol1='7'")
        tdSql.execute(f"alter table {tb} set tag tgcol2='8'")
        tdSql.execute(f"alter table {tb} set tag tgcol3=9")
        tdSql.execute(f"alter table {tb} set tag tgcol4=10")
        tdSql.execute(f"alter table {tb} set tag tgcol5=11")
        tdSql.execute(f"alter table {tb} set tag tgcol6=12")
        tdSql.execute(f"reset query cache")

        tdSql.query(f"select * from {mt} where tgcol2 = '8'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 7)

        tdSql.checkData(0, 3, 9)

        tdSql.checkData(0, 4, 11)

        tdSql.checkData(0, 5, 8)

        tdSql.checkData(0, 6, 10)

        tdSql.checkData(0, 7, 12)

        tdLog.info("=======><=========== taosd stop ")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        tdLog.info("=======><=========== taosd start ")
        # system sh/exec.sh -n dnode1 -s stop -x SIGINT
        # system sh/exec.sh -n dnode1 -s start

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)

        tdSql.execute(f"use {db}")

        tdLog.info(f"=============== step2")
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdLog.info(f"=============== step3")
        i = 3
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdLog.info(f"=============== step4")
        i = 4
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdLog.info(f"=============== step5")
        i = 5
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdLog.info(f"=============== step6")
        i = 6
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol5 = '5'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 6)

        tdSql.query(f"select * from {mt} where tgcol6 = '6'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 6)

        tdSql.query(f"select * from {mt} where tgcol4 = 1")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step7")
        i = 7
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol6 = 6")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 6)

        tdLog.info(f"=============== step8")
        i = 8
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol5 = '5'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 5)

        tdSql.checkData(0, 4, 1)

        tdLog.info(f"=============== step9")
        i = 9
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol5 = 1")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 4)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(0, 4, 6)

        tdLog.info(f"=============== step10")
        i = 10
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol4 = '4'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkData(0, 4, 0)

        tdSql.checkCols(5)

        tdLog.info(f"=============== step11")
        i = 11
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol5 =5")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(0, 3, 4)

        tdSql.checkData(0, 4, 5)

        tdSql.checkData(0, 5, 6)

        tdSql.checkData(0, 6, 7)

        tdSql.checkData(0, 7, 8)

        tdLog.info(f"=============== step12")
        i = 12
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol4 = '3'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(0, 4, 5)

        tdSql.checkData(0, 5, 4)

        tdSql.checkData(0, 6, 3)

        tdSql.checkData(0, 7, 2)

        tdSql.query(f"select * from {mt} where tgcol2 = '5'")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tgcol3 = 4")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tgcol5 = 2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from {mt} where tgcol6 = '1'")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step13")
        i = 13
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.query(f"select * from {mt} where tgcol2 = '8'")
        tdLog.info(f"{tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 7)

        tdSql.checkData(0, 3, 9)

        tdSql.checkData(0, 4, 11)

        tdSql.checkData(0, 5, 8)

        tdSql.checkData(0, 6, 10)

        tdSql.checkData(0, 7, 12)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info("=======> finally")

        