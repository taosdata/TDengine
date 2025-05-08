from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNcharColumn:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_static_create_table(self):
        """static create table

        1. 使用 nchar 作为超级表的普通列、标签列
        2. 当 nchar 作为标签列时，使用合法值、非法值创建子表
        3. 当 nchar 作为标签列时，测试 show tags 的返回结果

        Catalog:
            - DataTypes:Nchar
            - Tables:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/parser/columnValue_nchar.sim

        """

        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table mt_nchar (ts timestamp, c nchar(50)) tags(tagname nchar(50))"
        )

        tdLog.info(f"case 0: static create table for test tag values")

        tdSql.execute(f"create table st_nchar_0 using mt_nchar tags(NULL)")
        tdSql.query(f"show create table st_nchar_0")
        tdSql.query(f"show tags from st_nchar_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_nchar_1 using mt_nchar tags(NULL)")
        tdSql.query(f"show tags from st_nchar_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_nchar_2 using mt_nchar tags('NULL')")
        tdSql.query(f"show tags from st_nchar_2")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f"create table st_nchar_3 using mt_nchar tags('NULL')")
        tdSql.query(f"show tags from st_nchar_3")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f'create table st_nchar_4 using mt_nchar tags("NULL")')
        tdSql.query(f"show tags from st_nchar_4")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f'create table st_nchar_5 using mt_nchar tags("NULL")')
        tdSql.query(f"show tags from st_nchar_5")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f"create table st_nchar_6 using mt_nchar tags(+0123)")
        tdSql.query(f"show tags from st_nchar_6")
        tdSql.checkData(0, 5, "+0123")

        tdSql.execute(f"create table st_nchar_7 using mt_nchar tags(-01.23)")
        tdSql.query(f"show tags from st_nchar_7")
        tdSql.checkData(0, 5, "-01.23")

        tdSql.execute(f"create table st_nchar_8 using mt_nchar tags(+0x01)")
        tdSql.query(f"show tags from st_nchar_8")
        tdSql.checkData(0, 5, "+0x01")

        tdSql.execute(f"create table st_nchar_9 using mt_nchar tags(-0b01)")
        tdSql.query(f"show tags from st_nchar_9")
        tdSql.checkData(0, 5, "-0b01")

        tdSql.execute(f"create table st_nchar_10 using mt_nchar tags(-0.1e-10)")
        tdSql.query(f"show tags from st_nchar_10")
        tdSql.checkData(0, 5, "-0.1e-10")

        tdSql.execute(f"create table st_nchar_11 using mt_nchar tags(+0.1E+2)")
        tdSql.query(f"show tags from st_nchar_11")
        tdSql.checkData(0, 5, "+0.1e+2")

        tdSql.execute(f"create table st_nchar_12 using mt_nchar tags(tRue)")
        tdSql.query(f"show tags from st_nchar_12")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_nchar_13 using mt_nchar tags(FalsE)")
        tdSql.query(f"show tags from st_nchar_13")
        tdSql.checkData(0, 5, "false")

        tdSql.execute(f"create table st_nchar_14 using mt_nchar tags(noW)")
        tdSql.query(f"show tags from st_nchar_14")
        tdSql.checkData(0, 5, "now")

        tdSql.execute(f"create table st_nchar_15 using mt_nchar tags(toDay)")
        tdSql.query(f"show tags from st_nchar_15")
        tdSql.checkData(0, 5, "today")

    def test_insert_column_value(self):
        """insert column value

        1. 使用 nchar 作为超级表的普通列、标签列
        2. 当 nchar 作为普通列时，使用合法值、非法值向子表中写入数据

        Catalog:
            - DataTypes:Nchar

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 1: insert values for test column values")

        tdSql.execute(f"insert into st_nchar_0 values(now, NULL)")
        tdSql.query(f"select * from st_nchar_0")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_nchar_1 values(now, NULL)")
        tdSql.query(f"select * from st_nchar_1")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_nchar_2 values(now, 'NULL')")
        tdSql.query(f"select * from st_nchar_2")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(f"insert into st_nchar_3 values(now, 'NULL')")
        tdSql.query(f"select * from st_nchar_3")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(f'insert into st_nchar_4 values(now, "NULL")')
        tdSql.query(f"select * from st_nchar_4")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(f'insert into st_nchar_5 values(now, "NULL")')
        tdSql.query(f"select * from st_nchar_5")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(f"insert into st_nchar_6 values(now, +0123)")
        tdSql.query(f"select * from st_nchar_6")
        tdSql.checkData(0, 1, "+0123")

        tdSql.execute(f"insert into st_nchar_7 values(now, -01.23)")
        tdSql.query(f"select * from st_nchar_7")
        tdSql.checkData(0, 1, "-01.23")

        tdSql.execute(f"insert into st_nchar_8 values(now, +0x01)")
        tdSql.query(f"select * from st_nchar_8")
        tdSql.checkData(0, 1, "+0x01")

        tdSql.execute(f"insert into st_nchar_9 values(now, -0b01)")
        tdSql.query(f"select * from st_nchar_9")
        tdSql.checkData(0, 1, "-0b01")

        tdSql.execute(f"insert into st_nchar_10 values(now, -0.1e-10)")
        tdSql.query(f"select * from st_nchar_10")
        tdSql.checkData(0, 1, "-0.1e-10")

        tdSql.execute(f"insert into st_nchar_11 values(now, +0.1E+2)")
        tdSql.query(f"select * from st_nchar_11")
        tdSql.checkData(0, 1, "+0.1e+2")

        tdSql.execute(f"insert into st_nchar_12 values(now, tRue)")
        tdSql.query(f"select * from st_nchar_12")
        tdSql.checkData(0, 1, "true")

        tdSql.execute(f"insert into st_nchar_13 values(now, FalsE)")
        tdSql.query(f"select * from st_nchar_13")
        tdSql.checkData(0, 1, "false")

        tdSql.execute(f"insert into st_nchar_14 values(now, noW)")
        tdSql.query(f"select * from st_nchar_14")
        tdSql.checkData(0, 1, "now")

        tdSql.execute(f"insert into st_nchar_15 values(now, toDay)")
        tdSql.query(f"select * from st_nchar_15")
        tdSql.checkData(0, 1, "today")

    def test_dynamic_create_table(self):
        """dynamic create table

        1. 使用 nchar 作为超级表的普通列、标签列
        2. 使用合法值、非法值向子表中写入数据并自动建表

        Catalog:
            - DataTypes:Nchar

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 2: dynamic create table for test tag values")

        tdSql.execute(
            f"insert into st_nchar_0 using mt_nchar tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_nchar_0")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_nchar_0")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_nchar_1 using mt_nchar tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_nchar_1")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_nchar_1")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_nchar_2 using mt_nchar tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_nchar_2")
        tdSql.checkData(0, 5, "NULL")

        tdSql.query(f"select * from st_nchar_2")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(
            f"insert into st_nchar_3 using mt_nchar tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_nchar_3")
        tdSql.checkData(0, 5, "NULL")

        tdSql.query(f"select * from st_nchar_3")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(
            f'insert into st_nchar_4 using mt_nchar tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_nchar_4")
        tdSql.checkData(0, 5, "NULL")

        tdSql.query(f"select * from st_nchar_4")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(
            f'insert into st_nchar_5 using mt_nchar tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_nchar_5")
        tdSql.checkData(0, 5, "NULL")

        tdSql.query(f"select * from st_nchar_5")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(
            f"insert into st_nchar_6 using mt_nchar tags(+0123) values(now, +0123)"
        )
        tdSql.query(f"show tags from st_nchar_6")
        tdSql.checkData(0, 5, "+0123")

        tdSql.query(f"select * from st_nchar_6")
        tdSql.checkData(0, 1, "+0123")

        tdSql.execute(
            f"insert into st_nchar_7 using mt_nchar tags(-01.23) values(now, -01.23)"
        )
        tdSql.query(f"show tags from st_nchar_7")
        tdSql.checkData(0, 5, "-01.23")

        tdSql.query(f"select * from st_nchar_7")
        tdSql.checkData(0, 1, "-01.23")

        tdSql.execute(
            f"insert into st_nchar_8 using mt_nchar tags(+0x01) values(now, +0x01)"
        )
        tdSql.query(f"show tags from st_nchar_8")
        tdSql.checkData(0, 5, "+0x01")

        tdSql.query(f"select * from st_nchar_8")
        tdSql.checkData(0, 1, "+0x01")

        tdSql.execute(
            f"insert into st_nchar_9 using mt_nchar tags(-0b01) values(now, -0b01)"
        )
        tdSql.query(f"show tags from st_nchar_9")
        tdSql.checkData(0, 5, "-0b01")

        tdSql.query(f"select * from st_nchar_9")
        tdSql.checkData(0, 1, "-0b01")

        tdSql.execute(
            f"insert into st_nchar_10 using mt_nchar tags(-0.1e-10) values(now, -0.1e-10)"
        )
        tdSql.query(f"show tags from st_nchar_10")
        tdSql.checkData(0, 5, "-0.1e-10")

        tdSql.query(f"select * from st_nchar_10")
        tdSql.checkData(0, 1, "-0.1e-10")

        tdSql.execute(
            f"insert into st_nchar_11 using mt_nchar tags(+0.1E+2) values(now, +0.1E+2)"
        )
        tdSql.query(f"show tags from st_nchar_11")
        tdSql.checkData(0, 5, "+0.1e+2")

        tdSql.query(f"select * from st_nchar_11")
        tdSql.checkData(0, 1, "+0.1e+2")

        tdSql.execute(
            f"insert into st_nchar_12 using mt_nchar tags(tRue) values(now, tRue)"
        )
        tdSql.query(f"show tags from st_nchar_12")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_nchar_12")
        tdSql.checkData(0, 1, "true")

        tdSql.execute(
            f"insert into st_nchar_13 using mt_nchar tags(FalsE) values(now, FalsE)"
        )
        tdSql.query(f"show tags from st_nchar_13")
        tdSql.checkData(0, 5, "false")

        tdSql.query(f"select * from st_nchar_13")
        tdSql.checkData(0, 1, "false")

        tdSql.execute(
            f"insert into st_nchar_14 using mt_nchar tags(noW) values(now, noW)"
        )
        tdSql.query(f"show tags from st_nchar_14")
        tdSql.checkData(0, 5, "now")

        tdSql.query(f"select * from st_nchar_14")
        tdSql.checkData(0, 1, "now")

        tdSql.execute(
            f"insert into st_nchar_15 using mt_nchar tags(toDay) values(now, toDay)"
        )
        tdSql.query(f"show tags from st_nchar_15")
        tdSql.checkData(0, 5, "today")

        tdSql.query(f"select * from st_nchar_15")
        tdSql.checkData(0, 1, "today")

    def test_alter_tag_value(self):
        """alter tag value

        1. 使用 nchar 作为超级表的标签列
        2. 使用合法值、非法值修改子表的标签值

        Catalog:
            - DataTypes:Nchar

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 3: alter tag value")
        tdSql.execute(f"alter table st_nchar_0  set tag tagname=NULL")
        tdSql.query(f"show tags from st_nchar_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_nchar_1  set tag tagname=NULL")
        tdSql.query(f"show tags from st_nchar_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_nchar_2  set tag tagname='NULL'")
        tdSql.query(f"show tags from st_nchar_2")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f"alter table st_nchar_3  set tag tagname='NULL'")
        tdSql.query(f"show tags from st_nchar_3")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f'alter table st_nchar_4  set tag tagname="NULL"')
        tdSql.query(f"show tags from st_nchar_4")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f'alter table st_nchar_5  set tag tagname="NULL"')
        tdSql.query(f"show tags from st_nchar_5")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f"alter table st_nchar_6  set tag tagname=+0123")
        tdSql.query(f"show tags from st_nchar_6")
        tdSql.checkData(0, 5, "+0123")

        tdSql.execute(f"alter table st_nchar_7  set tag tagname=-01.23")
        tdSql.query(f"show tags from st_nchar_7")
        tdSql.checkData(0, 5, "-01.23")

        tdSql.execute(f"alter table st_nchar_8  set tag tagname=+0x01")
        tdSql.query(f"show tags from st_nchar_8")
        tdSql.checkData(0, 5, "+0x01")

        tdSql.execute(f"alter table st_nchar_9  set tag tagname=-0b01")
        tdSql.query(f"show tags from st_nchar_9")
        tdSql.checkData(0, 5, "-0b01")

        tdSql.execute(f"alter table st_nchar_10  set tag tagname=-0.1e-10")
        tdSql.query(f"show tags from st_nchar_10")
        tdSql.checkData(0, 5, "-0.1e-10")

        tdSql.execute(f"alter table st_nchar_11  set tag tagname=+0.1E+2")
        tdSql.query(f"show tags from st_nchar_11")
        tdSql.checkData(0, 5, "+0.1e+2")

        tdSql.execute(f"alter table st_nchar_12  set tag tagname=tRue")
        tdSql.query(f"show tags from st_nchar_12")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"alter table st_nchar_13  set tag tagname=FalsE")
        tdSql.query(f"show tags from st_nchar_13")
        tdSql.checkData(0, 5, "false")

        tdSql.execute(f"alter table st_nchar_14  set tag tagname=noW")
        tdSql.query(f"show tags from st_nchar_14")
        tdSql.checkData(0, 5, "now")

        tdSql.execute(f"alter table st_nchar_15  set tag tagname=toDay")
        tdSql.query(f"show tags from st_nchar_15")
        tdSql.checkData(0, 5, "today")

    def test_illegal_input(self):
        """illegal input

        1. 使用 nchar 作为超级表的标签列
        2. 使用非法标签值创建子表

        Catalog:
            - DataTypes:Nchar

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 4: illegal input")
