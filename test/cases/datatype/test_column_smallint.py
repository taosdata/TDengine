from new_test_framework.utils import tdLog, tdSql


class TestSmallintColumn:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_static_create_table(self):
        """static create table

        1. 使用 smallint 作为超级表的普通列、标签列
        2. 当 smallint 作为标签列时，使用合法值、非法值创建子表
        3. 当 smallint 作为标签列时，测试 show tags 的返回结果

        Catalog:
            - DataTypes:Smallint
            - Tables:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework, from tests/script/tsim/parser/columnValue_smallint.sim

        """

        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table mt_smallint (ts timestamp, c smallint) tags(tagname smallint)"
        )

        tdLog.info(f"case 0: static create table for test tag values")
        tdSql.execute(f"create table st_smallint_0 using mt_smallint tags(NULL)")
        tdSql.query(f"show create table st_smallint_0")
        tdSql.query(f"show tags from st_smallint_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_smallint_1 using mt_smallint tags(NULL)")
        tdSql.query(f"show tags from st_smallint_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_smallint_2 using mt_smallint tags('NULL')")
        tdSql.query(f"show tags from st_smallint_2")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_smallint_3 using mt_smallint tags('NULL')")
        tdSql.query(f"show tags from st_smallint_3")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_smallint_4 using mt_smallint tags("NULL")')
        tdSql.query(f"show tags from st_smallint_4")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_smallint_5 using mt_smallint tags("NULL")')
        tdSql.query(f"show tags from st_smallint_5")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_smallint_6 using mt_smallint tags(-32767)")
        tdSql.query(f"show tags from st_smallint_6")
        tdSql.checkData(0, 5, -32767)

        tdSql.execute(f"create table st_smallint_7 using mt_smallint tags(32767)")
        tdSql.query(f"show tags from st_smallint_7")
        tdSql.checkData(0, 5, 32767)

        tdSql.execute(f"create table st_smallint_8 using mt_smallint tags(37)")
        tdSql.query(f"show tags from st_smallint_8")
        tdSql.checkData(0, 5, 37)

        tdSql.execute(f"create table st_smallint_9 using mt_smallint tags(-100)")
        tdSql.query(f"show tags from st_smallint_9")
        tdSql.checkData(0, 5, -100)

        tdSql.execute(f"create table st_smallint_10 using mt_smallint tags(+113)")
        tdSql.query(f"show tags from st_smallint_10")
        tdSql.checkData(0, 5, 113)

        tdSql.execute(f"create table st_smallint_11 using mt_smallint tags('-100')")
        tdSql.query(f"show tags from st_smallint_11")
        tdSql.checkData(0, 5, -100)

        tdSql.execute(f'create table st_smallint_12 using mt_smallint tags("+78")')
        tdSql.query(f"show tags from st_smallint_12")
        tdSql.checkData(0, 5, 78)

        tdSql.execute(f"create table st_smallint_13 using mt_smallint tags(+0078)")
        tdSql.query(f"show tags from st_smallint_13")
        tdSql.checkData(0, 5, 78)

        tdSql.execute(f"create table st_smallint_14 using mt_smallint tags(-00078)")
        tdSql.query(f"show tags from st_smallint_14")
        tdSql.checkData(0, 5, -78)

    def test_insert_column_value(self):
        """insert column value

        1. 使用 smallint 作为超级表的普通列、标签列
        2. 当 smallint 作为普通列时，使用合法值、非法值向子表中写入数据

        Catalog:
            - DataTypes:Smallint

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 1: insert values for test column values")

        tdSql.execute(f"insert into st_smallint_0 values(now, NULL)")
        tdSql.query(f"select * from st_smallint_0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_smallint_1 values(now, NULL)")
        tdSql.query(f"select * from st_smallint_1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_smallint_6 values(now, 32767)")
        tdSql.query(f"select * from st_smallint_6")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 32767)

        tdSql.execute(f"insert into st_smallint_7 values(now, -32767)")
        tdSql.query(f"select * from st_smallint_7")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -32767)

        tdSql.execute(f"insert into st_smallint_8 values(now, +100)")
        tdSql.query(f"select * from st_smallint_8")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 100)

        tdSql.execute(f'insert into st_smallint_9 values(now, "-098")')
        tdSql.query(f"select * from st_smallint_9")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -98)

        tdSql.execute(f"insert into st_smallint_10 values(now, '0')")
        tdSql.query(f"select * from st_smallint_10")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_smallint_11 values(now, -0)")
        tdSql.query(f"select * from st_smallint_11")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f'insert into st_smallint_12 values(now, "+056")')
        tdSql.query(f"select * from st_smallint_12")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 56)

        tdSql.execute(f"insert into st_smallint_13 values(now, +056)")
        tdSql.query(f"select * from st_smallint_13")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 56)

        tdSql.execute(f"insert into st_smallint_14 values(now, -056)")
        tdSql.query(f"select * from st_smallint_14")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -56)

    def test_dynamic_create_table(self):
        """dynamic create table

        1. 使用 smallint 作为超级表的普通列、标签列
        2. 使用合法值、非法值向子表中写入数据并自动建表

        Catalog:
            - DataTypes:Smallint

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 2: dynamic create table for test tag values")

        tdSql.execute(
            f"insert into st_smallint_16 using mt_smallint tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show create table st_smallint_16")
        tdSql.query(f"show tags from st_smallint_16")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_smallint_16")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_smallint_17 using mt_smallint tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_smallint_17")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_smallint_17")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_smallint_18 using mt_smallint tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_smallint_18")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_smallint_18")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_smallint_19 using mt_smallint tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_smallint_19")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_smallint_19")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_smallint_20 using mt_smallint tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_smallint_20")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_smallint_20")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_smallint_21 using mt_smallint tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_smallint_21")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_smallint_21")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_smallint_22 using mt_smallint tags(32767) values(now, 32767)"
        )
        tdSql.query(f"show tags from st_smallint_22")
        tdSql.checkData(0, 5, 32767)

        tdSql.query(f"select * from st_smallint_22")
        tdSql.checkData(0, 1, 32767)

        tdSql.execute(
            f"insert into st_smallint_23 using mt_smallint tags(-32767) values(now, -32767)"
        )
        tdSql.query(f"show tags from st_smallint_23")
        tdSql.checkData(0, 5, -32767)

        tdSql.query(f"select * from st_smallint_23")
        tdSql.checkData(0, 1, -32767)

        tdSql.execute(
            f"insert into st_smallint_24 using mt_smallint tags(10) values(now, 10)"
        )
        tdSql.query(f"show tags from st_smallint_24")
        tdSql.checkData(0, 5, 10)

        tdSql.query(f"select * from st_smallint_24")
        tdSql.checkData(0, 1, 10)

        tdSql.execute(
            f'insert into st_smallint_25 using mt_smallint tags("-0") values(now, "-0")'
        )
        tdSql.query(f"show tags from st_smallint_25")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_smallint_25")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_smallint_26 using mt_smallint tags('123') values(now, '123')"
        )
        tdSql.query(f"show tags from st_smallint_26")
        tdSql.checkData(0, 5, 123)

        tdSql.query(f"select * from st_smallint_26")
        tdSql.checkData(0, 1, 123)

        tdSql.execute(
            f"insert into st_smallint_27 using mt_smallint tags(+056) values(now, +00056)"
        )
        tdSql.query(f"show tags from st_smallint_27")
        tdSql.checkData(0, 5, 56)

        tdSql.query(f"select * from st_smallint_27")
        tdSql.checkData(0, 1, 56)

        tdSql.execute(
            f"insert into st_smallint_28 using mt_smallint tags(-056) values(now, -0056)"
        )
        tdSql.query(f"show tags from st_smallint_28")
        tdSql.checkData(0, 5, -56)

        tdSql.query(f"select * from st_smallint_28")
        tdSql.checkData(0, 1, -56)

    def test_alter_tag_value(self):
        """alter tag value

        1. 使用 smallint 作为超级表的标签列
        2. 使用合法值、非法值修改子表的标签值

        Catalog:
            - DataTypes:Smallint

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 3: alter tag value")

        tdSql.error(f"create table st_smallint_e0 using mt_smallint tags(32768)")
        tdSql.execute(f"create table st_smallint_e0_0 using mt_smallint tags(-32768)")
        tdSql.error(f"create table st_smallint_e0 using mt_smallint tags(3276899)")
        tdSql.error(f"create table st_smallint_e0 using mt_smallint tags(-3276833)")
        tdSql.error(f"create table st_smallint_e0 using mt_smallint tags(123abc)")
        tdSql.error(f'create table st_smallint_e0 using mt_smallint tags("123abc")')
        tdSql.error(f"create table st_smallint_e0 using mt_smallint tags(abc)")
        tdSql.error(f'create table st_smallint_e0 using mt_smallint tags("abc")')
        tdSql.error(f'create table st_smallint_e0 using mt_smallint tags(" ")')
        tdSql.error(f"create table st_smallint_e0_1 using mt_smallint tags('')")
        tdSql.execute(f"create table st_smallint_e0_2 using mt_smallint tags('123')")

        tdSql.execute(f"create table st_smallint_e0 using mt_smallint tags(123)")
        tdSql.execute(f"create table st_smallint_e1 using mt_smallint tags(123)")
        tdSql.execute(f"create table st_smallint_e2 using mt_smallint tags(123)")
        tdSql.execute(f"create table st_smallint_e3 using mt_smallint tags(123)")
        tdSql.execute(f"create table st_smallint_e4 using mt_smallint tags(123)")
        tdSql.execute(f"create table st_smallint_e5 using mt_smallint tags(123)")
        tdSql.execute(f"create table st_smallint_e6 using mt_smallint tags(123)")
        tdSql.execute(f"create table st_smallint_e7 using mt_smallint tags(123)")
        tdSql.execute(f"create table st_smallint_e8 using mt_smallint tags(123)")
        tdSql.execute(f"create table st_smallint_e9 using mt_smallint tags(123)")
        tdSql.execute(f"create table st_smallint_e10 using mt_smallint tags(123)")
        tdSql.execute(f"create table st_smallint_e11 using mt_smallint tags(123)")
        tdSql.execute(f"create table st_smallint_e12 using mt_smallint tags(123)")

        tdSql.error(f"insert into st_smallint_e0 values(now, 32768)")
        tdSql.execute(f"insert into st_smallint_e1 values(now, -32768)")
        tdSql.error(f"insert into st_smallint_e2 values(now, 42768)")
        tdSql.error(f"insert into st_smallint_e3 values(now, -32769)")
        tdSql.error(f"insert into st_smallint_e6 values(now, 123abc)")
        tdSql.error(f'insert into st_smallint_e7 values(now, "123abc")')
        tdSql.error(f"insert into st_smallint_e9 values(now, abc)")
        tdSql.error(f'insert into st_smallint_e10 values(now, "abc")')
        tdSql.error(f'insert into st_smallint_e11 values(now, " ")')
        tdSql.error(f"insert into st_smallint_e12 values(now, '')")

        tdSql.error(
            f"insert into st_smallint_e13 using mt_smallint tags(033) values(now, 32768)"
        )
        tdSql.execute(
            f"insert into st_smallint_e14_1 using mt_smallint tags(033) values(now, -32768)"
        )
        tdSql.error(
            f"insert into st_smallint_e15 using mt_smallint tags(033) values(now, 32968)"
        )
        tdSql.error(
            f"insert into st_smallint_e16 using mt_smallint tags(033) values(now, -33768)"
        )
        tdSql.error(
            f"insert into st_smallint_e19 using mt_smallint tags(033) values(now, 123abc)"
        )
        tdSql.error(
            f'insert into st_smallint_e20 using mt_smallint tags(033) values(now, "123abc")'
        )
        tdSql.error(
            f"insert into st_smallint_e22 using mt_smallint tags(033) values(now, abc)"
        )
        tdSql.error(
            f'insert into st_smallint_e23 using mt_smallint tags(033) values(now, "abc")'
        )
        tdSql.error(
            f'insert into st_smallint_e24 using mt_smallint tags(033) values(now, " ")'
        )
        tdSql.error(
            f"insert into st_smallint_e25_1 using mt_smallint tags(033) values(now, '')"
        )

        tdSql.error(
            f"insert into st_smallint_e13 using mt_smallint tags(32768) values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_smallint_e14 using mt_smallint tags(-32768) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_smallint_e15 using mt_smallint tags(72768) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_smallint_e16 using mt_smallint tags(-92768) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_smallint_e19 using mt_smallint tags(123abc) values(now, -033)"
        )
        tdSql.error(
            f'insert into st_smallint_e20 using mt_smallint tags("123abc") values(now, -033)'
        )
        tdSql.error(
            f"insert into st_smallint_e22 using mt_smallint tags(abc) values(now, -033)"
        )
        tdSql.error(
            f'insert into st_smallint_e23 using mt_smallint tags("abc") values(now, -033)'
        )
        tdSql.error(
            f'insert into st_smallint_e24 using mt_smallint tags(" ") values(now, -033)'
        )
        tdSql.error(
            f"insert into st_smallint_e25 using mt_smallint tags('') values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_smallint_e26 using mt_smallint tags('123') values(now, -033)"
        )

        tdSql.execute(
            f"insert into st_smallint_e13 using mt_smallint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_smallint_e14 using mt_smallint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_smallint_e15 using mt_smallint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_smallint_e16 using mt_smallint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_smallint_e17 using mt_smallint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_smallint_e18 using mt_smallint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_smallint_e19 using mt_smallint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_smallint_e20 using mt_smallint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_smallint_e21 using mt_smallint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_smallint_e22 using mt_smallint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_smallint_e23 using mt_smallint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_smallint_e24 using mt_smallint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_smallint_e25 using mt_smallint tags(033) values(now, 00062)"
        )

        tdSql.error(f"alter table st_smallint_e13 set tag tagname=32768")
        tdSql.execute(f"alter table st_smallint_e14 set tag tagname=-32768")
        tdSql.error(f"alter table st_smallint_e15 set tag tagname=52768")
        tdSql.error(f"alter table st_smallint_e16 set tag tagname=-32778")
        tdSql.error(f"alter table st_smallint_e19 set tag tagname=123abc")
        tdSql.error(f'alter table st_smallint_e20 set tag tagname="123abc"')
        tdSql.error(f"alter table st_smallint_e22 set tag tagname=abc")
        tdSql.error(f'alter table st_smallint_e23 set tag tagname="abc"')
        tdSql.error(f'alter table st_smallint_e24 set tag tagname=" "')
        tdSql.error(f"alter table st_smallint_e25 set tag tagname=''")
        tdSql.execute(f"alter table st_smallint_e26 set tag tagname='123'")

    def test_illegal_input(self):
        """illegal input

        1. 使用 smallint 作为超级表的标签列
        2. 使用非法标签值创建子表

        Catalog:
            - DataTypes:Smallint

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 4: illegal input")
