from new_test_framework.utils import tdLog, tdSql


class TestJsonColumn:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_static_create_table(self):
        """static create table

        1. 使用 json 作为超级表的普通列、标签列
        2. 当 json 作为标签列时，使用合法值、非法值创建子表
        3. 当 json 作为标签列时，测试 show tags 的返回结果

        Catalog:
            - DataTypes:Json
            - Tables:Create

        Since: v3.0.0.0
        Labels: common,ci
        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table mt_json (ts timestamp, c varchar(50)) tags(tagname json)"
        )

        tdLog.info(f"case 0: static create table for test tag values")
        tdSql.execute(f"create table st_json_0 using mt_json tags(NULL)")
        tdSql.query(f"show tags from st_json_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_json_1 using mt_json tags(NULL)")
        tdSql.query(f"show tags from st_json_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_json_2 using mt_json tags('NULL')")
        tdSql.query(f"show tags from st_json_2")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_json_3 using mt_json tags('NULL')")
        tdSql.query(f"show tags from st_json_3")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_json_4 using mt_json tags("NULL")')
        tdSql.query(f"show tags from st_json_4")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_json_5 using mt_json tags("NULL")')
        tdSql.query(f"show tags from st_json_5")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_json_6 using mt_json tags("")')
        tdSql.query(f"show tags from st_json_6")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_json_7 using mt_json tags(" ")')
        tdSql.query(f"show tags from st_json_7")
        tdSql.checkData(0, 5, None)

        str = '{\\"k1\\":\\"v1\\",\\"k2\\":\\"v2\\"}'
        tdLog.info(f'create table st_json_8 using mt_json tags("{str}")')
        tdSql.execute(f'create table st_json_8 using mt_json tags("{str}")')
        tdSql.query(f"show tags from st_json_8")
        tdSql.checkData(0, 5, '{"k1":"v1","k2":"v2"}')

    def test_insert_column_value(self):
        """insert column value

        1. 使用 json 作为超级表的普通列、标签列
        2. 当 json 作为普通列时，使用合法值、非法值向子表中写入数据

        Catalog:
            - DataTypes:Json

        Since: v3.0.0.0
        Labels: common,ci
        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 1: insert values for test column values")

    def test_dynamic_create_table(self):
        """dynamic create table

        1. 使用 json 作为超级表的普通列、标签列
        2. 使用合法值、非法值向子表中写入数据并自动建表

        Catalog:
            - DataTypes:Json

        Since: v3.0.0.0
        Labels: common,ci
        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 2: dynamic create table for test tag values")

        tdSql.execute(
            f"insert into st_json_100 using mt_json tags(NULL) values(now,NULL)"
        )
        tdSql.query(f"show tags from st_json_100")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_json_100")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_json_101 using mt_json tags(NULL) values(now,NULL)"
        )
        tdSql.query(f"show tags from st_json_101")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_json_101")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_json_102 using mt_json tags('NULL') values(now,'NULL')"
        )
        tdSql.query(f"show tags from st_json_102")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_json_102")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(
            f"insert into st_json_103 using mt_json tags('NULL') values(now,'NULL')"
        )
        tdSql.query(f"show tags from st_json_103")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_json_103")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(
            f'insert into st_json_104 using mt_json tags("NULL") values(now,"NULL")'
        )
        tdSql.query(f"show tags from st_json_104")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_json_104")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(
            f'insert into st_json_105 using mt_json tags("NULL") values(now,"NULL")'
        )
        tdSql.query(f"show tags from st_json_105")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_json_105")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(
            f'insert into st_json_106 using mt_json tags("") values(now,"vc")'
        )
        tdSql.query(f"show tags from st_json_106")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_json_106")
        tdSql.checkData(0, 1, "vc")

        tdSql.execute(
            f'insert into st_json_107 using mt_json tags(" ") values(now,"vc")'
        )
        tdSql.query(f"show tags from st_json_107")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_json_107")
        tdSql.checkData(0, 1, "vc")

        str = '{\\"k1\\":\\"v1\\",\\"k2\\":\\"v2\\"}'
        tdSql.execute(
            f'insert into st_json_108 using mt_json tags("{str}") values(now,"vc")'
        )
        tdSql.query(f"show tags from st_json_108")
        tdSql.checkData(0, 5, '{"k1":"v1","k2":"v2"}')

        tdSql.query(f"select * from st_json_108")
        tdSql.checkData(0, 1, "vc")

    def test_alter_tag_value(self):
        """alter tag value

        1. 使用 json 作为超级表的标签列
        2. 使用合法值、非法值修改子表的标签值

        Catalog:
            - DataTypes:Json

        Since: v3.0.0.0
        Labels: common,ci
        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 3: alter tag value")
        tdSql.execute(f"alter table st_json_100  set tag tagname=NULL")
        tdSql.query(f"show tags from st_json_100")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_json_101  set tag tagname=NULL")
        tdSql.query(f"show tags from st_json_101")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_json_102  set tag tagname='NULL'")
        tdSql.query(f"show tags from st_json_102")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_json_103  set tag tagname='NULL'")
        tdSql.query(f"show tags from st_json_103")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'alter table st_json_104  set tag tagname="NULL"')
        tdSql.query(f"show tags from st_json_104")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'alter table st_json_105  set tag tagname="NULL"')
        tdSql.query(f"show tags from st_json_105")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'alter table st_json_106  set tag tagname=""')
        tdSql.query(f"show tags from st_json_106")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'alter table st_json_107  set tag tagname=" "')
        tdSql.query(f"show tags from st_json_107")
        tdSql.checkData(0, 5, None)

        str = '{\\"k1\\":\\"v1\\",\\"k2\\":\\"v2\\"}'
        tdSql.execute(f'alter table st_json_108  set tag tagname="{str}"')
        tdSql.query(f"show tags from st_json_108")
        tdSql.checkData(0, 5, '{"k1":"v1","k2":"v2"}')

    def test_illegal_input(self):
        """illegal input

        1. 使用 json 作为超级表的标签列
        2. 使用非法标签值创建子表

        Catalog:
            - DataTypes:Json

        Since: v3.0.0.0
        Labels: common,ci
        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 4: illegal input")

        # case 04: illegal input
        tdSql.error(f"error create table st_json_206 using mt_json tags(+0123)")
        tdSql.error(f"error create table st_json_207 using mt_json tags(-01.23)")
        tdSql.error(f"error create table st_json_208 using mt_json tags(+0x01)")
        tdSql.error(f"error create table st_json_209 using mt_json tags(-0b01)")
        tdSql.error(f"error create table st_json_2010 using mt_json tags(-0.1e-10)")
        tdSql.error(f"error create table st_json_2011 using mt_json tags(+0.1E+2)")
        tdSql.error(f"error create table st_json_2012 using mt_json tags(tRue)")
        tdSql.error(f"error create table st_json_2013 using mt_json tags(FalsE)")
        tdSql.error(f"error create table st_json_2014 using mt_json tags(noW)")
        tdSql.error(f"error create table st_json_2015 using mt_json tags(toDay)")
        tdSql.error(
            f"error insert into st_json_206 using mt_json tags(+0123) values(now, NULL);"
        )
        tdSql.error(
            f"error insert into st_json_207 using mt_json tags(-01.23) values(now, NULL);"
        )
        tdSql.error(
            f"error insert into st_json_208 using mt_json tags(+0x01) values(now, NULL);"
        )
        tdSql.error(
            f"error insert into st_json_209 using mt_json tags(-0b01) values(now, NULL);"
        )
        tdSql.error(
            f"error insert into st_json_2010 using mt_json tags(-0.1e-10) values(now, NULL);"
        )
        tdSql.error(
            f"error insert into st_json_2011 using mt_json tags(+0.1E+2) values(now, NULL);"
        )
        tdSql.error(
            f"error insert into st_json_2012 using mt_json tags(tRue) values(now, NULL);"
        )
        tdSql.error(
            f"error insert into st_json_2013 using mt_json tags(FalsE) values(now, NULL);"
        )
        tdSql.error(
            f"error insert into st_json_2014 using mt_json tags(noW) values(now, NULL);"
        )
        tdSql.error(
            f"error insert into st_json_2015 using mt_json tags(toDay) values(now, NULL);"
        )
