from new_test_framework.utils import tdLog, tdSql


class TestIntColumn:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_static_create_table(self):
        """static create table

        1. 使用 uint 作为超级表的普通列、标签列
        2. 当 uint 作为标签列时，使用合法值、非法值创建子表
        3. 当 uint 作为标签列时，测试 show tags 的返回结果

        Catalog:
            - DataTypes:UInt
            - Tables:Create

        Since: v3.0.0.0
        Labels: common,ci
        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"create super table")
        tdSql.execute(f"mt_geometry (ts timestamp, c geometry(128)) tags(tagname geometry(128))")

        tdLog.info(f"case 0: static create table for test tag values")



tdSql.execute(f'create table st_geometry_0  using mt_geometry tags(NULL)
tdSql.query(f'show tags from st_geometry_0
tdSql.checkData(0, 5, None )

tdSql.execute(f'create table st_geometry_1  using mt_geometry tags(NULL)
tdSql.query(f'show tags from st_geometry_1
tdSql.checkData(0, 5, None )

tdSql.execute(f'create table st_geometry_2  using mt_geometry tags('NULL')")
tdSql.query(f'show tags from st_geometry_2
tdSql.checkData(0, 5, None )

tdSql.execute(f'create table st_geometry_3  using mt_geometry tags('NULL')")
tdSql.query(f'show tags from st_geometry_3
tdSql.checkData(0, 5, None )

tdSql.execute(f'create table st_geometry_4  using mt_geometry tags("NULL")
tdSql.query(f'show tags from st_geometry_4
tdSql.checkData(0, 5, None )

tdSql.execute(f'create table st_geometry_5  using mt_geometry tags("NULL")
tdSql.query(f'show tags from st_geometry_5
tdSql.checkData(0, 5, None )

tdSql.execute(f'create table st_geometry_6  using mt_geometry tags("POINT(1.0 1.0)")
tdSql.query(f'show tags from st_geometry_6
tdSql.checkData(0, 5,  @POINT (1.000000 1.000000)@ )
  print $data05

tdSql.execute(f'create table st_geometry_7  using mt_geometry tags(" LINESTRING(1.0 1.0, 2.0 2.0)")
tdSql.query(f'show tags from st_geometry_7
tdSql.checkData(0, 5,  @LINESTRING (1.000000 1.000000, 2.000000 2.000000)@ )
  print $data05

tdSql.execute(f'create table st_geometry_8  using mt_geometry tags("POLYGON((1.0 1.0, -2.0 +2.0, 1.0 1.0))")
tdSql.query(f'show tags from st_geometry_8
tdSql.checkData(0, 5,  @POLYGON ((1.000000 1.000000, -2.000000 2.000000, 1.000000 1.000000))@ )
  print $data05



    def test_insert_column_value(self):
        """insert column value

        1. 使用 uint 作为超级表的普通列、标签列
        2. 当 uint 作为普通列时，使用合法值、非法值向子表中写入数据

        Catalog:
            - DataTypes:UInt

        Since: v3.0.0.0
        Labels: common,ci
        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 1: insert values for test column values")



tdSql.execute(f'insert into st_geometry_0 values(now, NULL)
tdSql.query(f'select *from st_geometry_0
tdSql.checkData(0, 1, None )

tdSql.execute(f'insert into st_geometry_1 values(now, NULL)
tdSql.query(f'select *from st_geometry_1
tdSql.checkData(0, 1, None )

tdSql.execute(f'insert into st_geometry_2 values(now, 'NULL')
tdSql.query(f'select *from st_geometry_2
tdSql.checkData(0, 1, None )

tdSql.execute(f'insert into st_geometry_3 values(now, 'NULL')
tdSql.query(f'select *from st_geometry_3
tdSql.checkData(0, 1, None )

tdSql.execute(f'insert into st_geometry_4 values(now, "NULL")
tdSql.query(f'select *from st_geometry_4
tdSql.checkData(0, 1, None )

tdSql.execute(f'insert into st_geometry_5 values(now, "NULL")
tdSql.query(f'select *from st_geometry_5
tdSql.checkData(0, 1, None )

tdSql.execute(f'insert into st_geometry_6 values(now, "POINT(1.0 1.0)")
tdSql.query(f'select *from st_geometry_6
tdSql.checkData(0, 1, @POINT (1.000000 1.000000)@ )
#  return -1
endi
tdSql.execute(f'insert into st_geometry_7 values(now, " LINESTRING(1.0 1.0, 2.0 2.0)")
tdSql.query(f'select *from st_geometry_7
tdSql.checkData(0, 1, @LINESTRING (1.000000 1.000000, 2.000000 2.000000)@ )
#  return -1
endi
tdSql.execute(f'insert into st_geometry_8 values(now, "POLYGON((1.0 1.0, -2.0 +2.0, 1.0 1.0))")
tdSql.query(f'select *from st_geometry_8
tdSql.checkData(0, 1, @POLYGON ((1.000000 1.000000, -2.000000 2.000000, 1.000000 1.000000))@ )
#  return -1
endi

      
    def test_dynamic_create_table(self):
        """dynamic create table

        1. 使用 uint 作为超级表的普通列、标签列
        2. 使用合法值、非法值向子表中写入数据并自动建表

        Catalog:
            - DataTypes:UInt

        Since: v3.0.0.0
        Labels: common,ci
        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 2: dynamic create table for test tag values")

## case 02: dynamic create table for test tag values
tdSql.execute(f'insert into st_geometry_100  using mt_geometry tags(NULL) values(now, NULL)
tdSql.query(f'show tags from st_geometry_100
tdSql.checkData(0, 5, None )

tdSql.query(f'select *from st_geometry_100
tdSql.checkData(0, 1, None )

tdSql.execute(f'insert into st_geometry_101  using mt_geometry tags(NULL) values(now, NULL)
tdSql.query(f'show tags from st_geometry_101
tdSql.checkData(0, 5, None )

tdSql.query(f'select *from st_geometry_100
tdSql.checkData(0, 1, None )

tdSql.execute(f'insert into st_geometry_102  using mt_geometry tags('NULL')") values(now, 'NULL')
tdSql.query(f'show tags from st_geometry_102
tdSql.checkData(0, 5, None )

tdSql.query(f'select *from st_geometry_100
tdSql.checkData(0, 1, None )

tdSql.execute(f'insert into st_geometry_103  using mt_geometry tags('NULL')") values(now, 'NULL')
tdSql.query(f'show tags from st_geometry_103
tdSql.checkData(0, 5, None )

tdSql.query(f'select *from st_geometry_100
tdSql.checkData(0, 1, None )

tdSql.execute(f'insert into st_geometry_104  using mt_geometry tags("NULL") values(now, "NULL")
tdSql.query(f'show tags from st_geometry_104
tdSql.checkData(0, 5, None )

tdSql.query(f'select *from st_geometry_100
tdSql.checkData(0, 1, None )

tdSql.execute(f'insert into st_geometry_105  using mt_geometry tags("NULL") values(now, "NULL")
tdSql.query(f'show tags from st_geometry_105
tdSql.checkData(0, 5, None )

tdSql.query(f'select *from st_geometry_100
tdSql.checkData(0, 1, None )

tdSql.execute(f'insert into st_geometry_106  using mt_geometry tags("POINT(1.0 1.0)") values(now, "POINT(1.0 1.0)")
tdSql.query(f'show tags from st_geometry_106
tdSql.checkData(0, 5,  @POINT (1.000000 1.000000)@ )

tdSql.query(f'select *from st_geometry_106
tdSql.checkData(0, 1, @POINT (1.000000 1.000000)@ )
#  return -1
endi
tdSql.execute(f'insert into st_geometry_107  using mt_geometry tags(" LINESTRING(1.0 1.0, 2.0 2.0)") values(now, "LINESTRING(1.0 1.0, 2.0 2.0)")
tdSql.query(f'show tags from st_geometry_107
tdSql.checkData(0, 5,  @LINESTRING (1.000000 1.000000, 2.000000 2.000000)@ )

tdSql.query(f'select *from st_geometry_107
tdSql.checkData(0, 1, @LINESTRING (1.000000 1.000000, 2.000000 2.000000)@ )
#  return -1
endi
tdSql.execute(f'insert into st_geometry_108  using mt_geometry tags("POLYGON((1.0 1.0, -2.0 +2.0, 1.0 1.0))") values(now, "POLYGON((1.0 1.0, -2.0 +2.0, 1.0 1.0))")
tdSql.query(f'show tags from st_geometry_108
tdSql.checkData(0, 5,  @POLYGON ((1.000000 1.000000, -2.000000 2.000000, 1.000000 1.000000))@ )

tdSql.query(f'select *from st_geometry_108
tdSql.checkData(0, 1, @POLYGON ((1.000000 1.000000, -2.000000 2.000000, 1.000000 1.000000))@ )
#  return -1
endi

         

    def test_alter_tag_value(self):
        """alter tag value

        1. 使用 uint 作为超级表的标签列
        2. 使用合法值、非法值修改子表的标签值

        Catalog:
            - DataTypes:UInt

        Since: v3.0.0.0
        Labels: common,ci
        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 3: alter tag value")

         
## case 03: alter tag values
tdSql.execute(f'alter table st_geometry_0 set tag tagname=NULL
tdSql.query(f'show tags from st_geometry_0
tdSql.checkData(0, 5, None )

tdSql.execute(f'alter table st_geometry_1 set tag tagname=NULL
tdSql.query(f'show tags from st_geometry_1
tdSql.checkData(0, 5, None )

tdSql.execute(f'alter table st_geometry_2 set tag tagname='NULL'
tdSql.query(f'show tags from st_geometry_2
tdSql.checkData(0, 5, None )

tdSql.execute(f'alter table st_geometry_3 set tag tagname='NULL'
tdSql.query(f'show tags from st_geometry_3
tdSql.checkData(0, 5, None )

tdSql.execute(f'alter table st_geometry_4 set tag tagname="NULL"
tdSql.query(f'show tags from st_geometry_4
tdSql.checkData(0, 5, None )

tdSql.execute(f'alter table st_geometry_5 set tag tagname="NULL"
tdSql.query(f'show tags from st_geometry_5
tdSql.checkData(0, 5, None )

tdSql.execute(f'alter table st_geometry_6 set tag tagname="POINT(1.0 1.0)"
tdSql.query(f'show tags from st_geometry_6
tdSql.checkData(0, 5,  @POINT (1.000000 1.000000)@ )

tdSql.execute(f'alter table st_geometry_7 set tag tagname=" LINESTRING(1.0 1.0, 2.0 2.0)"
tdSql.query(f'show tags from st_geometry_7
tdSql.checkData(0, 5,  @LINESTRING (1.000000 1.000000, 2.000000 2.000000)@ )

tdSql.execute(f'alter table st_geometry_8 set tag tagname="POLYGON((1.0 1.0, -2.0 +2.0, 1.0 1.0))"
tdSql.query(f'show tags from st_geometry_8
tdSql.checkData(0, 5,  @POLYGON ((1.000000 1.000000, -2.000000 2.000000, 1.000000 1.000000))@ )



    def test_illegal_input(self):
        """illegal input

        1. 使用 uint 作为超级表的标签列
        2. 使用非法标签值创建子表

        Catalog:
            - DataTypes:UInt

        Since: v3.0.0.0
        Labels: common,ci
        Jira: None

        History:
            - 2025-4-15 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 4: illegal input")

sql_error create table st_geometry_206  using mt_geometry tags(+0123)
sql_error create table st_geometry_207  using mt_geometry tags(-01.23)
sql_error create table st_geometry_208  using mt_geometry tags(+0x01)
sql_error create table st_geometry_209  using mt_geometry tags(-0b01)
sql_error create table st_geometry_2010  using mt_geometry tags(-0.1e-10)
sql_error create table st_geometry_2011  using mt_geometry tags(+0.1E+2)
sql_error create table st_geometry_2012  using mt_geometry tags(tRue)
sql_error create table st_geometry_2013  using mt_geometry tags(FalsE)
sql_error create table st_geometry_2014  using mt_geometry tags(noW)
sql_error create table st_geometry_2015  using mt_geometry tags(toDay)
sql_error insert into st_geometry_206  using mt_geometry tags(+0123) values(now, NULL);
sql_error insert into st_geometry_207  using mt_geometry tags(-01.23) values(now, NULL);
sql_error insert into st_geometry_208  using mt_geometry tags(+0x01) values(now, NULL);
sql_error insert into st_geometry_209  using mt_geometry tags(-0b01) values(now, NULL);
sql_error insert into st_geometry_2010  using mt_geometry tags(-0.1e-10) values(now, NULL);
sql_error insert into st_geometry_2011  using mt_geometry tags(+0.1E+2) values(now, NULL);
sql_error insert into st_geometry_2012  using mt_geometry tags(tRue) values(now, NULL);
sql_error insert into st_geometry_2013  using mt_geometry tags(FalsE) values(now, NULL);
sql_error insert into st_geometry_2014  using mt_geometry tags(noW) values(now, NULL);
sql_error insert into st_geometry_2015  using mt_geometry tags(toDay) values(now, NULL);
sql_error insert into st_geometry_106  using mt_varbinary tags(NULL) values(now(), +0123)
sql_error insert into st_geometry_107  using mt_varbinary tags(NULL) values(now(), -01.23)
sql_error insert into st_geometry_108  using mt_varbinary tags(NULL) values(now(), +0x01)
sql_error insert into st_geometry_109  using mt_varbinary tags(NULL) values(now(), -0b01)
sql_error insert into st_geometry_1010  using mt_varbinary tags(NULL) values(now(), -0.1e-10)
sql_error insert into st_geometry_1011  using mt_varbinary tags(NULL) values(now(), +0.1E+2)
sql_error insert into st_geometry_1012  using mt_varbinary tags(NULL) values(now(), tRue)
sql_error insert into st_geometry_1013  using mt_varbinary tags(NULL) values(now(), FalsE)
sql_error insert into st_geometry_1014  using mt_varbinary tags(NULL) values(now(), noW)
sql_error insert into st_geometry_1015  using mt_varbinary tags(NULL) values(now(), toDay)