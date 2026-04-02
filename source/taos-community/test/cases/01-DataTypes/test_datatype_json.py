from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatatypeJson:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_datatype_json(self):
        """DataTypes: json

        1. Create table
        2. Insert data
        3. Alter tag value
        4. Handle illegal input

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_json.sim

        """
        self.create_table()
        self.insert_data()
        self.auto_create_table()
        self.alter_tag_value()
        self.illegal_input()

    def create_table(self):
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

    def insert_data(self):
        tdLog.info(f"case 1: insert values for test column values")

    def auto_create_table(self):
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

    def alter_tag_value(self):
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

    def illegal_input(self):
        tdLog.info(f"case 4: illegal input")

        tdSql.error(f"create table st_json_206 using mt_json tags(+0123)")
        tdSql.error(f"create table st_json_207 using mt_json tags(-01.23)")
        tdSql.error(f"create table st_json_208 using mt_json tags(+0x01)")
        tdSql.error(f"create table st_json_209 using mt_json tags(-0b01)")
        tdSql.error(f"create table st_json_2010 using mt_json tags(-0.1e-10)")
        tdSql.error(f"create table st_json_2011 using mt_json tags(+0.1E+2)")
        tdSql.error(f"create table st_json_2012 using mt_json tags(tRue)")
        tdSql.error(f"create table st_json_2013 using mt_json tags(FalsE)")
        tdSql.error(f"create table st_json_2014 using mt_json tags(noW)")
        tdSql.error(f"create table st_json_2015 using mt_json tags(toDay)")
        tdSql.error(
            f"insert into st_json_206 using mt_json tags(+0123) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_json_207 using mt_json tags(-01.23) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_json_208 using mt_json tags(+0x01) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_json_209 using mt_json tags(-0b01) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_json_2010 using mt_json tags(-0.1e-10) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_json_2011 using mt_json tags(+0.1E+2) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_json_2012 using mt_json tags(tRue) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_json_2013 using mt_json tags(FalsE) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_json_2014 using mt_json tags(noW) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_json_2015 using mt_json tags(toDay) values(now, NULL);"
        )
