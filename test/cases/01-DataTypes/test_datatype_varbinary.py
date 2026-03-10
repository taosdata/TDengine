from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatatypeVarbinary:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_datatype_varbinary(self):
        """DataTypes: varbinary

        1. Create table
        2. Insert data
        3. Auto-create table
        4. Alter tag value
        5. Handle illegal input

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_varbinary.sim

        """
        self.create_table()
        self.insert_data()
        self.auto_create_table()
        self.alter_tag_value()
        self.illegal_input()

    def create_table(self):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table mt_varbinary (ts timestamp, c varbinary(50)) tags(tagname varbinary(50))"
        )

        tdLog.info(f"case 0: static create table for test tag values")

        tdSql.execute(f"create table st_varbinary_0 using mt_varbinary tags(NULL)")
        tdSql.query(f"show tags from st_varbinary_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_varbinary_1 using mt_varbinary tags(NULL)")
        tdSql.query(f"show tags from st_varbinary_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_varbinary_2 using mt_varbinary tags('NULL')")
        tdSql.query(f"show tags from st_varbinary_2")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f"create table st_varbinary_3 using mt_varbinary tags('NULL')")
        tdSql.query(f"show tags from st_varbinary_3")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'create table st_varbinary_4 using mt_varbinary tags("NULL")')
        tdSql.query(f"show tags from st_varbinary_4")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'create table st_varbinary_5 using mt_varbinary tags("NULL")')
        tdSql.query(f"show tags from st_varbinary_5")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'create table st_varbinary_6 using mt_varbinary tags("")')
        tdSql.query(f"show tags from st_varbinary_6")
        tdSql.checkData(0, 5, "\\x")

        tdSql.execute(f'create table st_varbinary_7 using mt_varbinary tags(" ")')
        tdSql.query(f"show tags from st_varbinary_7")
        tdSql.checkData(0, 5, "\\x20")

        str = "\\x"
        tdSql.execute(f'create table st_varbinary_8 using mt_varbinary tags("{str}")')
        tdSql.query(f"show tags from st_varbinary_8")
        tdSql.checkData(0, 5, "\\x")

        tdSql.execute(f'create table st_varbinary_9 using mt_varbinary tags("{str}aB")')
        tdSql.query(f"show tags from st_varbinary_9")
        tdSql.checkData(0, 5, "\\xAB")

        tdSql.execute(f'create table st_varbinary_10 using mt_varbinary tags("aB")')
        tdSql.query(f"show tags from st_varbinary_10")
        tdSql.checkData(0, 5, "\\x6142")

    def insert_data(self):
        tdLog.info(f"case 1: insert values for test column values")

        tdSql.execute(f"insert into st_varbinary_0 values(now, NULL)")
        tdSql.query(f"select * from st_varbinary_0")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_varbinary_1 values(now, NULL)")
        tdSql.query(f"select * from st_varbinary_1")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_varbinary_2 values(now, 'NULL')")
        tdSql.query(f"select * from st_varbinary_2")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(f"insert into st_varbinary_3 values(now, 'NULL')")
        tdSql.query(f"select * from st_varbinary_3")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(f'insert into st_varbinary_4 values(now, "NULL")')
        tdSql.query(f"select * from st_varbinary_4")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(f'insert into st_varbinary_5 values(now, "NULL")')
        tdSql.query(f"select * from st_varbinary_5")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(f'insert into st_varbinary_6 values(now, "")')
        tdSql.query(f"select * from st_varbinary_6")
        tdSql.checkData(0, 1, bytes.fromhex(''))

        tdSql.execute(f'insert into st_varbinary_7 values(now, " ")')
        tdSql.query(f"select * from st_varbinary_7")
        tdSql.checkData(0, 1, bytes.fromhex('20'))

        str = "\\x"
        tdSql.execute(f'insert into st_varbinary_8 values(now, "{str}")')
        tdSql.query(f"select * from st_varbinary_8")
        tdSql.checkData(0, 1, bytes.fromhex(''))

        tdSql.execute(f'insert into st_varbinary_9 values(now, "{str}aB")')
        tdSql.query(f"select * from st_varbinary_9")
        tdSql.checkData(0, 1, bytes.fromhex('AB'))

        tdSql.execute(f'insert into st_varbinary_10 values(now, "aB")')
        tdSql.query(f"select * from st_varbinary_10")
        tdSql.checkData(0, 1, bytes.fromhex('6142'))

    def auto_create_table(self):
        tdLog.info(f"case 2: dynamic create table for test tag values")

        tdSql.execute(
            f"insert into st_varbinary_100 using mt_varbinary tags(NULL) values(now,NULL)"
        )
        tdSql.query(f"show tags from st_varbinary_100")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_varbinary_100")
        # tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_varbinary_101 using mt_varbinary tags(NULL) values(now,NULL)"
        )
        tdSql.query(f"show tags from st_varbinary_101")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_varbinary_101")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_varbinary_102 using mt_varbinary tags('NULL') values(now,'NULL')"
        )
        tdSql.query(f"show tags from st_varbinary_102")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.query(f"select * from st_varbinary_102")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(
            f"insert into st_varbinary_103 using mt_varbinary tags('NULL') values(now,'NULL')"
        )
        tdSql.query(f"show tags from st_varbinary_103")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.query(f"select * from st_varbinary_103")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(
            f'insert into st_varbinary_104 using mt_varbinary tags("NULL") values(now,"NULL")'
        )
        tdSql.query(f"show tags from st_varbinary_104")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.query(f"select * from st_varbinary_104")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(
            f'insert into st_varbinary_105 using mt_varbinary tags("NULL") values(now,"NULL")'
        )
        tdSql.query(f"show tags from st_varbinary_105")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.query(f"select * from st_varbinary_105")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(
            f'insert into st_varbinary_106 using mt_varbinary tags("") values(now,"")'
        )
        tdSql.query(f"show tags from st_varbinary_106")
        tdSql.checkData(0, 5, "\\x")

        tdSql.query(f"select * from st_varbinary_106")
        tdSql.checkData(0, 1, bytes.fromhex(''))

        tdSql.execute(
            f'insert into st_varbinary_107 using mt_varbinary tags(" ") values(now," ")'
        )
        tdSql.query(f"show tags from st_varbinary_107")
        tdSql.checkData(0, 5, "\\x20")

        tdSql.query(f"select * from st_varbinary_107")
        tdSql.checkData(0, 1, bytes.fromhex('20'))

        str = "\\x"
        tdSql.execute(
            f'insert into st_varbinary_108 using mt_varbinary tags("{str}") values(now,"{str}")'
        )
        tdSql.query(f"show tags from st_varbinary_108")
        tdSql.checkData(0, 5, "\\x")

        tdSql.query(f"select * from st_varbinary_108")
        tdSql.checkData(0, 1, bytes.fromhex(''))

        tdSql.execute(
            f'insert into st_varbinary_109 using mt_varbinary tags("{str}aB") values(now,"{str}aB")'
        )
        tdSql.query(f"show tags from st_varbinary_109")
        tdSql.checkData(0, 5, "\\xAB")

        tdSql.query(f"select * from st_varbinary_109")
        tdSql.checkData(0, 1, bytes.fromhex('AB'))

        tdSql.execute(
            f'insert into st_varbinary_1010 using mt_varbinary tags("aB") values(now,"aB")'
        )
        tdSql.query(f"show tags from st_varbinary_1010")
        tdSql.checkData(0, 5, "\\x6142")

        tdSql.query(f"select * from st_varbinary_1010")
        tdSql.checkData(0, 1, bytes.fromhex('6142'))

    def alter_tag_value(self):
        tdLog.info(f"case 3: alter tag value")

        tdSql.execute(f"alter table st_varbinary_100  set tag tagname=NULL")
        tdSql.query(f"show tags from st_varbinary_100")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_varbinary_101  set tag tagname=NULL")
        tdSql.query(f"show tags from st_varbinary_101")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_varbinary_102  set tag tagname='NULL'")
        tdSql.query(f"show tags from st_varbinary_102")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f"alter table st_varbinary_103  set tag tagname='NULL'")
        tdSql.query(f"show tags from st_varbinary_103")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'alter table st_varbinary_104  set tag tagname="NULL"')
        tdSql.query(f"show tags from st_varbinary_104")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'alter table st_varbinary_105  set tag tagname="NULL"')
        tdSql.query(f"show tags from st_varbinary_105")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'alter table st_varbinary_106  set tag tagname=""')
        tdSql.query(f"show tags from st_varbinary_106")
        tdSql.checkData(0, 5, "\\x")

        tdSql.execute(f'alter table st_varbinary_107  set tag tagname=" "')
        tdSql.query(f"show tags from st_varbinary_107")
        tdSql.checkData(0, 5, "\\x20")

        str = "\\x"
        tdSql.execute(f'alter table st_varbinary_108  set tag tagname="{str}"')
        tdSql.query(f"show tags from st_varbinary_108")
        tdSql.checkData(0, 5, "\\x")

        tdSql.execute(f'alter table st_varbinary_109  set tag tagname="{str}aB"')
        tdSql.query(f"show tags from st_varbinary_109")
        tdSql.checkData(0, 5, "\\xAB")

        tdSql.execute(f'alter table st_varbinary_1010  set tag tagname="aB"')
        tdSql.query(f"show tags from st_varbinary_1010")
        tdSql.checkData(0, 5, "\\x6142")

    def illegal_input(self):
        tdLog.info(f"case 4: illegal input")

        tdSql.error(f"create table st_varbinary_106 using mt_varbinary tags(+0123)")
        tdSql.error(f"create table st_varbinary_107 using mt_varbinary tags(-01.23)")
        tdSql.error(f"create table st_varbinary_108 using mt_varbinary tags(+0x01)")
        tdSql.error(f"create table st_varbinary_109 using mt_varbinary tags(-0b01)")
        tdSql.error(f"create table st_varbinary_1010 using mt_varbinary tags(-0.1e-10)")
        tdSql.error(f"create table st_varbinary_1011 using mt_varbinary tags(+0.1E+2)")
        tdSql.error(f"create table st_varbinary_1012 using mt_varbinary tags(tRue)")
        tdSql.error(f"create table st_varbinary_1013 using mt_varbinary tags(FalsE)")
        tdSql.error(f"create table st_varbinary_1014 using mt_varbinary tags(noW)")
        tdSql.error(f"create table st_varbinary_1015 using mt_varbinary tags(toDay)")
        tdSql.error(f"create table st_varbinary_1016 using mt_varbinary tags(now()+1s)")
        tdSql.error(f"create table st_varbinary_1017 using mt_varbinary tags(1+1s)")
        tdSql.error(
            f"insert into st_varbinary_106 using mt_varbinary tags(+0123) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_107 using mt_varbinary tags(-01.23) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_108 using mt_varbinary tags(+0x01) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_109 using mt_varbinary tags(-0b01) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1010 using mt_varbinary tags(-0.1e-10) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1011 using mt_varbinary tags(+0.1E+2) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1012 using mt_varbinary tags(tRue) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1013 using mt_varbinary tags(FalsE) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1014 using mt_varbinary tags(noW) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1015 using mt_varbinary tags(toDay) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1016 using mt_varbinary tags(now()+1s) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1017 using mt_varbinary tags(1+1s) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_106 using mt_varbinary tags(NULL) values(now(), +0123)"
        )
        tdSql.error(
            f"insert into st_varbinary_107 using mt_varbinary tags(NULL) values(now(), -01.23)"
        )
        tdSql.error(
            f"insert into st_varbinary_108 using mt_varbinary tags(NULL) values(now(), +0x01)"
        )
        tdSql.error(
            f"insert into st_varbinary_109 using mt_varbinary tags(NULL) values(now(), -0b01)"
        )
        tdSql.error(
            f"insert into st_varbinary_1010 using mt_varbinary tags(NULL) values(now(), -0.1e-10)"
        )
        tdSql.error(
            f"insert into st_varbinary_1011 using mt_varbinary tags(NULL) values(now(), +0.1E+2)"
        )
        tdSql.error(
            f"insert into st_varbinary_1012 using mt_varbinary tags(NULL) values(now(), tRue)"
        )
        tdSql.error(
            f"insert into st_varbinary_1013 using mt_varbinary tags(NULL) values(now(), FalsE)"
        )
        tdSql.error(
            f"insert into st_varbinary_1014 using mt_varbinary tags(NULL) values(now(), noW)"
        )
        tdSql.error(
            f"insert into st_varbinary_1015 using mt_varbinary tags(NULL) values(now(), toDay)"
        )
        tdSql.error(
            f"insert into st_varbinary_1016 using mt_varbinary tags(NULL) values(now(), now()+1s)"
        )
        tdSql.error(
            f"insert into st_varbinary_1017 using mt_varbinary tags(NULL) values(now(), 1+1s)"
        )
