from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatatypeBlob:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_datatype_blob(self):
        """blob

        1. create table
        2. insert data
        3. auto create table
        4. alter tag value
        5. illegal input

        Catalog:
            - DataTypes
            - Tables:SubTables:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
           - 2025-7-27 yhDeng create 

        """
        self.check_limit() 
        self.create_table()
        self.insert_data()
        self.auto_create_table()
        self.alter_tag_value()
        self.illegal_input()
        self.clearup_data()

    def check_limit(self):  
        tdLog.info(f"create super table")
        tdSql.error(
            f"create table mt_blob_t (ts timestamp, c blob, c2 blob) tags(tagname varbinary(50))"
        )

        tdSql.error(
            f"create table mt_blob_t (ts timestamp, c int) tags(tagname blob)"
        )
        tdSql.execute(
            f"create table mt_blob_t (ts timestamp, c int) tags(t int)"
        )
        tdSql.execute(
            f"alter table mt_blob_t Add column b1 blob"
        )

        tdSql.error(
            f"alter table mt_blob_t Add column b2 blob"
        )

        tdSql.execute(
            f"alter table mt_blob_t drop column b1"
        )

        tdSql.execute(
            f"alter table mt_blob_t add column b1 blob"
        )
       
    def create_table(self):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table mt_blob (ts timestamp, c blob) tags(tagname varbinary(50))"
        )
        tdSql.error(
            f"ALTER TABLE mt_blob ADD COLUMN c1 BLOB"
        )

        tdLog.info(f"case 0: static create table for test tag values")

        tdSql.execute(f"create table st_varbinary_0 using mt_blob tags(NULL)")
        tdSql.query(f"show tags from st_varbinary_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_varbinary_1 using mt_blob tags(NULL)")
        tdSql.query(f"show tags from st_varbinary_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_varbinary_2 using mt_blob tags('NULL')")
        tdSql.query(f"show tags from st_varbinary_2")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f"create table st_varbinary_3 using mt_blob tags('NULL')")
        tdSql.query(f"show tags from st_varbinary_3")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'create table st_varbinary_4 using mt_blob tags("NULL")')
        tdSql.query(f"show tags from st_varbinary_4")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'create table st_varbinary_5 using mt_blob tags("NULL")')
        tdSql.query(f"show tags from st_varbinary_5")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'create table st_varbinary_6 using mt_blob tags("")')
        tdSql.query(f"show tags from st_varbinary_6")
        tdSql.checkData(0, 5, "\\x")

        tdSql.execute(f'create table st_varbinary_7 using mt_blob tags(" ")')
        tdSql.query(f"show tags from st_varbinary_7")
        tdSql.checkData(0, 5, "\\x20")

        str = "\\x"
        tdSql.execute(f'create table st_varbinary_8 using mt_blob tags("{str}")')
        tdSql.query(f"show tags from st_varbinary_8")
        tdSql.checkData(0, 5, "\\x")

        tdSql.execute(f'create table st_varbinary_9 using mt_blob tags("{str}aB")')
        tdSql.query(f"show tags from st_varbinary_9")
        tdSql.checkData(0, 5, "\\xAB")

        tdSql.execute(f'create table st_varbinary_10 using mt_blob tags("aB")')
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
        # tdSql.checkData(0, 1, '\\x4E554C4C')

        tdSql.execute(f"insert into st_varbinary_3 values(now, 'NULL')")
        tdSql.query(f"select * from st_varbinary_3")
        # tdSql.checkData(0, 1, '\\x4E554C4C')

        tdSql.execute(f'insert into st_varbinary_4 values(now, "NULL")')
        tdSql.query(f"select * from st_varbinary_4")
        # tdSql.checkData(0, 1, '\\x4E554C4C')

        tdSql.execute(f'insert into st_varbinary_5 values(now, "NULL")')
        tdSql.query(f"select * from st_varbinary_5")
        # tdSql.checkData(0, 1, '\\x4E554C4C')

        tdSql.execute(f'insert into st_varbinary_6 values(now, "")')
        tdSql.query(f"select * from st_varbinary_6")
        # tdSql.checkData(0, 1, '\\x')

        tdSql.execute(f'insert into st_varbinary_7 values(now, " ")')
        tdSql.query(f"select * from st_varbinary_7")
        # tdSql.checkData(0, 1, '\\x20')

        str = "\\x"
        tdSql.execute(f'insert into st_varbinary_8 values(now, "{str}")')
        tdSql.query(f"select * from st_varbinary_8")
        # tdSql.checkData(0, 1, '\\x')

        tdSql.execute(f'insert into st_varbinary_9 values(now, "{str}aB")')
        tdSql.query(f"select * from st_varbinary_9")
        # tdSql.checkData(0, 1, '\\xAB')

        tdSql.execute(f'insert into st_varbinary_10 values(now, "aB")')
        tdSql.query(f"select * from st_varbinary_10")
        # tdSql.checkData(0, 1, '\\x6142')

    def auto_create_table(self):
        tdLog.info(f"case 2: dynamic create table for test tag values")

        tdSql.execute(
            f"insert into st_varbinary_100 using mt_blob tags(NULL) values(now,NULL)"
        )
        tdSql.query(f"show tags from st_varbinary_100")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_varbinary_100")
        # tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_varbinary_101 using mt_blob tags(NULL) values(now,NULL)"
        )
        tdSql.query(f"show tags from st_varbinary_101")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_varbinary_101")
        # tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_varbinary_102 using mt_blob tags('NULL') values(now,'NULL')"
        )
        tdSql.query(f"show tags from st_varbinary_102")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.query(f"select * from st_varbinary_102")
        # tdSql.checkData(0, 1, '\\x4E554C4C')

        tdSql.execute(
            f"insert into st_varbinary_103 using mt_blob tags('NULL') values(now,'NULL')"
        )
        tdSql.query(f"show tags from st_varbinary_103")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.query(f"select * from st_varbinary_103")
        # tdSql.checkData(0, 1, '\\x4E554C4C')

        tdSql.execute(
            f'insert into st_varbinary_104 using mt_blob tags("NULL") values(now,"NULL")'
        )
        tdSql.query(f"show tags from st_varbinary_104")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.query(f"select * from st_varbinary_104")
        # tdSql.checkData(0, 1, '\\x4E554C4C')

        tdSql.execute(
            f'insert into st_varbinary_105 using mt_blob tags("NULL") values(now,"NULL")'
        )
        tdSql.query(f"show tags from st_varbinary_105")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.query(f"select * from st_varbinary_105")
        # tdSql.checkData(0, 1, '\\x4E554C4C')

        tdSql.execute(
            f'insert into st_varbinary_106 using mt_blob tags("") values(now,"")'
        )
        tdSql.query(f"show tags from st_varbinary_106")
        tdSql.checkData(0, 5, "\\x")

        tdSql.query(f"select * from st_varbinary_106")
        # tdSql.checkData(0, 1, '\\x')

        tdSql.execute(
            f'insert into st_varbinary_107 using mt_blob tags(" ") values(now," ")'
        )
        tdSql.query(f"show tags from st_varbinary_107")
        tdSql.checkData(0, 5, "\\x20")

        tdSql.query(f"select * from st_varbinary_107")
        # tdSql.checkData(0, 1, '\\x20')

        str = "\\x"
        tdSql.execute(
            f'insert into st_varbinary_108 using mt_blob tags("{str}") values(now,"{str}")'
        )
        tdSql.query(f"show tags from st_varbinary_108")
        tdSql.checkData(0, 5, "\\x")

        tdSql.query(f"select * from st_varbinary_108")
        # tdSql.checkData(0, 1, '\\x')

        tdSql.execute(
            f'insert into st_varbinary_109 using mt_blob tags("{str}aB") values(now,"{str}aB")'
        )
        tdSql.query(f"show tags from st_varbinary_109")
        tdSql.checkData(0, 5, "\\xAB")

        tdSql.query(f"select * from st_varbinary_109")
        # tdSql.checkData(0, 1, '\\xAB')

        tdSql.execute(
            f'insert into st_varbinary_1010 using mt_blob tags("aB") values(now,"aB")'
        )
        tdSql.query(f"show tags from st_varbinary_1010")
        tdSql.checkData(0, 5, "\\x6142")

        tdSql.query(f"select * from st_varbinary_1010")
        # tdSql.checkData(0, 1, '\\x6142')

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

        tdSql.error(f"create table st_varbinary_106 using mt_blob tags(+0123)")
        tdSql.error(f"create table st_varbinary_107 using mt_blob tags(-01.23)")
        tdSql.error(f"create table st_varbinary_108 using mt_blob tags(+0x01)")
        tdSql.error(f"create table st_varbinary_109 using mt_blob tags(-0b01)")
        tdSql.error(f"create table st_varbinary_1010 using mt_blob tags(-0.1e-10)")
        tdSql.error(f"create table st_varbinary_1011 using mt_blob tags(+0.1E+2)")
        tdSql.error(f"create table st_varbinary_1012 using mt_blob tags(tRue)")
        tdSql.error(f"create table st_varbinary_1013 using mt_blob tags(FalsE)")
        tdSql.error(f"create table st_varbinary_1014 using mt_blob tags(noW)")
        tdSql.error(f"create table st_varbinary_1015 using mt_blob tags(toDay)")
        tdSql.error(f"create table st_varbinary_1016 using mt_blob tags(now()+1s)")
        tdSql.error(f"create table st_varbinary_1017 using mt_blob tags(1+1s)")
        tdSql.error(
            f"insert into st_varbinary_106 using mt_blob tags(+0123) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_107 using mt_blob tags(-01.23) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_108 using mt_blob tags(+0x01) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_109 using mt_blob tags(-0b01) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1010 using mt_blob tags(-0.1e-10) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1011 using mt_blob tags(+0.1E+2) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1012 using mt_blob tags(tRue) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1013 using mt_blob tags(FalsE) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1014 using mt_blob tags(noW) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1015 using mt_blob tags(toDay) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1016 using mt_blob tags(now()+1s) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_1017 using mt_blob tags(1+1s) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_varbinary_106 using mt_blob tags(NULL) values(now(), +0123)"
        )
        tdSql.error(
            f"insert into st_varbinary_107 using mt_blob tags(NULL) values(now(), -01.23)"
        )
        tdSql.error(
            f"insert into st_varbinary_108 using mt_blob tags(NULL) values(now(), +0x01)"
        )
        tdSql.error(
            f"insert into st_varbinary_109 using mt_blob tags(NULL) values(now(), -0b01)"
        )
        tdSql.error(
            f"insert into st_varbinary_1010 using mt_blob tags(NULL) values(now(), -0.1e-10)"
        )
        tdSql.error(
            f"insert into st_varbinary_1011 using mt_blob tags(NULL) values(now(), +0.1E+2)"
        )
        tdSql.error(
            f"insert into st_varbinary_1012 using mt_blob tags(NULL) values(now(), tRue)"
        )
        tdSql.error(
            f"insert into st_varbinary_1013 using mt_blob tags(NULL) values(now(), FalsE)"
        )
        tdSql.error(
            f"insert into st_varbinary_1014 using mt_blob tags(NULL) values(now(), noW)"
        )
        tdSql.error(
            f"insert into st_varbinary_1015 using mt_blob tags(NULL) values(now(), toDay)"
        )
        tdSql.error(
            f"insert into st_varbinary_1016 using mt_blob tags(NULL) values(now(), now()+1s)"
        )
        tdSql.error(
            f"insert into st_varbinary_1017 using mt_blob tags(NULL) values(now(), 1+1s)"
        )
    def clearup_data(self):
        tdSql.execute(f"drop database if exists db ")