from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatatypeVarchar:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_datatype_varchar(self):
        """DataTypes: varchar

        1. Create table
        2. Insert data
        3. Auto-create table
        4. Alter tag value
        5. Handle illegal input
        6. Query with varchar values
        7. Cast on varchar

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_varchar.sim
            - 2025-12-21 Alex Duan Migrated from uncatalog/system-test/2-query/test_varchar.py

        """
        self.create_table()
        self.insert_data()
        self.auto_create_table()
        self.alter_tag_value()
        self.illegal_input()
        self.do_varchar_value()

    def create_table(self):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table mt_varchar (ts timestamp, c varchar(50)) tags(tagname varchar(50))"
        )

        tdLog.info(f"case 0: static create table for test tag values")
        tdSql.execute(f"create table st_varchar_0 using mt_varchar tags(NULL)")
        tdSql.query(f"show create table st_varchar_0")
        tdSql.query(f"show tags from st_varchar_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_varchar_1 using mt_varchar tags(NULL)")
        tdSql.query(f"show tags from st_varchar_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_varchar_2 using mt_varchar tags('NULL')")
        tdSql.query(f"show tags from st_varchar_2")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f"create table st_varchar_3 using mt_varchar tags('NULL')")
        tdSql.query(f"show tags from st_varchar_3")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f'create table st_varchar_4 using mt_varchar tags("NULL")')
        tdSql.query(f"show tags from st_varchar_4")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f'create table st_varchar_5 using mt_varchar tags("NULL")')
        tdSql.query(f"show tags from st_varchar_5")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f"create table st_varchar_6 using mt_varchar tags(+0123)")
        tdSql.query(f"show tags from st_varchar_6")
        tdSql.checkData(0, 5, "+0123")

        tdSql.execute(f"create table st_varchar_7 using mt_varchar tags(-01.23)")
        tdSql.query(f"show tags from st_varchar_7")
        tdSql.checkData(0, 5, "-01.23")

        tdSql.execute(f"create table st_varchar_8 using mt_varchar tags(+0x01)")
        tdSql.query(f"show tags from st_varchar_8")
        tdSql.checkData(0, 5, "+0x01")

        tdSql.execute(f"create table st_varchar_9 using mt_varchar tags(-0b01)")
        tdSql.query(f"show tags from st_varchar_9")
        tdSql.checkData(0, 5, "-0b01")

        tdSql.execute(f"create table st_varchar_10 using mt_varchar tags(-0.1e-10)")
        tdSql.query(f"show tags from st_varchar_10")
        tdSql.checkData(0, 5, "-0.1e-10")

        tdSql.execute(f"create table st_varchar_11 using mt_varchar tags(+0.1E+2)")
        tdSql.query(f"show tags from st_varchar_11")
        # tdSql.checkData(0, 5, "+0")

        tdSql.execute(f"create table st_varchar_12 using mt_varchar tags(tRue)")
        tdSql.query(f"show tags from st_varchar_12")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_varchar_13 using mt_varchar tags(FalsE)")
        tdSql.query(f"show tags from st_varchar_13")
        tdSql.checkData(0, 5, "false")

        tdSql.execute(f"create table st_varchar_14 using mt_varchar tags(noW)")
        tdSql.query(f"show tags from st_varchar_14")
        tdSql.checkData(0, 5, "now")

        tdSql.execute(f"create table st_varchar_15 using mt_varchar tags(toDay)")
        tdSql.query(f"show tags from st_varchar_15")
        tdSql.checkData(0, 5, "today")

    def insert_data(self):
        tdLog.info(f"case 1: insert values for test column values")

        tdSql.execute(f"insert into st_varchar_0 values(now, NULL)")
        tdSql.query(f"select * from st_varchar_0")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_varchar_1 values(now, NULL)")
        tdSql.query(f"select * from st_varchar_1")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_varchar_2 values(now, 'NULL')")
        tdSql.query(f"select * from st_varchar_2")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(f"insert into st_varchar_3 values(now, 'NULL')")
        tdSql.query(f"select * from st_varchar_3")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(f'insert into st_varchar_4 values(now, "NULL")')
        tdSql.query(f"select * from st_varchar_4")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(f'insert into st_varchar_5 values(now, "NULL")')
        tdSql.query(f"select * from st_varchar_5")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(f"insert into st_varchar_6 values(now, +0123)")
        tdSql.query(f"select * from st_varchar_6")
        tdSql.checkData(0, 1, "+0123")

        tdSql.execute(f"insert into st_varchar_7 values(now, -01.23)")
        tdSql.query(f"select * from st_varchar_7")
        tdSql.checkData(0, 1, "-01.23")

        tdSql.execute(f"insert into st_varchar_8 values(now, +0x01)")
        tdSql.query(f"select * from st_varchar_8")
        tdSql.checkData(0, 1, "+0x01")

        tdSql.execute(f"insert into st_varchar_9 values(now, -0b01)")
        tdSql.query(f"select * from st_varchar_9")
        tdSql.checkData(0, 1, "-0b01")

        tdSql.execute(f"insert into st_varchar_10 values(now, -0.1e-10)")
        tdSql.query(f"select * from st_varchar_10")
        tdSql.checkData(0, 1, "-0.1e-10")

        tdSql.execute(f"insert into st_varchar_11 values(now, +0.1E+2)")
        tdSql.query(f"select * from st_varchar_11")
        tdSql.checkData(0, 1, "+0.1e+2")

        tdSql.execute(f"insert into st_varchar_12 values(now, tRue)")
        tdSql.query(f"select * from st_varchar_12")
        tdSql.checkData(0, 1, "true")

        tdSql.execute(f"insert into st_varchar_13 values(now, FalsE)")
        tdSql.query(f"select * from st_varchar_13")
        tdSql.checkData(0, 1, "false")

        tdSql.execute(f"insert into st_varchar_14 values(now, noW)")
        tdSql.query(f"select * from st_varchar_14")
        tdSql.checkData(0, 1, "now")

        tdSql.execute(f"insert into st_varchar_15 values(now, toDay)")
        tdSql.query(f"select * from st_varchar_15")
        tdSql.checkData(0, 1, "today")

    def auto_create_table(self):
        tdLog.info(f"case 2: dynamic create table for test tag values")
        tdSql.execute(
            f"insert into st_varchar_0 using mt_varchar tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_varchar_0")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_varchar_0")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_varchar_1 using mt_varchar tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_varchar_1")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_varchar_1")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_varchar_2 using mt_varchar tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_varchar_2")
        tdSql.checkData(0, 5, "NULL")

        tdSql.query(f"select * from st_varchar_2")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(
            f"insert into st_varchar_3 using mt_varchar tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_varchar_3")
        tdSql.checkData(0, 5, "NULL")

        tdSql.query(f"select * from st_varchar_3")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(
            f'insert into st_varchar_4 using mt_varchar tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_varchar_4")
        tdSql.checkData(0, 5, "NULL")

        tdSql.query(f"select * from st_varchar_4")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(
            f'insert into st_varchar_5 using mt_varchar tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_varchar_5")
        tdSql.checkData(0, 5, "NULL")

        tdSql.query(f"select * from st_varchar_5")
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(
            f"insert into st_varchar_6 using mt_varchar tags(+0123) values(now, +0123)"
        )
        tdSql.query(f"show tags from st_varchar_6")
        tdSql.checkData(0, 5, "+0123")

        tdSql.query(f"select * from st_varchar_6")
        tdSql.checkData(0, 1, "+0123")

        tdSql.execute(
            f"insert into st_varchar_7 using mt_varchar tags(-01.23) values(now, -01.23)"
        )
        tdSql.query(f"show tags from st_varchar_7")
        tdSql.checkData(0, 5, "-01.23")

        tdSql.query(f"select * from st_varchar_7")
        tdSql.checkData(0, 1, "-01.23")

        tdSql.execute(
            f"insert into st_varchar_8 using mt_varchar tags(+0x01) values(now, +0x01)"
        )
        tdSql.query(f"show tags from st_varchar_8")
        tdSql.checkData(0, 5, "+0x01")

        tdSql.query(f"select * from st_varchar_8")
        tdSql.checkData(0, 1, "+0x01")

        tdSql.execute(
            f"insert into st_varchar_9 using mt_varchar tags(-0b01) values(now, -0b01)"
        )
        tdSql.query(f"show tags from st_varchar_9")
        tdSql.checkData(0, 5, "-0b01")

        tdSql.query(f"select * from st_varchar_9")
        tdSql.checkData(0, 1, "-0b01")

        tdSql.execute(
            f"insert into st_varchar_10 using mt_varchar tags(-0.1e-10) values(now, -0.1e-10)"
        )
        tdSql.query(f"show tags from st_varchar_10")
        # tdSql.checkData(0, 5, -0)

        tdSql.query(f"select * from st_varchar_10")
        # tdSql.checkData(0, 1, -0)

        tdSql.execute(
            f"insert into st_varchar_11 using mt_varchar tags(+0.1E+2) values(now, +0.1E+2)"
        )
        tdSql.query(f"show tags from st_varchar_11")
        # tdSql.checkData(0, 5, +0)

        tdSql.query(f"select * from st_varchar_11")
        # tdSql.checkData(0, 1, +0)

        tdSql.execute(
            f"insert into st_varchar_12 using mt_varchar tags(tRue) values(now, tRue)"
        )
        tdSql.query(f"show tags from st_varchar_12")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_varchar_12")
        tdSql.checkData(0, 1, "true")

        tdSql.execute(
            f"insert into st_varchar_13 using mt_varchar tags(FalsE) values(now, FalsE)"
        )
        tdSql.query(f"show tags from st_varchar_13")
        tdSql.checkData(0, 5, "false")

        tdSql.query(f"select * from st_varchar_13")
        tdSql.checkData(0, 1, "false")

        tdSql.execute(
            f"insert into st_varchar_14 using mt_varchar tags(noW) values(now, noW)"
        )
        tdSql.query(f"show tags from st_varchar_14")
        tdSql.checkData(0, 5, "now")

        tdSql.query(f"select * from st_varchar_14")
        tdSql.checkData(0, 1, "now")

        tdSql.execute(
            f"insert into st_varchar_15 using mt_varchar tags(toDay) values(now, toDay)"
        )
        tdSql.query(f"show tags from st_varchar_15")
        tdSql.checkData(0, 5, "today")

        tdSql.query(f"select * from st_varchar_15")
        tdSql.checkData(0, 1, "today")

    def alter_tag_value(self):
        tdLog.info(f"case 3: alter tag value")

        tdSql.execute(f"alter table st_varchar_0  set tag tagname=NULL")
        tdSql.query(f"show tags from st_varchar_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_varchar_1  set tag tagname=NULL")
        tdSql.query(f"show tags from st_varchar_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_varchar_2  set tag tagname='NULL'")
        tdSql.query(f"show tags from st_varchar_2")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f"alter table st_varchar_3  set tag tagname='NULL'")
        tdSql.query(f"show tags from st_varchar_3")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f'alter table st_varchar_4  set tag tagname="NULL"')
        tdSql.query(f"show tags from st_varchar_4")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f'alter table st_varchar_5  set tag tagname="NULL"')
        tdSql.query(f"show tags from st_varchar_5")
        tdSql.checkData(0, 5, "NULL")

        tdSql.execute(f"alter table st_varchar_6  set tag tagname=+0123")
        tdSql.query(f"show tags from st_varchar_6")
        tdSql.checkData(0, 5, "+0123")

        tdSql.execute(f"alter table st_varchar_7  set tag tagname=-01.23")
        tdSql.query(f"show tags from st_varchar_7")
        tdSql.checkData(0, 5, "-01.23")

        tdSql.execute(f"alter table st_varchar_8  set tag tagname=+0x01")
        tdSql.query(f"show tags from st_varchar_8")
        tdSql.checkData(0, 5, "+0x01")

        tdSql.execute(f"alter table st_varchar_9  set tag tagname=-0b01")
        tdSql.query(f"show tags from st_varchar_9")
        tdSql.checkData(0, 5, "-0b01")

        tdSql.execute(f"alter table st_varchar_10  set tag tagname=-0.1e-10")
        tdSql.query(f"show tags from st_varchar_10")
        tdSql.checkData(0, 5, "-0.1e-10")

        tdSql.execute(f"alter table st_varchar_11  set tag tagname=+0.1E+2")
        tdSql.query(f"show tags from st_varchar_11")
        # tdSql.checkData(0, 5, "+0")

        tdSql.execute(f"alter table st_varchar_12  set tag tagname=tRue")
        tdSql.query(f"show tags from st_varchar_12")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"alter table st_varchar_13  set tag tagname=FalsE")
        tdSql.query(f"show tags from st_varchar_13")
        tdSql.checkData(0, 5, "false")

        tdSql.execute(f"alter table st_varchar_14  set tag tagname=noW")
        tdSql.query(f"show tags from st_varchar_14")
        tdSql.checkData(0, 5, "now")

        tdSql.execute(f"alter table st_varchar_15  set tag tagname=toDay")
        tdSql.query(f"show tags from st_varchar_15")
        tdSql.checkData(0, 5, "today")

    def illegal_input(self):
        tdSql.error(f"create table st_varchar_100 using mt_varchar tags(now+1d)")
        tdSql.error(f"create table st_varchar_101 using mt_varchar tags(toDay+1d)")
        tdSql.error(f"create table st_varchar_102 using mt_varchar tags(1+1b)")
        tdSql.error(f"create table st_varchar_103 using mt_varchar tags(0x01+1d)")
        tdSql.error(f"create table st_varchar_104 using mt_varchar tags(0b01+1s)")
        tdSql.error(
            f"insert into st_varchar_1100 using mt_varchar tags('now') values(now(),now+1d)"
        )
        tdSql.error(
            f"insert into st_varchar_1101 using mt_varchar tags('now') values(now(),toDay+1d)"
        )
        tdSql.error(
            f"insert into st_varchar_1102 using mt_varchar tags('now') values(now(),1+1b)"
        )
        tdSql.error(
            f"insert into st_varchar_1103 using mt_varchar tags('now') values(now(),0x01+1d)"
        )
        tdSql.error(
            f"insert into st_varchar_1104 using mt_varchar tags('now') values(now(),0b01+1s)"
        )
        tdSql.error(f"alter table st_varchar_15  set tag tagname=now()+1d")

    #
    # ------------------- test_varchar.py ----------------
    #
    def do_varchar_value(self):
        dbname = "db"
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 varchar(16),c9 nchar(32), c10 timestamp)
            tags (t1 int)
            '''
        )
        tdSql.execute(
            f'''
            create table {dbname}.t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 varchar(16),c9 nchar(32), c10 timestamp)
            '''
        )
        sql = "create table "
        for i in range(4):
            sql += f'{dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )'
        tdSql.execute(sql)

        tdLog.printNoPrefix("==========step2:insert data")
        for i in range(9):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'varchar{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'varchar{i}', 'nchar{i}', now()+{1*i}a )"
            )

        tdSql.execute(f"insert into {dbname}.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'varchar0', 'nchar0', now()+8a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'varchar9', 'nchar9', now()+9a )")

        tdSql.execute(f"insert into {dbname}.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdSql.execute(
            f'''insert into {dbname}.t1 values
            ( '2020-04-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2020-10-21 01:01:01.000', 1, 11111, 111, 11, 1.11, 11.11, 1, "varchar1", "nchar1", now()+1a )
            ( '2020-12-31 01:01:01.000', 2, 22222, 222, 22, 2.22, 22.22, 0, "varchar2", "nchar2", now()+2a )
            ( '2021-01-01 01:01:06.000', 3, 33333, 333, 33, 3.33, 33.33, 0, "varchar3", "nchar3", now()+3a )
            ( '2021-05-07 01:01:10.000', 4, 44444, 444, 44, 4.44, 44.44, 1, "varchar4", "nchar4", now()+4a )
            ( '2021-07-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2021-09-30 01:01:16.000', 5, 55555, 555, 55, 5.55, 55.55, 0, "varchar5", "nchar5", now()+5a )
            ( '2022-02-01 01:01:20.000', 6, 66666, 666, 66, 6.66, 66.66, 1, "varchar6", "nchar6", now()+6a )
            ( '2022-10-28 01:01:26.000', 7, 00000, 000, 00, 0.00, 00.00, 1, "varchar7", "nchar7", "1970-01-01 08:00:00.000" )
            ( '2022-12-01 01:01:30.000', 8, -88888, -888, -88, -8.88, -88.88, 0, "varchar8", "nchar8", "1969-01-01 01:00:00.000" )
            ( '2022-12-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "varchar9 ", "nchar9", "1900-01-01 00:00:00.000" )
            ( '2023-02-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            '''
        )

        data_ct1_c8 = ["varchar8","varchar7","varchar6","varchar5","varchar0","varchar4","varchar3","varchar2","varchar1","varchar0","varchar9"]

        tdLog.printNoPrefix("==========step3: cast on varchar")

        tdSql.query(f"select c8 from {dbname}.ct1")
        for i in range(tdSql.queryRows):
            tdSql.checkData(i,0, data_ct1_c8[i])

        tdSql.error("create stable empty_col_stable(ts timestamp, c2 varchar(0)) tags(tg1 int)")
        tdSql.error("create stable empty_col_stable(ts timestamp, c2 varchar(10)) tags(tg1 varchar(0))")
        tdSql.error("create stable empty_col_stable(ts timestamp, c2 varchar(10)) tags(tg1 nchar(0))")
        tdSql.error("create stable empty_col_stable(ts timestamp, c2 varchar(10)) tags(tg1 binary(0))")
        tdSql.error("create stable empty_col_stable(ts timestamp, c2 varchar(10)) tags(tg1 varbinary(0))")
     