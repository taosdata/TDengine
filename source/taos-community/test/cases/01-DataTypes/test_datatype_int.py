from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatatypeInt:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_datatype_int(self):
        """DataTypes: int

        1. Create table
        2. Insert data
        3. Auto-create table
        4. Alter tag value
        5. Handle illegal input


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_int.sim

        """
        self.create_table()
        self.insert_data()
        self.auto_create_table()
        self.alter_tag_value()
        self.illegal_input()

    def create_table(self):
        tdLog.info(f"create super table")
        tdSql.execute(f"create table mt_int (ts timestamp, c int) tags(tagname int)")

        tdLog.info(f"case 0: static create table for test tag values")

        tdSql.execute(f"create table st_int_0 using mt_int tags(NULL)")
        tdSql.query(f"show create table st_int_0")
        tdSql.query(f"show tags from st_int_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_int_1 using mt_int tags(NULL)")
        tdSql.query(f"show tags from st_int_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_int_2 using mt_int tags('NULL')")
        tdSql.query(f"show tags from st_int_2")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_int_3 using mt_int tags('NULL')")
        tdSql.query(f"show tags from st_int_3")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_int_4 using mt_int tags("NULL")')
        tdSql.query(f"show tags from st_int_4")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_int_5 using mt_int tags("NULL")')
        tdSql.query(f"show tags from st_int_5")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_int_6 using mt_int tags(-2147483647)")
        tdSql.query(f"show tags from st_int_6")
        tdSql.checkData(0, 5, -2147483647)

        tdSql.execute(f"create table st_int_7 using mt_int tags(2147483647)")
        tdSql.query(f"show tags from st_int_7")
        tdSql.checkData(0, 5, 2147483647)

        tdSql.execute(f"create table st_int_8 using mt_int tags(37)")
        tdSql.query(f"show tags from st_int_8")
        tdSql.checkData(0, 5, 37)

        tdSql.execute(f"create table st_int_9 using mt_int tags(-100)")
        tdSql.query(f"show tags from st_int_9")
        tdSql.checkData(0, 5, -100)

        tdSql.execute(f"create table st_int_10 using mt_int tags(+113)")
        tdSql.query(f"show tags from st_int_10")
        tdSql.checkData(0, 5, 113)

        tdSql.execute(f"create table st_int_11 using mt_int tags('-100')")
        tdSql.query(f"show tags from st_int_11")
        tdSql.checkData(0, 5, -100)

        tdSql.execute(f'create table st_int_12 using mt_int tags("+78")')
        tdSql.query(f"show tags from st_int_12")
        tdSql.checkData(0, 5, 78)

        tdSql.execute(f"create table st_int_13 using mt_int tags(+0078)")
        tdSql.query(f"show tags from st_int_13")
        tdSql.checkData(0, 5, 78)

        tdSql.execute(f"create table st_int_14 using mt_int tags(-00078)")
        tdSql.query(f"show tags from st_int_14")
        tdSql.checkData(0, 5, -78)

        tdSql.execute(f'create table st_int_100 using mt_int tags("0x01")')
        tdSql.query(f"show tags from st_int_100")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'create table st_int_101 using mt_int tags("0b01")')
        tdSql.query(f"show tags from st_int_101")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'create table st_int_102 using mt_int tags("+0x01")')
        tdSql.query(f"show tags from st_int_102")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'create table st_int_103 using mt_int tags("-0b01")')
        tdSql.query(f"show tags from st_int_103")
        tdSql.checkData(0, 5, -1)

        tdSql.execute(f'create table st_int_104 using mt_int tags("-123.1")')
        tdSql.query(f"show tags from st_int_104")
        tdSql.checkData(0, 5, -123)

        tdSql.execute(f'create table st_int_105 using mt_int tags("+123.5")')
        tdSql.query(f"show tags from st_int_105")
        tdSql.checkData(0, 5, 124)

        tdSql.execute(f'create table st_int_106 using mt_int tags("-1e-1")')
        tdSql.query(f"show tags from st_int_106")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f'create table st_int_107 using mt_int tags("+0.1235e3")')
        tdSql.query(f"show tags from st_int_107")
        tdSql.checkData(0, 5, 124)

        tdSql.execute(f'create table st_int_108 using mt_int tags("-0.11e-30")')
        tdSql.query(f"show tags from st_int_108")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f'create table st_int_109 using mt_int tags("-1.1e-307")')
        tdSql.query(f"show tags from st_int_109")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"create table st_int_110 using mt_int tags( -1e-1 )")
        tdSql.query(f"show tags from st_int_110")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"create table st_int_111 using mt_int tags( +0.1235e3 )")
        tdSql.query(f"show tags from st_int_111")
        tdSql.checkData(0, 5, 124)

        tdSql.execute(f"create table st_int_112 using mt_int tags(-0.11e-30)")
        tdSql.query(f"show tags from st_int_112")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"create table st_int_113 using mt_int tags(-1.1e-307)")
        tdSql.query(f"show tags from st_int_113")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"create table st_int_200 using mt_int tags( 0x01)")
        tdSql.query(f"show tags from st_int_200")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"create table st_int_201 using mt_int tags(0b01 )")
        tdSql.query(f"show tags from st_int_201")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"create table st_int_202 using mt_int tags(+0x01)")
        tdSql.query(f"show tags from st_int_202")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"create table st_int_203 using mt_int tags( -0b01 )")
        tdSql.query(f"show tags from st_int_203")
        tdSql.checkData(0, 5, -1)

    def insert_data(self):
        tdLog.info(f"case 1: insert values for test column values")

        tdSql.execute(f"insert into st_int_0 values(now, NULL)")
        tdSql.query(f"select * from st_int_0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_int_1 values(now, NULL)")
        tdSql.query(f"select * from st_int_1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_int_6 values(now, 2147483647)")
        tdSql.query(f"select * from st_int_6")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 2147483647)

        tdSql.execute(f"insert into st_int_7 values(now, -2147483647)")
        tdSql.query(f"select * from st_int_7")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -2147483647)

        tdSql.execute(f"insert into st_int_8 values(now, +100)")
        tdSql.query(f"select * from st_int_8")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 100)

        tdSql.execute(f'insert into st_int_9 values(now, "-098")')
        tdSql.query(f"select * from st_int_9")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -98)

        tdSql.execute(f"insert into st_int_10 values(now, '0')")
        tdSql.query(f"select * from st_int_10")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_int_11 values(now, -0)")
        tdSql.query(f"select * from st_int_11")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f'insert into st_int_12 values(now, "+056")')
        tdSql.query(f"select * from st_int_12")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 56)

        tdSql.execute(f"insert into st_int_13 values(now, +056)")
        tdSql.query(f"select * from st_int_13")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 56)

        tdSql.execute(f"insert into st_int_14 values(now, -056)")
        tdSql.query(f"select * from st_int_14")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -56)

        tdSql.execute(f'insert into st_int_100 values(now, "0x01")')
        tdSql.query(f"select * from st_int_100")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_int_101 values(now, "0b01")')
        tdSql.query(f"select * from st_int_101")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_int_102 values(now, "+0x01")')
        tdSql.query(f"select * from st_int_102")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_int_103 values(now, "-0b01")')
        tdSql.query(f"select * from st_int_103")
        tdSql.checkData(0, 1, -1)

        tdSql.execute(f'insert into st_int_104 values(now, "-123.1")')
        tdSql.query(f"select * from st_int_104")
        tdSql.checkData(0, 1, -123)

        tdSql.execute(f'insert into st_int_105 values(now, "+123.5")')
        tdSql.query(f"select * from st_int_105")
        tdSql.checkData(0, 1, 124)

        tdSql.execute(f'insert into st_int_106 values(now, "-1e-1")')
        tdSql.query(f"select * from st_int_106")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f'insert into st_int_107 values(now, "+0.1235e3")')
        tdSql.query(f"select * from st_int_107")
        tdSql.checkData(0, 1, 124)

        tdSql.execute(f'insert into st_int_108 values(now, "-0.11e-30")')
        tdSql.query(f"select * from st_int_108")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f'insert into st_int_109 values(now, "-1.1e-307")')
        tdSql.query(f"select * from st_int_109")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_int_110 values(now,  -1e-1 )")
        tdSql.query(f"select * from st_int_110")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_int_111 values(now,  +0.1235e3 )")
        tdSql.query(f"select * from st_int_111")
        tdSql.checkData(0, 1, 124)

        tdSql.execute(f"insert into st_int_112 values(now, -0.11e-30)")
        tdSql.query(f"select * from st_int_112")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_int_113 values(now, -1.1e-307)")
        tdSql.query(f"select * from st_int_113")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_int_200 values(now,  0x01)")
        tdSql.query(f"select * from st_int_200")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_int_201 values(now, 0b01 )")
        tdSql.query(f"select * from st_int_201")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_int_202 values(now, +0x01)")
        tdSql.query(f"select * from st_int_202")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_int_203 values(now,  -0b01 )")
        tdSql.query(f"select * from st_int_203")
        tdSql.checkData(0, 1, -1)

    def auto_create_table(self):
        tdLog.info(f"case 2: dynamic create table for test tag values")

        tdSql.execute(
            f"insert into st_int_16 using mt_int tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show create table st_int_16")
        tdSql.query(f"show tags from st_int_16")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_int_16")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_int_17 using mt_int tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_int_17")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_int_17")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_int_18 using mt_int tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_int_18")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_int_18")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_int_19 using mt_int tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_int_19")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_int_19")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_int_20 using mt_int tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_int_20")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_int_20")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_int_21 using mt_int tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_int_21")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_int_21")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_int_22 using mt_int tags(2147483647) values(now, 2147483647)"
        )
        tdSql.query(f"show tags from st_int_22")
        tdSql.checkData(0, 5, 2147483647)

        tdSql.query(f"select * from st_int_22")
        tdSql.checkData(0, 1, 2147483647)

        tdSql.execute(
            f"insert into st_int_23 using mt_int tags(-2147483647) values(now, -2147483647)"
        )
        tdSql.query(f"show tags from st_int_23")
        tdSql.checkData(0, 5, -2147483647)

        tdSql.query(f"select * from st_int_23")
        tdSql.checkData(0, 1, -2147483647)

        tdSql.execute(f"insert into st_int_24 using mt_int tags(10) values(now, 10)")
        tdSql.query(f"show tags from st_int_24")
        tdSql.checkData(0, 5, 10)

        tdSql.query(f"select * from st_int_24")
        tdSql.checkData(0, 1, 10)

        tdSql.execute(
            f'insert into st_int_25 using mt_int tags("-0") values(now, "-0")'
        )
        tdSql.query(f"show tags from st_int_25")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_int_25")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_int_26 using mt_int tags('123') values(now, '123')"
        )
        tdSql.query(f"show tags from st_int_26")
        tdSql.checkData(0, 5, 123)

        tdSql.query(f"select * from st_int_26")
        tdSql.checkData(0, 1, 123)

        tdSql.execute(
            f"insert into st_int_27 using mt_int tags(+056) values(now, +00056)"
        )
        tdSql.query(f"show tags from st_int_27")
        tdSql.checkData(0, 5, 56)

        tdSql.query(f"select * from st_int_27")
        tdSql.checkData(0, 1, 56)

        tdSql.execute(
            f"insert into st_int_28 using mt_int tags(-056) values(now, -0056)"
        )
        tdSql.query(f"show tags from st_int_28")
        tdSql.checkData(0, 5, -56)

        tdSql.query(f"select * from st_int_28")
        tdSql.checkData(0, 1, -56)

        tdSql.execute(
            f'insert into st_int_1100 using mt_int tags("0x01") values(now, "0x01");'
        )
        tdSql.query(f"show tags from st_int_1100")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select * from st_int_1100")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_int_1101 using mt_int tags("0b01") values(now, "0b01")'
        )
        tdSql.query(f"show tags from st_int_1101")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select * from st_int_1101")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_int_1102 using mt_int tags("+0x01") values(now, "+0x01")'
        )
        tdSql.query(f"show tags from st_int_1102")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select * from st_int_1102")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_int_1103 using mt_int tags("-0b01") values(now, "-0b01")'
        )
        tdSql.query(f"show tags from st_int_1103")
        tdSql.checkData(0, 5, -1)

        tdSql.query(f"select * from st_int_1103")
        tdSql.checkData(0, 1, -1)

        tdSql.execute(
            f'insert into st_int_1104 using mt_int tags("-123.1") values(now, "-123.1");'
        )
        tdSql.query(f"show tags from st_int_1104")
        tdSql.checkData(0, 5, -123)

        tdSql.query(f"select * from st_int_1104")
        tdSql.checkData(0, 1, -123)

        tdSql.execute(
            f'insert into st_int_1105 using mt_int tags("+123.5") values(now, "+123.5")'
        )
        tdSql.query(f"show tags from st_int_1105")
        tdSql.checkData(0, 5, 124)

        tdSql.query(f"select * from st_int_1105")
        tdSql.checkData(0, 1, 124)

        tdSql.execute(
            f'insert into st_int_1106 using mt_int tags("-1e-1") values(now, "-1e-1")'
        )
        tdSql.query(f"show tags from st_int_1106")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_int_1106")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f'insert into st_int_1107 using mt_int tags("+0.1235e3") values(now, "+0.1235e3");'
        )
        tdSql.query(f"show tags from st_int_1107")
        tdSql.checkData(0, 5, 124)

        tdSql.execute(
            f'insert into st_int_1108 using mt_int tags("-0.11e-30") values(now, "-0.11e-30")'
        )
        tdSql.query(f"show tags from st_int_1108")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_int_1108")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f'insert into st_int_1109 using mt_int tags("-1.1e-307") values(now, "-1.1e-307")'
        )
        tdSql.query(f"show tags from st_int_1109")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_int_1109")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_int_1110 using mt_int tags( -1e-1 ) values(now, -1e-1);"
        )
        tdSql.query(f"show tags from st_int_1110")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_int_1110")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_int_1111 using mt_int tags( +0.1235e3 ) values(now, +0.1235e3)"
        )
        tdSql.query(f"show tags from st_int_1111")
        tdSql.checkData(0, 5, 124)

        tdSql.query(f"select * from st_int_1111")
        tdSql.checkData(0, 1, 124)

        tdSql.execute(
            f"insert into st_int_1112 using mt_int tags(-0.11e-30) values(now, -0.11e-30)"
        )
        tdSql.query(f"show tags from st_int_1112")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_int_1112")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_int_1113 using mt_int tags(-1.1e-307) values(now, -1.1e-307)"
        )
        tdSql.query(f"show tags from st_int_1113")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_int_1113")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_int_1200 using mt_int tags( 0x01) values(now, 0x01)"
        )
        tdSql.query(f"show tags from st_int_1200")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select * from st_int_1200")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_int_1201 using mt_int tags(0b01 ) values(now, 0b01)"
        )
        tdSql.query(f"show tags from st_int_1201")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select * from st_int_1201")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_int_1202 using mt_int tags(+0x01) values(now, +0x01)"
        )
        tdSql.query(f"show tags from st_int_1202")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select * from st_int_1202")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_int_1203 using mt_int tags( -0b01 ) values(now, -0b01)"
        )
        tdSql.query(f"show tags from st_int_1203")
        tdSql.checkData(0, 5, -1)

        tdSql.query(f"select * from st_int_1203")
        tdSql.checkData(0, 1, -1)

    def alter_tag_value(self):
        tdLog.info(f"case 3: alter tag value")

        tdSql.execute(f"alter table st_int_0 set tag tagname=2147483647")
        tdSql.query(f"show tags from st_int_0")
        tdSql.checkData(0, 5, 2147483647)

        tdSql.execute(f"alter table st_int_0 set tag tagname=-2147483647")
        tdSql.query(f"show tags from st_int_0")
        tdSql.checkData(0, 5, -2147483647)

        tdSql.execute(f"alter table st_int_0 set tag tagname=+100")
        tdSql.query(f"show tags from st_int_0")
        tdSql.checkData(0, 5, 100)

        tdSql.execute(f"alter table st_int_0 set tag tagname=-33")
        tdSql.query(f"show tags from st_int_0")
        tdSql.checkData(0, 5, -33)

        tdSql.execute(f"alter table st_int_0 set tag tagname='+98'")
        tdSql.query(f"show tags from st_int_0")
        tdSql.checkData(0, 5, 98)

        tdSql.execute(f"alter table st_int_0 set tag tagname='-076'")
        tdSql.query(f"show tags from st_int_0")
        tdSql.checkData(0, 5, -76)

        tdSql.execute(f"alter table st_int_0 set tag tagname=+0012")
        tdSql.query(f"show tags from st_int_0")
        tdSql.checkData(0, 5, 12)

        tdSql.execute(f"alter table st_int_0 set tag tagname=-00063")
        tdSql.query(f"show tags from st_int_0")
        tdSql.checkData(0, 5, -63)

        tdSql.execute(f'alter table st_int_100 set tag tagname="0x01"')
        tdSql.query(f"show tags from st_int_100")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'alter table st_int_101 set tag tagname="0b01"')
        tdSql.query(f"show tags from st_int_101")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'alter table st_int_102 set tag tagname="+0x01"')
        tdSql.query(f"show tags from st_int_102")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'alter table st_int_103 set tag tagname="-0b01"')
        tdSql.query(f"show tags from st_int_103")
        tdSql.checkData(0, 5, -1)

        tdSql.execute(f'alter table st_int_104 set tag tagname="-123.1"')
        tdSql.query(f"show tags from st_int_104")
        tdSql.checkData(0, 5, -123)

        tdSql.execute(f'alter table st_int_105 set tag tagname="+123.5"')
        tdSql.query(f"show tags from st_int_105")
        tdSql.checkData(0, 5, 124)

        tdSql.execute(f'alter table st_int_106 set tag tagname="-1e-1"')
        tdSql.query(f"show tags from st_int_106")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f'alter table st_int_107 set tag tagname="+0.1235e3"')
        tdSql.query(f"show tags from st_int_107")
        tdSql.checkData(0, 5, 124)

        tdSql.execute(f'alter table st_int_108 set tag tagname="-0.11e-30"')
        tdSql.query(f"show tags from st_int_108")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f'alter table st_int_109 set tag tagname="-1.1e-307"')
        tdSql.query(f"show tags from st_int_109")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"alter table st_int_110 set tag tagname= -1e-1 ")
        tdSql.query(f"show tags from st_int_110")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"alter table st_int_111 set tag tagname= +0.1235e3 ")
        tdSql.query(f"show tags from st_int_111")
        tdSql.checkData(0, 5, 124)

        tdSql.execute(f"alter table st_int_112 set tag tagname=-0.11e-30")
        tdSql.query(f"show tags from st_int_112")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"alter table st_int_113 set tag tagname=-1.1e-307")
        tdSql.query(f"show tags from st_int_113")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"alter table st_int_200 set tag tagname= 0x01")
        tdSql.query(f"show tags from st_int_200")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"alter table st_int_201 set tag tagname=0b01 ")
        tdSql.query(f"show tags from st_int_201")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"alter table st_int_202 set tag tagname=+0x01")
        tdSql.query(f"show tags from st_int_202")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"alter table st_int_203 set tag tagname= -0b01 ")
        tdSql.query(f"show tags from st_int_203")
        tdSql.checkData(0, 5, -1)

    def illegal_input(self):
        tdLog.info(f"case 4: illegal input")

        tdSql.error(f"create table st_int_e0 using mt_int tags(2147483648)")
        tdSql.execute(f"create table st_int_e0_err1 using mt_int tags(-2147483648)")
        tdSql.error(f"create table st_int_e0_err2 using mt_int tags(-2147483649)")
        tdSql.error(f"create table st_int_e0 using mt_int tags(214748364800)")
        tdSql.error(f"create table st_int_e0 using mt_int tags(-214748364800)")
        # truncate integer part
        tdSql.execute(f"create table st_int_e0 using mt_int tags(12.80)")
        tdSql.execute(f"drop table st_int_e0")
        tdSql.execute(f"create table st_int_e0 using mt_int tags(-11.80)")
        tdSql.execute(f"drop table st_int_e0")
        tdSql.error(f"create table st_int_e0 using mt_int tags(123abc)")
        tdSql.error(f'create table st_int_e0 using mt_int tags("123abc")')
        tdSql.error(f"create table st_int_e0 using mt_int tags(abc)")
        tdSql.error(f'create table st_int_e0 using mt_int tags("abc")')
        tdSql.error(f'create table st_int_e0 using mt_int tags(" ")')
        tdSql.error(f"create table st_int_e0_err2 using mt_int tags('')")

        tdSql.execute(f"create table st_int_e0 using mt_int tags(123)")
        tdSql.execute(f"create table st_int_e1 using mt_int tags(123)")
        tdSql.execute(f"create table st_int_e2 using mt_int tags(123)")
        tdSql.execute(f"create table st_int_e3 using mt_int tags(123)")
        tdSql.execute(f"create table st_int_e4 using mt_int tags(123)")
        tdSql.execute(f"create table st_int_e5 using mt_int tags(123)")
        tdSql.execute(f"create table st_int_e6 using mt_int tags(123)")
        tdSql.execute(f"create table st_int_e7 using mt_int tags(123)")
        tdSql.execute(f"create table st_int_e8 using mt_int tags(123)")
        tdSql.execute(f"create table st_int_e9 using mt_int tags(123)")
        tdSql.execute(f"create table st_int_e10 using mt_int tags(123)")
        tdSql.execute(f"create table st_int_e11 using mt_int tags(123)")
        tdSql.execute(f"create table st_int_e12 using mt_int tags(123)")

        tdSql.error(f"insert into st_int_e0 values(now, 2147483648)")
        tdSql.execute(f"insert into st_int_e1 values(now, -2147483648)")
        tdSql.error(f"insert into st_int_e2 values(now, 3147483648)")
        tdSql.error(f"insert into st_int_e3 values(now, -21474836481)")
        tdSql.execute(f"insert into st_int_e4 values(now, 12.80)")
        tdSql.execute(f"insert into st_int_e5 values(now, -11.80)")
        tdSql.error(f"insert into st_int_e6 values(now, 123abc)")
        tdSql.error(f'insert into st_int_e7 values(now, "123abc")')
        tdSql.error(f"insert into st_int_e9 values(now, abc)")
        tdSql.error(f'insert into st_int_e10 values(now, "abc")')
        tdSql.error(f'insert into st_int_e11 values(now, " ")')
        tdSql.error(f"insert into st_int_e12 values(now, ''")

        tdSql.error(
            f"insert into st_int_e13 using mt_int tags(033) values(now, 2147483648)"
        )
        tdSql.execute(
            f"insert into st_int_e14 using mt_int tags(033) values(now, -2147483648)"
        )
        tdSql.error(
            f"insert into st_int_e15 using mt_int tags(033) values(now, 5147483648)"
        )
        tdSql.error(
            f"insert into st_int_e16 using mt_int tags(033) values(now, -21474836481)"
        )
        tdSql.execute(
            f"insert into st_int_e17 using mt_int tags(033) values(now, 12.80)"
        )
        tdSql.execute(
            f"insert into st_int_e18 using mt_int tags(033) values(now, -11.80)"
        )
        tdSql.error(
            f"insert into st_int_e19 using mt_int tags(033) values(now, 123abc)"
        )
        tdSql.error(
            f'insert into st_int_e20 using mt_int tags(033) values(now, "123abc")'
        )
        tdSql.error(f"insert into st_int_e22 using mt_int tags(033) values(now, abc)")
        tdSql.error(f'insert into st_int_e23 using mt_int tags(033) values(now, "abc")')
        tdSql.error(f'insert into st_int_e24 using mt_int tags(033) values(now, " ")')
        tdSql.error(f"insert into st_int_e25 using mt_int tags(033) values(now, '')")

        tdSql.error(
            f"insert into st_int_e13 using mt_int tags(2147483648) values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_int_e14_1 using mt_int tags(-2147483648) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_int_e15 using mt_int tags(21474836480) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_int_e16 using mt_int tags(-2147483649) values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_int_e17 using mt_int tags(12.80) values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_int_e18 using mt_int tags(-11.80) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_int_e19 using mt_int tags(123abc) values(now, -033)"
        )
        tdSql.error(
            f'insert into st_int_e20 using mt_int tags("123abc") values(now, -033)'
        )
        tdSql.error(f"insert into st_int_e22 using mt_int tags(abc) values(now, -033)")
        tdSql.error(
            f'insert into st_int_e23 using mt_int tags("abc") values(now, -033)'
        )
        tdSql.error(f'insert into st_int_e24 using mt_int tags(" ") values(now, -033)')
        tdSql.error(
            f"insert into st_int_e25_1 using mt_int tags(" ") values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_int_e26_1 using mt_int tags('123') values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_int_e27_1 using mt_int tags('12.80') values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_int_e28_1 using mt_int tags('-11.80') values(now, -033)"
        )

        tdSql.execute(
            f"insert into st_int_e13 using mt_int tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_int_e14 using mt_int tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_int_e15 using mt_int tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_int_e16 using mt_int tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_int_e17 using mt_int tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_int_e18 using mt_int tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_int_e19 using mt_int tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_int_e20 using mt_int tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_int_e21 using mt_int tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_int_e22 using mt_int tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_int_e23 using mt_int tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_int_e24 using mt_int tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_int_e25 using mt_int tags(033) values(now, 00062)"
        )

        tdSql.execute(f"alter table st_int_e13 set tag tagname=2147483647")
        tdSql.error(f"alter table st_int_e13 set tag tagname=2147483648")
        tdSql.execute(f"alter table st_int_e14 set tag tagname=-2147483648")
        tdSql.error(f"alter table st_int_e14 set tag tagname=2147483649")
        tdSql.error(f"alter table st_int_e15 set tag tagname=12147483648")
        tdSql.error(f"alter table st_int_e16 set tag tagname=-3147483648")
        tdSql.error(f"alter table st_int_e19 set tag tagname=123abc")
        tdSql.error(f'alter table st_int_e20 set tag tagname="123abc"')
        tdSql.error(f"alter table st_int_e22 set tag tagname=abc")
        tdSql.error(f'alter table st_int_e23 set tag tagname="abc"')
        tdSql.error(f"alter table st_int_e25 set tag tagname=1+1d")
        tdSql.error(f'alter table st_int_e25 set tag tagname="1"+1d')
        tdSql.error(f'alter table st_int_e24 set tag tagname=" "')
        tdSql.error(f"alter table st_int_e25 set tag tagname=''")
        tdSql.execute(f"alter table st_int_e26_1 set tag tagname='123'")
        tdSql.execute(f"alter table st_int_e27_1 set tag tagname='12.80'")
        tdSql.execute(f"alter table st_int_e28_1 set tag tagname='-11.80'")
