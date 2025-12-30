from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatatypeUint:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_datatype_uint(self):
        """DataTypes: unsigned int

        1. Create table
        2. Insert data
        3. Auto-create table
        4. Alter tag value
        5. Handle illegal input

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_uint.sim

        """
        self.create_table()
        self.insert_data()
        self.auto_create_table()
        self.alter_tag_value()
        self.illegal_input()

    def create_table(self):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table mt_uint (ts timestamp, c int unsigned) tags(tagname int unsigned)"
        )

        tdLog.info(f"case 0: static create table for test tag values")

        tdSql.execute(f"create table st_uint_0 using mt_uint tags(NULL)")
        tdSql.query(f"show create table st_uint_0")
        tdSql.query(f"show tags from st_uint_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_uint_1 using mt_uint tags(NULL)")
        tdSql.query(f"show tags from st_uint_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_uint_2 using mt_uint tags('NULL')")
        tdSql.query(f"show tags from st_uint_2")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_uint_3 using mt_uint tags('NULL')")
        tdSql.query(f"show tags from st_uint_3")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_uint_4 using mt_uint tags("NULL")')
        tdSql.query(f"show tags from st_uint_4")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_uint_5 using mt_uint tags("-0")')
        tdSql.query(f"show tags from st_uint_5")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"create table st_uint_6 using mt_uint tags(-0 )")
        tdSql.query(f"show tags from st_uint_6")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"create table st_uint_7 using mt_uint tags(2147483647)")
        tdSql.query(f"show tags from st_uint_7")
        tdSql.checkData(0, 5, 2147483647)

        tdSql.execute(f"create table st_uint_8 using mt_uint tags(37)")
        tdSql.query(f"show tags from st_uint_8")
        tdSql.checkData(0, 5, 37)

        tdSql.execute(f"create table st_uint_9 using mt_uint tags(098)")
        tdSql.query(f"show tags from st_uint_9")
        tdSql.checkData(0, 5, 98)

        tdSql.execute(f"create table st_uint_10 using mt_uint tags(+113)")
        tdSql.query(f"show tags from st_uint_10")
        tdSql.checkData(0, 5, 113)

        tdSql.execute(f"create table st_uint_11 using mt_uint tags(+000.000)")
        tdSql.query(f"show tags from st_uint_11")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f'create table st_uint_12 using mt_uint tags("+78")')
        tdSql.query(f"show tags from st_uint_12")
        tdSql.checkData(0, 5, 78)

        tdSql.execute(f"create table st_uint_13 using mt_uint tags(+0078)")
        tdSql.query(f"show tags from st_uint_13")
        tdSql.checkData(0, 5, 78)

        tdSql.execute(f"create table st_uint_14 using mt_uint tags(00078)")
        tdSql.query(f"show tags from st_uint_14")
        tdSql.checkData(0, 5, 78)

        tdSql.execute(f'create table st_uint_100 using mt_uint tags("0x01")')
        tdSql.query(f"show tags from st_uint_100")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'create table st_uint_101 using mt_uint tags("0b01")')
        tdSql.query(f"show tags from st_uint_101")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'create table st_uint_102 using mt_uint tags("+0x01")')
        tdSql.query(f"show tags from st_uint_102")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'create table st_uint_103 using mt_uint tags("-0b00")')
        tdSql.query(f"show tags from st_uint_103")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f'create table st_uint_104 using mt_uint tags("123.1")')
        tdSql.query(f"show tags from st_uint_104")
        tdSql.checkData(0, 5, 123)

        tdSql.execute(f'create table st_uint_105 using mt_uint tags("+123.5")')
        tdSql.query(f"show tags from st_uint_105")
        tdSql.checkData(0, 5, 124)

        tdSql.execute(f'create table st_uint_106 using mt_uint tags("-1e-1")')
        tdSql.query(f"show tags from st_uint_106")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f'create table st_uint_107 using mt_uint tags("+0.1235e3")')
        tdSql.query(f"show tags from st_uint_107")
        tdSql.checkData(0, 5, 124)

        tdSql.execute(f'create table st_uint_108 using mt_uint tags("-0.11e-30")')
        tdSql.query(f"show tags from st_uint_108")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f'create table st_uint_109 using mt_uint tags("-1.1e-307")')
        tdSql.query(f"show tags from st_uint_109")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"create table st_uint_110 using mt_uint tags( -1e-1 )")
        tdSql.query(f"show tags from st_uint_110")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"create table st_uint_111 using mt_uint tags( +0.1235e3 )")
        tdSql.query(f"show tags from st_uint_111")
        tdSql.checkData(0, 5, 124)

        tdSql.execute(f"create table st_uint_112 using mt_uint tags(-0.11e-30)")
        tdSql.query(f"show tags from st_uint_112")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"create table st_uint_113 using mt_uint tags(-1.1e-307)")
        tdSql.query(f"show tags from st_uint_113")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"create table st_uint_200 using mt_uint tags( 0x01)")
        tdSql.query(f"show tags from st_uint_200")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"create table st_uint_201 using mt_uint tags(0b01 )")
        tdSql.query(f"show tags from st_uint_201")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"create table st_uint_202 using mt_uint tags(+0x01)")
        tdSql.query(f"show tags from st_uint_202")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"create table st_uint_203 using mt_uint tags( -0b00 )")
        tdSql.query(f"show tags from st_uint_203")
        tdSql.checkData(0, 5, 0)

    def insert_data(self):
        tdLog.info(f"case 1: insert values for test column values")

        tdSql.execute(f"insert into st_uint_0 values(now, NULL)")
        tdSql.query(f"select * from st_uint_0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f'insert into st_uint_1 values(now, "-0")')
        tdSql.query(f"select * from st_uint_1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_uint_6 values(now, 2147483647)")
        tdSql.query(f"select * from st_uint_6")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 2147483647)

        tdSql.execute(f"insert into st_uint_7 values(now, -0)")
        tdSql.query(f"select * from st_uint_7")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_uint_8 values(now, +100)")
        tdSql.query(f"select * from st_uint_8")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 100)

        tdSql.execute(f'insert into st_uint_9 values(now, "098")')
        tdSql.query(f"select * from st_uint_9")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 98)

        tdSql.execute(f"insert into st_uint_10 values(now, '0')")
        tdSql.query(f"select * from st_uint_10")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_uint_11 values(now, +000.000)")
        tdSql.query(f"select * from st_uint_11")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f'insert into st_uint_12 values(now, "+056")')
        tdSql.query(f"select * from st_uint_12")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 56)

        tdSql.execute(f"insert into st_uint_13 values(now, +056)")
        tdSql.query(f"select * from st_uint_13")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 56)

        tdSql.execute(f"insert into st_uint_14 values(now, 056)")
        tdSql.query(f"select * from st_uint_14")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 56)

        tdSql.execute(f'insert into st_uint_100 values(now, "0x01")')
        tdSql.query(f"select * from st_uint_100")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_uint_101 values(now, "0b01")')
        tdSql.query(f"select * from st_uint_101")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_uint_102 values(now, "+0x01")')
        tdSql.query(f"select * from st_uint_102")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_uint_103 values(now, "-0b00")')
        tdSql.query(f"select * from st_uint_103")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f'insert into st_uint_104 values(now, "123.1")')
        tdSql.query(f"select * from st_uint_104")
        tdSql.checkData(0, 1, 123)

        tdSql.execute(f'insert into st_uint_105 values(now, "+123.5")')
        tdSql.query(f"select * from st_uint_105")
        tdSql.checkData(0, 1, 124)

        tdSql.execute(f'insert into st_uint_106 values(now, "-1e-1")')
        tdSql.query(f"select * from st_uint_106")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f'insert into st_uint_107 values(now, "+0.1235e3")')
        tdSql.query(f"select * from st_uint_107")
        tdSql.checkData(0, 1, 124)

        tdSql.execute(f'insert into st_uint_108 values(now, "-0.11e-30")')
        tdSql.query(f"select * from st_uint_108")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f'insert into st_uint_109 values(now, "-1.1e-307")')
        tdSql.query(f"select * from st_uint_109")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_uint_110 values(now,  -1e-1 )")
        tdSql.query(f"select * from st_uint_110")

        tdSql.execute(f"insert into st_uint_111 values(now,  +0.1235e3 )")
        tdSql.query(f"select * from st_uint_111")
        tdSql.checkData(0, 1, 124)

        tdSql.execute(f"insert into st_uint_112 values(now, -0.11e-30)")
        tdSql.query(f"select * from st_uint_112")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_uint_113 values(now, -1.1e-307)")
        tdSql.query(f"select * from st_uint_113")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_uint_200 values(now,  0x01)")
        tdSql.query(f"select * from st_uint_200")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_uint_201 values(now, 0b01 )")
        tdSql.query(f"select * from st_uint_201")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_uint_202 values(now, +0x01)")
        tdSql.query(f"select * from st_uint_202")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_uint_203 values(now,  -0b00 )")
        tdSql.query(f"select * from st_uint_203")
        tdSql.checkData(0, 1, 0)

    def auto_create_table(self):
        tdLog.info(f"case 2: dynamic create table for test tag values")

        tdSql.execute(
            f"insert into st_uint_16 using mt_uint tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show create table st_uint_16")
        tdSql.query(f"show tags from st_uint_16")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_uint_16")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_uint_17 using mt_uint tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_uint_17")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_uint_17")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_uint_18 using mt_uint tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_uint_18")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_uint_18")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_uint_19 using mt_uint tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_uint_19")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_uint_19")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_uint_20 using mt_uint tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_uint_20")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_uint_20")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_uint_21 using mt_uint tags("-0") values(now, "-0")'
        )
        tdSql.query(f"show tags from st_uint_21")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_uint_21")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_uint_22 using mt_uint tags(2147483647) values(now, 2147483647)"
        )
        tdSql.query(f"show tags from st_uint_22")
        tdSql.checkData(0, 5, 2147483647)

        tdSql.query(f"select * from st_uint_22")
        tdSql.checkData(0, 1, 2147483647)

        tdSql.execute(f"insert into st_uint_23 using mt_uint tags(-0) values(now, -0)")
        tdSql.query(f"show tags from st_uint_23")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_uint_23")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_uint_24 using mt_uint tags(10) values(now, 10)")
        tdSql.query(f"show tags from st_uint_24")
        tdSql.checkData(0, 5, 10)

        tdSql.query(f"select * from st_uint_24")
        tdSql.checkData(0, 1, 10)

        tdSql.execute(
            f'insert into st_uint_25 using mt_uint tags("-0") values(now, "-0")'
        )
        tdSql.query(f"show tags from st_uint_25")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_uint_25")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_uint_26 using mt_uint tags('123') values(now, '123')"
        )
        tdSql.query(f"show tags from st_uint_26")
        tdSql.checkData(0, 5, 123)

        tdSql.query(f"select * from st_uint_26")
        tdSql.checkData(0, 1, 123)

        tdSql.execute(
            f"insert into st_uint_27 using mt_uint tags(+056) values(now, +00056)"
        )
        tdSql.query(f"show tags from st_uint_27")
        tdSql.checkData(0, 5, 56)

        tdSql.query(f"select * from st_uint_27")
        tdSql.checkData(0, 1, 56)

        tdSql.execute(
            f"insert into st_uint_28 using mt_uint tags(056) values(now, 0056)"
        )
        tdSql.query(f"show tags from st_uint_28")
        tdSql.checkData(0, 5, 56)

        tdSql.query(f"select * from st_uint_28")
        tdSql.checkData(0, 1, 56)

        tdSql.execute(
            f'insert into st_uint_1100 using mt_uint tags("0x01") values(now, "0x01")'
        )
        tdSql.query(f"show tags from st_uint_1100")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select * from st_uint_1100")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_uint_1101 using mt_uint tags("0b01") values(now, "0b01")'
        )
        tdSql.query(f"show tags from st_uint_1101")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select * from st_uint_1101")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_uint_1102 using mt_uint tags("+0x01") values(now, "+0x01")'
        )
        tdSql.query(f"show tags from st_uint_1102")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select * from st_uint_1102")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_uint_1103 using mt_uint tags("-0b00") values(now, "-0b000")'
        )
        tdSql.query(f"show tags from st_uint_1103")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_uint_1103")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f'insert into st_uint_1104 using mt_uint tags("123.1") values(now, "123.1")'
        )
        tdSql.query(f"show tags from st_uint_1104")
        tdSql.checkData(0, 5, 123)

        tdSql.query(f"select * from st_uint_1104")
        tdSql.checkData(0, 1, 123)

        tdSql.execute(
            f'insert into st_uint_1105 using mt_uint tags("+123.5") values(now, "+123.5")'
        )
        tdSql.query(f"show tags from st_uint_1105")
        tdSql.checkData(0, 5, 124)

        tdSql.query(f"select * from st_uint_1105")
        tdSql.checkData(0, 1, 124)

        tdSql.execute(
            f'insert into st_uint_1106 using mt_uint tags("-1e-1") values(now, "-1e-1")'
        )
        tdSql.query(f"show tags from st_uint_1106")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_uint_1106")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f'insert into st_uint_1107 using mt_uint tags("+0.1235e3") values(now, "+0.1235e3")'
        )
        tdSql.query(f"show tags from st_uint_1107")
        tdSql.checkData(0, 5, 124)

        tdSql.execute(
            f'insert into st_uint_1108 using mt_uint tags("-0.11e-30") values(now, "-0.11e-30")'
        )
        tdSql.query(f"show tags from st_uint_1108")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_uint_1108")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f'insert into st_uint_1109 using mt_uint tags("-1.1e-307") values(now, "-1.1e-307")'
        )
        tdSql.query(f"show tags from st_uint_1109")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_uint_1109")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_uint_1110 using mt_uint tags( -1e-1 ) values(now, -1e-1)"
        )
        tdSql.query(f"show tags from st_uint_1110")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_uint_1110")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_uint_1111 using mt_uint tags( +0.1235e3 ) values(now, +0.1235e3)"
        )
        tdSql.query(f"show tags from st_uint_1111")
        tdSql.checkData(0, 5, 124)

        tdSql.query(f"select * from st_uint_1111")
        tdSql.checkData(0, 1, 124)

        tdSql.execute(
            f"insert into st_uint_1112 using mt_uint tags(-0.11e-30) values(now, -0.11e-30)"
        )
        tdSql.query(f"show tags from st_uint_1112")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_uint_1112")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_uint_1113 using mt_uint tags(-1.1e-307) values(now, -1.1e-307)"
        )
        tdSql.query(f"show tags from st_uint_1113")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_uint_1113")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_uint_1200 using mt_uint tags( 0x01) values(now, 0x01)"
        )
        tdSql.query(f"show tags from st_uint_1200")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select * from st_uint_1200")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_uint_1201 using mt_uint tags(0b01 ) values(now, 0b01)"
        )
        tdSql.query(f"show tags from st_uint_1201")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select * from st_uint_1201")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_uint_1202 using mt_uint tags(+0x01) values(now, +0x01)"
        )
        tdSql.query(f"show tags from st_uint_1202")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select * from st_uint_1202")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_uint_1203 using mt_uint tags( 0b000000 ) values(now, -0b0000)"
        )
        tdSql.query(f"show tags from st_uint_1203")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_uint_1203")
        tdSql.checkData(0, 1, 0)

    def alter_tag_value(self):
        tdLog.info(f"case 3: alter tag value")

        tdSql.execute(f"alter table st_uint_0 set tag tagname=2147483647")
        tdSql.query(f"show tags from st_uint_0")
        tdSql.checkData(0, 5, 2147483647)

        tdSql.execute(f"alter table st_uint_0 set tag tagname=-0")
        tdSql.query(f"show tags from st_uint_0")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"alter table st_uint_0 set tag tagname=+100")
        tdSql.query(f"show tags from st_uint_0")
        tdSql.checkData(0, 5, 100)

        tdSql.execute(f"alter table st_uint_0 set tag tagname=+33.333")
        tdSql.query(f"show tags from st_uint_0")
        tdSql.checkData(0, 5, 33)

        tdSql.execute(f"alter table st_uint_0 set tag tagname='+98'")
        tdSql.query(f"show tags from st_uint_0")
        tdSql.checkData(0, 5, 98)

        tdSql.execute(f"alter table st_uint_0 set tag tagname='076'")
        tdSql.query(f"show tags from st_uint_0")
        tdSql.checkData(0, 5, 76)

        tdSql.execute(f"alter table st_uint_0 set tag tagname=+0012")
        tdSql.query(f"show tags from st_uint_0")
        tdSql.checkData(0, 5, 12)

        tdSql.execute(f"alter table st_uint_0 set tag tagname=00063")
        tdSql.query(f"show tags from st_uint_0")
        tdSql.checkData(0, 5, 63)

        tdSql.execute(f'alter table st_uint_100 set tag tagname="0x01"')
        tdSql.query(f"show tags from st_uint_100")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'alter table st_uint_101 set tag tagname="0b01"')
        tdSql.query(f"show tags from st_uint_101")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'alter table st_uint_102 set tag tagname="+0x01"')
        tdSql.query(f"show tags from st_uint_102")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'alter table st_uint_103 set tag tagname="-0b00"')
        tdSql.query(f"show tags from st_uint_103")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f'alter table st_uint_104 set tag tagname="123.1"')
        tdSql.query(f"show tags from st_uint_104")
        tdSql.checkData(0, 5, 123)

        tdSql.execute(f'alter table st_uint_105 set tag tagname="+123.5"')
        tdSql.query(f"show tags from st_uint_105")
        tdSql.checkData(0, 5, 124)

        tdSql.execute(f'alter table st_uint_106 set tag tagname="-1e-1"')
        tdSql.query(f"show tags from st_uint_106")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f'alter table st_uint_107 set tag tagname="+0.1235e3"')
        tdSql.query(f"show tags from st_uint_107")
        tdSql.checkData(0, 5, 124)

        tdSql.execute(f'alter table st_uint_108 set tag tagname="-0.11e-30"')
        tdSql.query(f"show tags from st_uint_108")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f'alter table st_uint_109 set tag tagname="-1.1e-307"')
        tdSql.query(f"show tags from st_uint_109")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"alter table st_uint_110 set tag tagname= -1e-1 ")
        tdSql.query(f"show tags from st_uint_110")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"alter table st_uint_111 set tag tagname= +0.1235e3 ")
        tdSql.query(f"show tags from st_uint_111")
        tdSql.checkData(0, 5, 124)

        tdSql.execute(f"alter table st_uint_112 set tag tagname=-0.11e-30")
        tdSql.query(f"show tags from st_uint_112")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"alter table st_uint_113 set tag tagname=-1.1e-307")
        tdSql.query(f"show tags from st_uint_113")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"alter table st_uint_200 set tag tagname= 0x01")
        tdSql.query(f"show tags from st_uint_200")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"alter table st_uint_201 set tag tagname=0b00")
        tdSql.query(f"show tags from st_uint_201")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"alter table st_uint_202 set tag tagname=+0x01")
        tdSql.query(f"show tags from st_uint_202")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"alter table st_uint_203 set tag tagname= -0b0")
        tdSql.query(f"show tags from st_uint_203")
        tdSql.checkData(0, 5, 0)

    def illegal_input(self):
        tdLog.info(f"case 4: illegal input")

        tdSql.error(f"create table st_uint_e0_err0 using mt_uint tags(4294967296)")
        tdSql.error(f"create table st_uint_e0_err1 using mt_uint tags(-4294967297)")
        tdSql.error(f"create table st_uint_e0_err2 using mt_uint tags(-214748364800)")
        tdSql.error(f"create table st_uint_e0_err3 using mt_uint tags(123abc)")
        tdSql.error(f'create table st_uint_e0_err4 using mt_uint tags("123abc")')
        tdSql.error(f"create table st_uint_e0_err5 using mt_uint tags(abc)")
        tdSql.error(f'create table st_uint_e0_err6 using mt_uint tags("abc")')
        tdSql.error(f'create table st_uint_e0_err7 using mt_uint tags(" ")')
        tdSql.error(f"create table st_uint_e0_err8 using mt_uint tags('')")

        tdSql.execute(f"create table st_uint_e0 using mt_uint tags(123)")
        tdSql.execute(f"create table st_uint_e1 using mt_uint tags(123)")
        tdSql.execute(f"create table st_uint_e2 using mt_uint tags(123)")
        tdSql.execute(f"create table st_uint_e3 using mt_uint tags(123)")
        tdSql.execute(f"create table st_uint_e4 using mt_uint tags(123)")
        tdSql.execute(f"create table st_uint_e5 using mt_uint tags(123)")
        tdSql.execute(f"create table st_uint_e6 using mt_uint tags(123)")
        tdSql.execute(f"create table st_uint_e7 using mt_uint tags(123)")
        tdSql.execute(f"create table st_uint_e8 using mt_uint tags(123)")
        tdSql.execute(f"create table st_uint_e9 using mt_uint tags(123)")
        tdSql.execute(f"create table st_uint_e10 using mt_uint tags(123)")
        tdSql.execute(f"create table st_uint_e11 using mt_uint tags(123)")
        tdSql.execute(f"create table st_uint_e12 using mt_uint tags(123)")

        tdSql.error(f"insert into st_uint_e0 values(now, 4294967296)")
        tdSql.error(f"insert into st_uint_e1 values(now, -4294967297)")
        tdSql.error(f"insert into st_uint_e3 values(now, -21474836481)")
        tdSql.error(f"insert into st_uint_e6 values(now, 123abc)")
        tdSql.error(f'insert into st_uint_e7 values(now, "123abc")')
        tdSql.error(f"insert into st_uint_e9 values(now, abc)")
        tdSql.error(f'insert into st_uint_e10 values(now, "abc")')
        tdSql.error(f'insert into st_uint_e11 values(now, " ")')
        tdSql.error(f"insert into st_uint_e12 values(now, '')")

        tdSql.error(
            f"insert into st_uint_e13 using mt_uint tags(033) values(now, 4294967296)"
        )
        tdSql.error(
            f"insert into st_uint_e14 using mt_uint tags(033) values(now, -4294967297)"
        )
        tdSql.error(
            f"insert into st_uint_e16 using mt_uint tags(033) values(now, -21474836481)"
        )
        tdSql.error(
            f"insert into st_uint_e19 using mt_uint tags(033) values(now, 123abc)"
        )
        tdSql.error(
            f'insert into st_uint_e20 using mt_uint tags(033) values(now, "123abc")'
        )
        tdSql.error(f"insert into st_uint_e22 using mt_uint tags(033) values(now, abc)")
        tdSql.error(
            f'insert into st_uint_e23 using mt_uint tags(033) values(now, "abc")'
        )
        tdSql.error(f'insert into st_uint_e24 using mt_uint tags(033) values(now, " ")')
        tdSql.error(f"insert into st_uint_e25 using mt_uint tags(033) values(now, '')")

        tdSql.error(
            f"insert into st_uint_e13 using mt_uint tags(21474294967296483648) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_uint_e14_1 using mt_uint tags(-2147483648) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_uint_e16 using mt_uint tags(-2147483649) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_uint_e19 using mt_uint tags(123abc) values(now, -033)"
        )
        tdSql.error(
            f'insert into st_uint_e20 using mt_uint tags("123abc") values(now, -033)'
        )
        tdSql.error(
            f"insert into st_uint_e22 using mt_uint tags(abc) values(now, -033)"
        )
        tdSql.error(
            f'insert into st_uint_e23 using mt_uint tags("abc") values(now, -033)'
        )
        tdSql.error(
            f'insert into st_uint_e24 using mt_uint tags(" ") values(now, -033)'
        )
        tdSql.error(
            f"insert into st_uint_e25_1 using mt_uint tags('') values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_uint_e26_1 using mt_uint tags('123') values(now, 033)"
        )
        tdSql.execute(
            f"insert into st_uint_e27_1 using mt_uint tags('12.80') values(now, 033)"
        )
        tdSql.error(
            f"insert into st_uint_e28_1 using mt_uint tags('-11.80') values(now, 033)"
        )

        tdSql.execute(
            f"insert into st_uint_e13 using mt_uint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_uint_e14 using mt_uint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_uint_e15 using mt_uint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_uint_e16 using mt_uint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_uint_e17 using mt_uint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_uint_e18 using mt_uint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_uint_e19 using mt_uint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_uint_e20 using mt_uint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_uint_e21 using mt_uint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_uint_e22 using mt_uint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_uint_e23 using mt_uint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_uint_e24 using mt_uint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_uint_e25 using mt_uint tags(033) values(now, 00062)"
        )

        tdSql.error(f"alter table st_uint_e13 set tag tagname=4294967296")
        tdSql.error(f"alter table st_uint_e14 set tag tagname=-4294967297")
        tdSql.error(f"alter table st_uint_e16 set tag tagname=-3147483648")
        tdSql.error(f"alter table st_uint_e19 set tag tagname=123abc")
        tdSql.error(f'alter table st_uint_e20 set tag tagname="123abc"')
        tdSql.error(f"alter table st_uint_e22 set tag tagname=abc")
        tdSql.error(f'alter table st_uint_e23 set tag tagname="abc"')
        tdSql.error(f'alter table st_uint_e24 set tag tagname=" "')
        tdSql.error(f"alter table st_uint_e25 set tag tagname=''")
        tdSql.execute(f"alter table st_uint_e26_1 set tag tagname='123'")
        tdSql.execute(f"alter table st_uint_e27_1 set tag tagname='12.80'")
        tdSql.error(f"alter table st_uint_e28_1 set tag tagname='-11.80'")
