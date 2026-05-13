from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatatypeDouble:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_datatype_double(self):
        """DataTypes: double

        1. Create table
        2. Insert data
        3. Auto-create table
        4. Alter tag value
        5. Handle illegal input


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_double.sim

        """
        self.create_table()
        self.insert_data()
        self.auto_create_table()
        self.alter_tag_value()
        self.illegal_input()

    def create_table(self):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table mt_double (ts timestamp, c double) tags(tagname double)"
        )

        tdLog.info(f"case 0: static create table for test tag values")

        tdSql.execute(f"create table st_double_0 using mt_double tags(NULL )")
        tdSql.query(f"show tags from st_double_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_double_1 using mt_double tags(NULL)")
        tdSql.query(f"show tags from st_double_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_double_2 using mt_double tags('NULL')")
        tdSql.query(f"show tags from st_double_2")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_double_3 using mt_double tags('NULL')")
        tdSql.query(f"show tags from st_double_3")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_double_4 using mt_double tags("NULL")')
        tdSql.query(f"show tags from st_double_4")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_double_5 using mt_double tags("NULL")')
        tdSql.query(f"show tags from st_double_5")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_double_6 using mt_double tags(-123.321)")
        tdSql.query(f"show tags from st_double_6")
        tdSql.checkData(0, 5, "-123.321000000")

        tdSql.execute(f"create table st_double_7 using mt_double tags(+1.567)")
        tdSql.query(f"show tags from st_double_7")
        tdSql.checkData(0, 5, "1.567000000")

        tdSql.execute(f"create table st_double_8 using mt_double tags(379.001)")
        tdSql.query(f"show tags from st_double_8")
        tdSql.checkData(0, 5, "379.001000000")

        tdSql.execute(f"create table st_double_9 using mt_double tags(1.5e+3)")
        tdSql.query(f"show tags from st_double_9")
        tdSql.checkData(0, 5, "1500.000000000")

        tdSql.execute(f"create table st_double_10 using mt_double tags(-1.5e-3)")
        tdSql.query(f"show tags from st_double_10")
        tdSql.checkData(0, 5, "-0.001500000")

        tdSql.execute(f"create table st_double_11 using mt_double tags(+1.5e+3)")
        tdSql.query(f"show tags from st_double_11")
        tdSql.checkData(0, 5, "1500.000000000")

        tdSql.execute(f"create table st_double_12 using mt_double tags(-1.5e+3)")
        tdSql.query(f"show tags from st_double_12")
        tdSql.checkData(0, 5, "-1500.000000000")

        tdSql.execute(f"create table st_double_13 using mt_double tags(1.5e-3)")
        tdSql.query(f"show tags from st_double_13")
        tdSql.checkData(0, 5, "0.001500000")

        tdSql.execute(f"create table st_double_14 using mt_double tags(1.5E-3)")
        tdSql.query(f"show tags from st_double_14")
        tdSql.checkData(0, 5, "0.001500000")

        tdSql.execute(f"create table st_double_6_0 using mt_double tags('-123.321')")
        tdSql.query(f"show tags from st_double_6_0")
        tdSql.checkData(0, 5, "-123.321000000")

        tdSql.execute(f"create table st_double_7_0 using mt_double tags('+1.567')")
        tdSql.query(f"show tags from st_double_7_0")
        tdSql.checkData(0, 5, "1.567000000")

        tdSql.execute(f"create table st_double_8_0 using mt_double tags('379.001')")
        tdSql.query(f"show tags from st_double_8_0")
        tdSql.checkData(0, 5, "379.001000000")

        tdSql.execute(f"create table st_double_9_0 using mt_double tags('1.5e+3')")
        tdSql.query(f"show tags from st_double_9_0")
        tdSql.checkData(0, 5, "1500.000000000")

        tdSql.execute(f"create table st_double_10_0 using mt_double tags('-1.5e-3')")
        tdSql.query(f"show tags from st_double_10_0")
        tdSql.checkData(0, 5, "-0.001500000")

        tdSql.execute(f"create table st_double_11_0 using mt_double tags('+1.5e+3')")
        tdSql.query(f"show tags from st_double_11_0")
        tdSql.checkData(0, 5, "1500.000000000")

        tdSql.execute(f"create table st_double_12_0 using mt_double tags('-1.5e+3')")
        tdSql.query(f"show tags from st_double_12_0")
        tdSql.checkData(0, 5, "-1500.000000000")

        tdSql.execute(f"create table st_double_13_0 using mt_double tags('1.5e-3')")
        tdSql.query(f"show tags from st_double_13_0")
        tdSql.checkData(0, 5, "0.001500000")

        tdSql.execute(f"create table st_double_14_0 using mt_double tags('1.5E-3')")
        tdSql.query(f"show tags from st_double_14_0")
        tdSql.checkData(0, 5, "0.001500000")

        tdSql.execute(
            f"create table st_double_15_0 using mt_double tags(1.7976931348623157e+308)"
        )
        tdSql.query(f"show tags from st_double_15_0")
        tdSql.checkData(
            0,
            5,
            "179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.000000000",
        )

        tdSql.execute(
            f"create table st_double_16_0 using mt_double tags(-1.7976931348623157e+308)"
        )
        tdSql.query(f"show tags from st_double_16_0")
        tdSql.checkData(
            0,
            5,
            "-179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.000000000",
        )

        tdSql.execute(f'create table st_double_100 using mt_double tags("0x01")')
        tdSql.query(f"show tags from st_double_100")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.execute(f'create table st_double_101 using mt_double tags("0b01")')
        tdSql.query(f"show tags from st_double_101")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.execute(f'create table st_double_102 using mt_double tags("+0x01")')
        tdSql.query(f"show tags from st_double_102")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.execute(f'create table st_double_103 using mt_double tags("-0b01")')
        tdSql.query(f"show tags from st_double_103")
        tdSql.checkData(0, 5, "-1.000000000")

        tdSql.execute(f"create table st_double_200 using mt_double tags( 0x01)")
        tdSql.query(f"show tags from st_double_200")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.execute(f"create table st_double_201 using mt_double tags(0b01 )")
        tdSql.query(f"show tags from st_double_201")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.execute(f"create table st_double_202 using mt_double tags(+0x01)")
        tdSql.query(f"show tags from st_double_202")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.execute(f"create table st_double_203 using mt_double tags( -0b01 )")
        tdSql.query(f"show tags from st_double_203")
        tdSql.checkData(0, 5, "-1.000000000")

    def insert_data(self):
        tdLog.info(f"case 1: insert values for test column values")

        tdSql.execute(f"insert into st_double_0 values(now, NULL )")
        tdSql.query(f"select * from st_double_0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_double_1 values(now, NULL)")
        tdSql.query(f"select * from st_double_1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_double_2 values(now, 'NULL')")
        tdSql.query(f"select * from st_double_2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_double_3 values(now, 'NULL')")
        tdSql.query(f"select * from st_double_3")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f'insert into st_double_4 values(now, "NULL")')
        tdSql.query(f"select * from st_double_4")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f'insert into st_double_5 values(now, "NULL")')
        tdSql.query(f"select * from st_double_5")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_double_6 values(now, 1.7976931348623157e+308)")
        tdSql.query(f"select * from st_double_6")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1.7976931348623157e+308)

        tdSql.execute(f"insert into st_double_7 values(now, -1.7976931348623157e+308)")
        tdSql.query(f"select * from st_double_7")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -1.7976931348623157e+308)

        tdSql.execute(f"insert into st_double_8 values(now, +100.89)")
        tdSql.query(f"select * from st_double_8")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 100.89000000)

        tdSql.execute(f'insert into st_double_9 values(now, "-0.98")')
        tdSql.query(f"select * from st_double_9")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -0.980000000)

        tdSql.execute(f"insert into st_double_10 values(now, '0')")
        tdSql.query(f"select * from st_double_10")
        tdSql.checkRows(1)

        tdSql.execute(f"insert into st_double_11 values(now, -0)")
        tdSql.query(f"select * from st_double_11")
        tdSql.checkRows(1)

        tdSql.execute(f'insert into st_double_12 values(now, "+056")')
        tdSql.query(f"select * from st_double_12")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 56.000000)

        tdSql.execute(f"insert into st_double_13 values(now, +056)")
        tdSql.query(f"select * from st_double_13")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 56.000000000)

        tdSql.execute(f"insert into st_double_14 values(now, -056)")
        tdSql.query(f"select * from st_double_14")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -56)

        tdSql.execute(f'insert into  st_double_100 values(now, "0x01")')
        tdSql.query(f"select * from st_double_100")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1.000000000)

        tdSql.execute(f'insert into  st_double_101 values(now, "0b01")')
        tdSql.query(f"select * from st_double_101")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1.000000000)

        tdSql.execute(f'insert into  st_double_102 values(now, "+0x01")')
        tdSql.query(f"select * from st_double_102")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1.000000000)

        tdSql.execute(f'insert into  st_double_103 values(now, "-0b01")')
        tdSql.query(f"select * from st_double_103")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -1.000000000)

        tdSql.execute(f"insert into  st_double_200 values(now,  0x01)")
        tdSql.query(f"select * from st_double_200")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1.000000000)

        tdSql.execute(f"insert into  st_double_201 values(now, 0b01 )")
        tdSql.query(f"select * from st_double_201")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1.000000000)

        tdSql.execute(f"insert into  st_double_202 values(now, +0x01)")
        tdSql.query(f"select * from st_double_202")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1.000000000)

        tdSql.execute(f"insert into  st_double_203 values(now,  -0b01 )")
        tdSql.query(f"select * from st_double_203")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -1.000000000)

    def auto_create_table(self):
        tdLog.info(f"case 2: dynamic create table for test tag values")

        tdSql.execute(
            f"insert into st_double_16 using mt_double tags(NULL ) values(now, NULL )"
        )
        tdSql.query(f"show tags from st_double_16")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_double_16")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_double_17 using mt_double tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_double_17")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_double_17")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_double_18 using mt_double tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_double_18")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_double_18")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_double_19 using mt_double tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_double_19")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_double_19")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_double_20 using mt_double tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_double_20")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_double_20")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_double_21 using mt_double tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_double_21")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_double_21")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_double_22 using mt_double tags(127) values(now, 1.7976931348623157e+308)"
        )
        tdSql.query(f"show tags from st_double_22")
        tdSql.checkData(0, 5, "127.000000000")

        tdSql.query(f"select * from st_double_22")
        tdSql.checkData(0, 1, 1.7976931348623157e+308)

        tdSql.execute(
            f"insert into st_double_23 using mt_double tags(-127) values(now, -1.7976931348623157e+308)"
        )
        tdSql.query(f"show tags from st_double_23")
        tdSql.checkData(0, 5, "-127.000000000")

        tdSql.query(f"select * from st_double_23")
        tdSql.checkData(0, 1, -1.7976931348623157e+308)

        tdSql.execute(
            f"insert into st_double_24 using mt_double tags(10) values(now, 10)"
        )
        tdSql.query(f"show tags from st_double_24")

        tdSql.query(f"select * from st_double_24")
        tdSql.checkData(0, 1, 10)

        tdSql.execute(
            f'insert into st_double_25 using mt_double tags("-0") values(now, "-0")'
        )
        tdSql.query(f"show tags from st_double_25")

        tdSql.query(f"select * from st_double_25")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_double_26 using mt_double tags('123') values(now, '12.3')"
        )
        tdSql.query(f"show tags from st_double_26")
        tdSql.checkData(0, 5, "123.000000000")

        tdSql.query(f"select * from st_double_26")
        tdSql.checkData(0, 1, 12.3 )

        tdSql.execute(
            f"insert into st_double_27 using mt_double tags(+056) values(now, +0005.6)"
        )
        tdSql.query(f"show tags from st_double_27")
        tdSql.checkData(0, 5, "56.000000000")

        tdSql.query(f"select * from st_double_27")
        tdSql.checkData(0, 1, 5.6)

        tdSql.execute(
            f"insert into st_double_28 using mt_double tags(-056) values(now, -005.6)"
        )
        tdSql.query(f"show tags from st_double_28")
        tdSql.checkData(0, 5, "-56.000000000")

        tdSql.query(f"select * from st_double_28")
        tdSql.checkData(0, 1, -5.6)

        tdSql.execute(
            f'insert into st_double_100 using mt_double tags("0x01") values(now, "0x01")'
        )
        tdSql.query(f"show tags from st_double_100")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.query(f"select * from st_double_100")
        tdSql.checkData(0, 1, 1.000000000)

        tdSql.execute(
            f'insert into st_double_101 using mt_double tags("0b01") values(now, "0b01")'
        )
        tdSql.query(f"show tags from st_double_101")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.query(f"select * from st_double_101")
        tdSql.checkData(0, 1, 1.000000000)

        tdSql.execute(
            f'insert into st_double_102 using mt_double tags("+0x01") values(now, "+0x01")'
        )
        tdSql.query(f"show tags from st_double_102")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.query(f"select * from st_double_102")
        tdSql.checkData(0, 1, 1.000000000)

        tdSql.execute(
            f'insert into st_double_103 using mt_double tags("-0b01") values(now, "-0b01")'
        )
        tdSql.query(f"show tags from st_double_103")
        tdSql.checkData(0, 5, "-1.000000000")

        tdSql.query(f"select * from st_double_103")
        tdSql.checkData(0, 1, -1.000000000)

        tdSql.execute(
            f"insert into st_double_200 using mt_double tags( 0x01) values(now, 0x01)"
        )
        tdSql.query(f"show tags from st_double_200")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.query(f"select * from st_double_200")
        tdSql.checkData(0, 1, 1.000000000)

        tdSql.execute(
            f"insert into st_double_201 using mt_double tags(0b01 ) values(now, 0b01)"
        )
        tdSql.query(f"show tags from st_double_201")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.query(f"select * from st_double_201")
        tdSql.checkData(0, 1, 1.000000000)

        tdSql.execute(
            f"insert into st_double_202 using mt_double tags(+0x01) values(now, +0x01)"
        )
        tdSql.query(f"show tags from st_double_202")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.query(f"select * from st_double_202")
        tdSql.checkData(0, 1, 1.000000000)

        tdSql.execute(
            f"insert into st_double_203 using mt_double tags( -0b01 ) values(now, -0b01)"
        )
        tdSql.query(f"show tags from st_double_203")
        tdSql.checkData(0, 5, "-1.000000000")

        tdSql.query(f"select * from st_double_203")
        tdSql.checkData(0, 1, -1.000000000)

    def alter_tag_value(self):
        tdLog.info(f"case 3: alter tag value")

        tdSql.execute(f'alter table st_double_100 set tag tagname="0x01"')
        tdSql.query(f"show tags from st_double_100")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.execute(f'alter table st_double_101 set tag tagname="0b01"')
        tdSql.query(f"show tags from st_double_101")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.execute(f'alter table st_double_102 set tag tagname="+0x01"')
        tdSql.query(f"show tags from st_double_102")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.execute(f'alter table st_double_103 set tag tagname="-0b01"')
        tdSql.query(f"show tags from st_double_103")
        tdSql.checkData(0, 5, "-1.000000000")

        tdSql.execute(f"alter table st_double_200 set tag tagname= 0x01")
        tdSql.query(f"show tags from st_double_200")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.execute(f"alter table st_double_201 set tag tagname=0b01 ")
        tdSql.query(f"show tags from st_double_201")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.execute(f"alter table st_double_202 set tag tagname=+0x01")
        tdSql.query(f"show tags from st_double_202")
        tdSql.checkData(0, 5, "1.000000000")

        tdSql.execute(f"alter table st_double_203 set tag tagname= -0b01 ")
        tdSql.query(f"show tags from st_double_203")
        tdSql.checkData(0, 5, "-1.000000000")

    def illegal_input(self):
        tdLog.info(f"case 4: illegal input")

        tdSql.error(
            f"create table st_double_e0 using mt_double tags(1.8976931348623157e+308)"
        )
        tdSql.error(
            f"create table st_double_e0 using mt_double tags(-1.8976931348623157e+308)"
        )
        tdSql.error(
            f"create table st_double_e0 using mt_double tags(31.7976931348623157e+308)"
        )
        tdSql.error(
            f"create table st_double_e0 using mt_double tags(-31.7976931348623157e+308)"
        )
        # truncate integer part
        tdSql.execute(f"create table st_double_e0 using mt_double tags(12.80)")
        tdSql.execute(f"drop table st_double_e0")
        tdSql.execute(f"create table st_double_e0 using mt_double tags(-11.80)")
        tdSql.execute(f"drop table st_double_e0")
        tdSql.error(f"create table st_double_e0 using mt_double tags(123abc)")
        tdSql.error(f'create table st_double_e0_1 using mt_double tags("123abc")')
        tdSql.error(f"create table st_double_e0 using mt_double tags(abc)")
        tdSql.error(f'create table st_double_e0_2 using mt_double tags("abc")')
        tdSql.error(f'create table st_double_e0_3 using mt_double tags(" ")')
        tdSql.error(f"create table st_double_e0_4 using mt_double tags('')")

        tdSql.execute(f"create table st_double_e0 using mt_double tags(123)")
        tdSql.execute(f"create table st_double_e1 using mt_double tags(123)")
        tdSql.execute(f"create table st_double_e2 using mt_double tags(123)")
        tdSql.execute(f"create table st_double_e3 using mt_double tags(123)")
        tdSql.execute(f"create table st_double_e4 using mt_double tags(123)")
        tdSql.execute(f"create table st_double_e5 using mt_double tags(123)")
        tdSql.execute(f"create table st_double_e6 using mt_double tags(123)")
        tdSql.execute(f"create table st_double_e7 using mt_double tags(123)")
        tdSql.execute(f"create table st_double_e8 using mt_double tags(123)")
        tdSql.execute(f"create table st_double_e9 using mt_double tags(123)")
        tdSql.execute(f"create table st_double_e10 using mt_double tags(123)")
        tdSql.execute(f"create table st_double_e11 using mt_double tags(123)")
        tdSql.execute(f"create table st_double_e12 using mt_double tags(123)")

        tdSql.error(f"insert into st_double_e0 values(now, 11.7976931348623157e+308)")
        tdSql.error(f"insert into st_double_e1 values(now, -11.7976931348623157e+308)")
        tdSql.error(f"insert into st_double_e2 values(now, 111.7976931348623157e+308)")
        tdSql.error(f"insert into st_double_e3 values(now, -111.7976931348623157e+308)")
        tdSql.execute(f"insert into st_double_e4 values(now, 12.80)")
        tdSql.execute(f"insert into st_double_e5 values(now, -11.80)")
        tdSql.error(f"insert into st_double_e6 values(now, 123abc)")
        tdSql.error(f'insert into st_double_e7 values(now, "123abc")')
        tdSql.error(f"insert into st_double_e9 values(now, abc)")
        tdSql.error(f'insert into st_double_e10 values(now, "abc")')
        tdSql.error(f'insert into st_double_e11 values(now, " ")')
        tdSql.error(f"insert into st_double_e12 values(now, '')")

        tdSql.error(
            f"insert into st_double_e13 using mt_double tags(033) values(now, 11.7976931348623157e+308)"
        )
        tdSql.error(
            f"insert into st_double_e14 using mt_double tags(033) values(now, -11.7976931348623157e+308)"
        )
        tdSql.error(
            f"insert into st_double_e15 using mt_double tags(033) values(now, 131.7976931348623157e+308)"
        )
        tdSql.error(
            f"insert into st_double_e16 using mt_double tags(033) values(now, -131.7976931348623157e+308)"
        )
        tdSql.execute(
            f"insert into st_double_e17 using mt_double tags(033) values(now, 12.80)"
        )
        tdSql.execute(
            f"insert into st_double_e18 using mt_double tags(033) values(now, -11.80)"
        )
        tdSql.error(
            f"insert into st_double_e19 using mt_double tags(033) values(now, 123abc)"
        )
        tdSql.error(
            f'insert into st_double_e20 using mt_double tags(033) values(now, "123abc")'
        )
        tdSql.error(
            f"insert into st_double_e22 using mt_double tags(033) values(now, abc)"
        )
        tdSql.error(
            f'insert into st_double_e23 using mt_double tags(033) values(now, "abc")'
        )
        tdSql.error(
            f'insert into st_double_e24 using mt_double tags(033) values(now, " ")'
        )
        tdSql.error(
            f"insert into st_double_e25_1 using mt_double tags(033) values(now, '')"
        )

        tdSql.error(
            f"insert into st_double_e13 using mt_double tags(31.7976931348623157e+308) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_double_e14 using mt_double tags(-31.7976931348623157e+308) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_double_e15 using mt_double tags(131.7976931348623157e+308) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_double_e16 using mt_double tags(-131.7976931348623157e+308) values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_double_e17 using mt_double tags(12.80) values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_double_e18 using mt_double tags(-11.80) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_double_e19 using mt_double tags(123abc) values(now, -033)"
        )
        tdSql.error(
            f'insert into st_double_e20 using mt_double tags("123abc") values(now, -033)'
        )
        tdSql.error(
            f"insert into st_double_e22 using mt_double tags(abc) values(now, -033)"
        )
        tdSql.error(
            f'insert into st_double_e23 using mt_double tags("abc") values(now, -033)'
        )
        tdSql.error(
            f'insert into st_double_e24 using mt_double tags(" ") values(now, -033)'
        )
        tdSql.error(
            f"insert into st_double_e25 using mt_double tags(" ") values(now, -033)"
        )
        tdSql.execute(
            f'insert into st_double_e20 using mt_double tags("123") values(now, -033)'
        )

        tdSql.execute(
            f"insert into st_double_e13 using mt_double tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_double_e14 using mt_double tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_double_e15 using mt_double tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_double_e16 using mt_double tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_double_e17 using mt_double tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_double_e18 using mt_double tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_double_e19 using mt_double tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_double_e20 using mt_double tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_double_e21 using mt_double tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_double_e22 using mt_double tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_double_e23 using mt_double tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_double_e24 using mt_double tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_double_e25 using mt_double tags(033) values(now, 00062)"
        )

        tdSql.error(
            f"alter table st_double_e13 set tag tagname=1.8976931348623157e+308"
        )
        tdSql.error(
            f"alter table st_double_e14 set tag tagname=-1.8976931348623157e+308"
        )
        tdSql.error(
            f"alter table st_double_e15 set tag tagname=131.7976931348623157e+308"
        )
        tdSql.error(
            f"alter table st_double_e16 set tag tagname=-131.7976931348623157e+308"
        )
        tdSql.error(f"alter table st_double_e19 set tag tagname=123abc")
        tdSql.error(f'alter table st_double_e20 set tag tagname="123abc"')
        tdSql.error(f"alter table st_double_e22 set tag tagname=abc")
        tdSql.error(f'alter table st_double_e23 set tag tagname="abc"')
        tdSql.error(f'alter table st_double_e24 set tag tagname=" "')
        tdSql.error(f"alter table st_double_e25 set tag tagname=''")
        tdSql.execute(f"alter table st_double_e25 set tag tagname='123'")
