from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatatypeFloat:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_datatype_float(self):
        """DataTypes: float

        1. Create table
        2. Insert data
        3. Auto-create table
        4. Alter tag value
        5. Handle illegal input


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_float.sim

        """
        self.create_table()
        self.insert_data()
        self.auto_create_table()
        self.alter_tag_value()
        self.illegal_input()

    def create_table(self):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table mt_float (ts timestamp, c float) tags(tagname float)"
        )

        tdLog.info(f"case 0: static create table for test tag values")

        tdSql.execute(f"create table st_float_0 using mt_float tags(NULL)")
        tdSql.query(f"show create table st_float_0")
        tdSql.query(f"show tags from st_float_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_float_1 using mt_float tags(NULL)")
        tdSql.query(f"show tags from st_float_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_float_2 using mt_float tags('NULL')")
        tdSql.query(f"show tags from st_float_2")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_float_3 using mt_float tags('NULL')")
        tdSql.query(f"show tags from st_float_3")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_float_4 using mt_float tags("NULL")')
        tdSql.query(f"show tags from st_float_4")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_float_5 using mt_float tags("NULL")')
        tdSql.query(f"show tags from st_float_5")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_float_6 using mt_float tags(-123.321)")
        tdSql.query(f"show tags from st_float_6")
        tdSql.checkData(0, 5, "-123.32100")

        tdSql.execute(f"create table st_float_7 using mt_float tags(+1.567)")
        tdSql.query(f"show tags from st_float_7")
        tdSql.checkData(0, 5, "1.56700")

        tdSql.execute(f"create table st_float_8 using mt_float tags(379.001)")
        tdSql.query(f"show tags from st_float_8")
        tdSql.checkData(0, 5, "379.00101")

        tdSql.execute(f"create table st_float_9 using mt_float tags(1.5e+3)")
        tdSql.query(f"show tags from st_float_9")
        tdSql.checkData(0, 5, "1500.00000")

        tdSql.execute(f"create table st_float_10 using mt_float tags(-1.5e-3)")
        tdSql.query(f"show tags from st_float_10")
        tdSql.checkData(0, 5, "-0.00150")

        tdSql.execute(f"create table st_float_11 using mt_float tags(+1.5e+3)")
        tdSql.query(f"show tags from st_float_11")
        tdSql.checkData(0, 5, "1500.00000")

        tdSql.execute(f"create table st_float_12 using mt_float tags(-1.5e+3)")
        tdSql.query(f"show tags from st_float_12")
        tdSql.checkData(0, 5, "-1500.00000")

        tdSql.execute(f"create table st_float_13 using mt_float tags(1.5e-3)")
        tdSql.query(f"show tags from st_float_13")
        tdSql.checkData(0, 5, "0.00150")

        tdSql.execute(f"create table st_float_14 using mt_float tags(1.5E-3)")
        tdSql.query(f"show tags from st_float_14")
        tdSql.checkData(0, 5, "0.00150")

        tdSql.execute(f"create table st_float_6_0 using mt_float tags('-123.321')")
        tdSql.query(f"show tags from st_float_6_0")
        tdSql.checkData(0, 5, "-123.32100")

        tdSql.execute(f"create table st_float_7_0 using mt_float tags('+1.567')")
        tdSql.query(f"show tags from st_float_7_0")
        tdSql.checkData(0, 5, "1.56700")

        tdSql.execute(f"create table st_float_8_0 using mt_float tags('379.001')")
        tdSql.query(f"show tags from st_float_8_0")
        tdSql.checkData(0, 5, "379.00101")

        tdSql.execute(f"create table st_float_9_0 using mt_float tags('1.5e+3')")
        tdSql.query(f"show tags from st_float_9_0")
        tdSql.checkData(0, 5, "1500.00000")

        tdSql.execute(f"create table st_float_10_0 using mt_float tags('-1.5e-3')")
        tdSql.query(f"show tags from st_float_10_0")
        tdSql.checkData(0, 5, "-0.00150")

        tdSql.execute(f"create table st_float_11_0 using mt_float tags('+1.5e+3')")
        tdSql.query(f"show tags from st_float_11_0")
        tdSql.checkData(0, 5, "1500.00000")

        tdSql.execute(f"create table st_float_12_0 using mt_float tags('-1.5e+3')")
        tdSql.query(f"show tags from st_float_12_0")
        tdSql.checkData(0, 5, "-1500.00000")

        tdSql.execute(f"create table st_float_13_0 using mt_float tags('1.5e-3')")
        tdSql.query(f"show tags from st_float_13_0")
        tdSql.checkData(0, 5, "0.00150")

        tdSql.execute(f"create table st_float_14_0 using mt_float tags('1.5E-3')")
        tdSql.query(f"show tags from st_float_14_0")
        tdSql.checkData(0, 5, "0.00150")

        tdSql.execute(
            f"create table st_float_15_0 using mt_float tags(3.40282346638528859811704183484516925e+38)"
        )
        tdSql.query(f"show tags from st_float_15_0")
        tdSql.execute(
            f"create table st_float_16_0 using mt_float tags(-3.40282346638528859811704183484516925e+38)"
        )
        tdSql.query(f"show tags from st_float_16_0")

        tdSql.execute(f'create table st_float_100 using mt_float tags("0x01")')
        tdSql.query(f"show tags from st_float_100")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.execute(f'create table st_float_101 using mt_float tags("0b01")')
        tdSql.query(f"show tags from st_float_101")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.execute(f'create table st_float_102 using mt_float tags("+0x01")')
        tdSql.query(f"show tags from st_float_102")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.execute(f'create table st_float_103 using mt_float tags("-0b01")')
        tdSql.query(f"show tags from st_float_103")
        tdSql.checkData(0, 5, "-1.00000")

        tdSql.execute(f"create table st_float_200 using mt_float tags( 0x01)")
        tdSql.query(f"show tags from st_float_200")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.execute(f"create table st_float_201 using mt_float tags(0b01 )")
        tdSql.query(f"show tags from st_float_201")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.execute(f"create table st_float_202 using mt_float tags(+0x01)")
        tdSql.query(f"show tags from st_float_202")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.execute(f"create table st_float_203 using mt_float tags( -0b01 )")
        tdSql.query(f"show tags from st_float_203")
        tdSql.checkData(0, 5, "-1.00000")

    def insert_data(self):
        tdLog.info(f"case 1: insert values for test column values")

        tdSql.execute(f"insert into st_float_0 values(now, NULL)")
        tdSql.query(f"select * from st_float_0")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_float_1 values(now, NULL)")
        tdSql.query(f"select * from st_float_1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_float_2 values(now, 'NULL')")
        tdSql.query(f"select * from st_float_2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_float_3 values(now, 'NULL')")
        tdSql.query(f"select * from st_float_3")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, None)

        tdSql.execute(f'insert into st_float_4 values(now, "NULL")')
        tdSql.query(f"select * from st_float_4")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, None)

        tdSql.execute(f'insert into st_float_5 values(now, "NULL")')
        tdSql.query(f"select * from st_float_5")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f'insert into  st_float_100 values(now, "0x01")')
        tdSql.query(f"select * from st_float_100")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1.00000)

        tdSql.execute(f'insert into  st_float_101 values(now, "0b01")')
        tdSql.query(f"select * from st_float_101")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1.00000)

        tdSql.execute(f'insert into  st_float_102 values(now, "+0x01")')
        tdSql.query(f"select * from st_float_102")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1.00000)

        tdSql.execute(f'insert into  st_float_103 values(now, "-0b01")')
        tdSql.query(f"select * from st_float_103")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, -1.00000)

        tdSql.execute(f"insert into  st_float_200 values(now,  0x01)")
        tdSql.query(f"select * from st_float_200")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1.00000)

        tdSql.execute(f"insert into  st_float_201 values(now, 0b01 )")
        tdSql.query(f"select * from st_float_201")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1.00000)

        tdSql.execute(f"insert into  st_float_202 values(now, +0x01)")
        tdSql.query(f"select * from st_float_202")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1.00000)

        tdSql.execute(f"insert into  st_float_203 values(now,  -0b01 )")
        tdSql.query(f"select * from st_float_203")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -1.00000)

        tdSql.error(f"insert into st_float_6 values(now, 3.40282347e+38)")
        tdSql.error(f"insert into st_float_6 values(now, -3.40282347e+38)")

        tdSql.execute(
            f"insert into st_float_6 values(now, 340282346638528859811704183484516925440.00000)"
        )
        tdSql.query(f"select * from st_float_6")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 340282346638528859811704183484516925440.00000)

        tdSql.execute(
            f"insert into st_float_7 values(now, -340282346638528859811704183484516925440.00000)"
        )
        tdSql.query(f"select * from st_float_7")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -340282346638528859811704183484516925440.00000)

        tdSql.execute(f"insert into st_float_8 values(now, +100.89)")
        tdSql.query(f"select * from st_float_8")
        tdSql.checkRows(1)

        tdSql.execute(f'insert into st_float_9 values(now, "-0.98")')
        tdSql.query(f"select * from st_float_9")
        tdSql.checkRows(1)

        tdSql.execute(f"insert into st_float_10 values(now, '0')")
        tdSql.query(f"select * from st_float_10")
        tdSql.checkRows(1)

        tdSql.execute(f"insert into st_float_11 values(now, -0)")
        tdSql.query(f"select * from st_float_11")
        tdSql.checkRows(1)

        tdSql.execute(f'insert into st_float_12 values(now, "+056")')
        tdSql.query(f"select * from st_float_12")
        tdSql.checkRows(1)

        tdSql.execute(f"insert into st_float_13 values(now, +056)")
        tdSql.query(f"select * from st_float_13")
        tdSql.checkRows(1)

        tdSql.execute(f"insert into st_float_14 values(now, -056)")
        tdSql.query(f"select * from st_float_14")
        tdSql.checkRows(1)

    def auto_create_table(self):
        tdLog.info(f"case 2: dynamic create table for test tag values")

        tdSql.execute(
            f"insert into st_float_16 using mt_float tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show create table st_float_16")
        tdSql.query(f"show tags from st_float_16")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_float_16")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_float_17 using mt_float tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_float_17")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_float_17")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_float_18 using mt_float tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_float_18")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_float_18")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_float_19 using mt_float tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_float_19")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_float_19")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_float_20 using mt_float tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_float_20")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_float_20")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_float_21 using mt_float tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_float_21")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_float_21")
        tdSql.checkData(0, 1, None)

        tdSql.error(
            f" insert into st_float_22 using mt_float tags(127) values(now, 3.40282347e+38)"
        )

        tdSql.execute(
            f"insert into st_float_22 using mt_float tags(127) values(now, 340282346638528859811704183484516925440.00000)"
        )
        tdSql.query(f"show tags from st_float_22")
        tdSql.checkData(0, 5, "127.00000")

        tdSql.query(f"select tbname, tagname from st_float_22")
        tdSql.checkData(0, 1, 127.00000)

        tdSql.execute(
            f"insert into st_float_23 using mt_float tags(-127) values(now, -340282346638528859811704183484516925440.00000)"
        )
        tdSql.query(f"show tags from st_float_23")
        tdSql.checkData(0, 5, "-127.00000")

        tdSql.execute(
            f"insert into st_float_24 using mt_float tags(10) values(now, 10)"
        )
        tdSql.query(f"show tags from st_float_24")
        tdSql.checkData(0, 5, "10.00000")

        tdSql.query(f"select * from st_float_24")
        tdSql.checkData(0, 1, 10.00000)

        tdSql.execute(
            f'insert into st_float_25 using mt_float tags("-0") values(now, "-0")'
        )
        tdSql.query(f"show tags from st_float_25")
        tdSql.checkData(0, 5, "-0.00000")

        tdSql.query(f"select * from st_float_25")
        tdSql.checkData(0, 1, -0.00000)

        tdSql.execute(
            f"insert into st_float_26 using mt_float tags('123') values(now, '12.3')"
        )
        tdSql.query(f"show tags from st_float_26")
        tdSql.checkData(0, 5, "123.00000")

        tdSql.query(f"select * from st_float_26")
        tdSql.checkData(0, 1, 12.30000)

        tdSql.execute(
            f"insert into st_float_27 using mt_float tags(+056) values(now, +0005.6)"
        )
        tdSql.query(f"show tags from st_float_27")
        tdSql.checkData(0, 5, "56.00000")

        tdSql.query(f"select * from st_float_27")
        tdSql.checkData(0, 1, 5.60000)

        tdSql.execute(
            f"insert into st_float_28 using mt_float tags(-056) values(now, -005.6)"
        )
        tdSql.query(f"show tags from st_float_28")
        tdSql.checkData(0, 5, "-56.00000")

        tdSql.query(f"select * from st_float_28")
        tdSql.checkData(0, 1, -5.60000)

        tdSql.execute(
            f'insert into st_float_100 using mt_float tags("0x01") values(now, "0x01")'
        )
        tdSql.query(f"show tags from st_float_100")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.query(f"select * from st_float_100")
        tdSql.checkData(0, 1, 1.00000)

        tdSql.execute(
            f'insert into st_float_101 using mt_float tags("0b01") values(now, "0b01")'
        )
        tdSql.query(f"show tags from st_float_101")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.query(f"select * from st_float_101")
        tdSql.checkData(0, 1, 1.00000)

        tdSql.execute(
            f'insert into st_float_102 using mt_float tags("+0x01") values(now, "+0x01")'
        )
        tdSql.query(f"show tags from st_float_102")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.query(f"select * from st_float_102")
        tdSql.checkData(0, 1, 1.00000)

        tdSql.execute(
            f'insert into st_float_103 using mt_float tags("-0b01") values(now, "-0b01")'
        )
        tdSql.query(f"show tags from st_float_103")
        tdSql.checkData(0, 5, "-1.00000")

        tdSql.query(f"select * from st_float_103")
        tdSql.checkData(0, 1, -1.00000)

        tdSql.execute(
            f"insert into st_float_200 using mt_float tags( 0x01) values(now, 0x01)"
        )
        tdSql.query(f"show tags from st_float_200")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.query(f"select * from st_float_200")
        tdSql.checkData(0, 1, 1.00000)

        tdSql.execute(
            f"insert into st_float_201 using mt_float tags(0b01 ) values(now, 0b01)"
        )
        tdSql.query(f"show tags from st_float_201")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.query(f"select * from st_float_201")
        tdSql.checkData(0, 1, 1.00000)

        tdSql.execute(
            f"insert into st_float_202 using mt_float tags(+0x01) values(now, +0x01)"
        )
        tdSql.query(f"show tags from st_float_202")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.query(f"select * from st_float_202")
        tdSql.checkData(0, 1, 1.00000)

        tdSql.execute(
            f"insert into st_float_203 using mt_float tags( -0b01 ) values(now, -0b01)"
        )
        tdSql.query(f"show tags from st_float_203")
        tdSql.checkData(0, 5, "-1.00000")

        tdSql.query(f"select * from st_float_203")
        tdSql.checkData(0, 1, -1.00000)

    def alter_tag_value(self):
        tdLog.info(f"case 3: alter tag value")

        tdSql.execute(
            f"alter table st_float_0 set tag tagname=340282346638528859811704183484516925440.00000"
        )
        tdSql.query(f"show tags from st_float_0")
        tdSql.checkData(0, 5, "340282346638528859811704183484516925440.00000")

        tdSql.execute(
            f"alter table st_float_0 set tag tagname=-340282346638528859811704183484516925440.00000"
        )
        tdSql.query(f"show tags from st_float_0")
        tdSql.checkData(0, 5, "-340282346638528859811704183484516925440.00000")

        tdSql.execute(f"alter table st_float_0 set tag tagname=+10.340")
        tdSql.query(f"show tags from st_float_0")
        tdSql.checkData(0, 5, "10.34000")

        tdSql.execute(f"alter table st_float_0 set tag tagname=-33.87")
        tdSql.query(f"show tags from st_float_0")
        tdSql.checkData(0, 5, "-33.87000")

        tdSql.execute(f"alter table st_float_0 set tag tagname='+9.8'")
        tdSql.query(f"show tags from st_float_0")
        tdSql.checkData(0, 5, "9.80000")

        tdSql.execute(f"alter table st_float_0 set tag tagname='-07.6'")
        tdSql.query(f"show tags from st_float_0")
        tdSql.checkData(0, 5, "-7.60000")

        tdSql.execute(f"alter table st_float_0 set tag tagname=+0012.871")
        tdSql.query(f"show tags from st_float_0")
        tdSql.checkData(0, 5, "12.87100")

        tdSql.execute(f"alter table st_float_0 set tag tagname=-00063.582")
        tdSql.query(f"show tags from st_float_0")
        tdSql.checkData(0, 5, "-63.58200")

        tdSql.execute(f'alter table st_float_100 set tag tagname="0x01"')
        tdSql.query(f"show tags from st_float_100")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.execute(f'alter table st_float_101 set tag tagname="0b01"')
        tdSql.query(f"show tags from st_float_101")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.execute(f'alter table st_float_102 set tag tagname="+0x01"')
        tdSql.query(f"show tags from st_float_102")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.execute(f'alter table st_float_103 set tag tagname="-0b01"')
        tdSql.query(f"show tags from st_float_103")
        tdSql.checkData(0, 5, "-1.00000")

        tdSql.execute(f"alter table st_float_200 set tag tagname= 0x01")
        tdSql.query(f"show tags from st_float_200")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.execute(f"alter table st_float_201 set tag tagname=0b01 ")
        tdSql.query(f"show tags from st_float_201")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.execute(f"alter table st_float_202 set tag tagname=+0x01")
        tdSql.query(f"show tags from st_float_202")
        tdSql.checkData(0, 5, "1.00000")

        tdSql.execute(f"alter table st_float_203 set tag tagname= -0b01 ")
        tdSql.query(f"show tags from st_float_203")
        tdSql.checkData(0, 5, "-1.00000")

    def illegal_input(self):
        tdLog.info(f"case 4: illegal input")

        tdSql.error(f"create table st_float_e0 using mt_float tags(3.50282347e+38)")
        tdSql.error(f"create table st_float_e0 using mt_float tags(-3.50282347e+38)")
        tdSql.error(f"create table st_float_e0 using mt_float tags(333.40282347e+38)")
        tdSql.error(f"create table st_float_e0 using mt_float tags(-333.40282347e+38)")
        # truncate integer part
        tdSql.execute(f"create table st_float_e0 using mt_float tags(12.80)")
        tdSql.execute(f"drop table st_float_e0")
        tdSql.execute(f"create table st_float_e0 using mt_float tags(-11.80)")
        tdSql.execute(f"drop table st_float_e0")
        tdSql.error(f"create table st_float_e0 using mt_float tags(123abc)")
        tdSql.error(f'create table st_float_e0_1 using mt_float tags("123abc")')
        tdSql.error(f"create table st_float_e0 using mt_float tags(abc)")
        tdSql.error(f'create table st_float_e0_2 using mt_float tags("abc")')
        tdSql.error(f'create table st_float_e0_3 using mt_float tags(" ")')
        tdSql.error(f"create table st_float_e0_4 using mt_float tags('')")

        tdSql.execute(f"create table st_float_e0 using mt_float tags(123)")
        tdSql.execute(f"create table st_float_e1 using mt_float tags(123)")
        tdSql.execute(f"create table st_float_e2 using mt_float tags(123)")
        tdSql.execute(f"create table st_float_e3 using mt_float tags(123)")
        tdSql.execute(f"create table st_float_e4 using mt_float tags(123)")
        tdSql.execute(f"create table st_float_e5 using mt_float tags(123)")
        tdSql.execute(f"create table st_float_e6 using mt_float tags(123)")
        tdSql.execute(f"create table st_float_e7 using mt_float tags(123)")
        tdSql.execute(f"create table st_float_e8 using mt_float tags(123)")
        tdSql.execute(f"create table st_float_e9 using mt_float tags(123)")
        tdSql.execute(f"create table st_float_e10 using mt_float tags(123)")
        tdSql.execute(f"create table st_float_e11 using mt_float tags(123)")
        tdSql.execute(f"create table st_float_e12 using mt_float tags(123)")

        tdSql.error(f"insert into st_float_e0 values(now, 3.50282347e+38)")
        tdSql.error(f"insert into st_float_e1 values(now, -3.50282347e+38)")
        tdSql.error(f"insert into st_float_e2 values(now, 13.40282347e+38)")
        tdSql.error(f"insert into st_float_e3 values(now, -13.40282347e+38)")
        tdSql.execute(f"insert into st_float_e4 values(now, 12.80)")
        tdSql.execute(f"insert into st_float_e5 values(now, -11.80)")
        tdSql.error(f"insert into st_float_e6 values(now, 123abc)")
        tdSql.error(f'insert into st_float_e7 values(now, "123abc")')
        tdSql.error(f"insert into st_float_e9 values(now, abc)")
        tdSql.error(f'insert into st_float_e10 values(now, "abc")')
        tdSql.error(f'insert into st_float_e11 values(now, " ")')
        tdSql.error(f"insert into st_float_e12 values(now, '')")

        tdSql.error(
            f"insert into st_float_e13 using mt_float tags(033) values(now, 3.50282347e+38)"
        )
        tdSql.error(
            f"insert into st_float_e14 using mt_float tags(033) values(now, -3.50282347e+38)"
        )
        tdSql.error(
            f"insert into st_float_e15 using mt_float tags(033) values(now, 13.40282347e+38)"
        )
        tdSql.error(
            f"insert into st_float_e16 using mt_float tags(033) values(now, -13.40282347e+38)"
        )
        tdSql.execute(
            f"insert into st_float_e17 using mt_float tags(033) values(now, 12.80)"
        )
        tdSql.execute(
            f"insert into st_float_e18 using mt_float tags(033) values(now, -11.80)"
        )
        tdSql.error(
            f"insert into st_float_e19 using mt_float tags(033) values(now, 123abc)"
        )
        tdSql.error(
            f'insert into st_float_e20 using mt_float tags(033) values(now, "123abc")'
        )
        tdSql.error(
            f"insert into st_float_e22 using mt_float tags(033) values(now, abc)"
        )
        tdSql.error(
            f'insert into st_float_e23 using mt_float tags(033) values(now, "abc")'
        )
        tdSql.error(
            f'insert into st_float_e24 using mt_float tags(033) values(now, " ")'
        )
        tdSql.error(
            f"insert into st_float_e25_1 using mt_float tags(033) values(now, '')"
        )

        tdSql.error(
            f"insert into st_float_e13 using mt_float tags(3.50282347e+38) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_float_e14 using mt_float tags(-3.50282347e+38) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_float_e15 using mt_float tags(13.40282347e+38) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_float_e16 using mt_float tags(-13.40282347e+38) values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_float_e17 using mt_float tags(12.80) values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_float_e18 using mt_float tags(-11.80) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_float_e19 using mt_float tags(123abc) values(now, -033)"
        )
        tdSql.error(
            f'insert into st_float_e20 using mt_float tags("123abc") values(now, -033)'
        )
        tdSql.error(
            f"insert into st_float_e22 using mt_float tags(abc) values(now, -033)"
        )
        tdSql.error(
            f'insert into st_float_e23 using mt_float tags("abc") values(now, -033)'
        )
        tdSql.error(
            f'insert into st_float_e24 using mt_float tags(" ") values(now, -033)'
        )
        tdSql.error(
            f"insert into st_float_e25_3 using mt_float tags(" ") values(now, -033)"
        )

        tdSql.execute(
            f"insert into st_float_e13 using mt_float tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_float_e14 using mt_float tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_float_e15 using mt_float tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_float_e16 using mt_float tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_float_e17 using mt_float tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_float_e18 using mt_float tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_float_e19 using mt_float tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_float_e20 using mt_float tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_float_e21 using mt_float tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_float_e22 using mt_float tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_float_e23 using mt_float tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_float_e24 using mt_float tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_float_e25 using mt_float tags(033) values(now, 00062)"
        )

        tdSql.error(f"alter table st_float_e13 set tag tagname=3.50282347e+38")
        tdSql.error(f"alter table st_float_e14 set tag tagname=-3.50282347e+38")
        tdSql.error(f"alter table st_float_e15 set tag tagname=13.40282347e+38")
        tdSql.error(f"alter table st_float_e16 set tag tagname=-13.40282347e+38")
        tdSql.error(f"alter table st_float_e19 set tag tagname=123abc")
        tdSql.error(f'alter table st_float_e20 set tag tagname="123abc"')
        tdSql.error(f"alter table st_float_e22 set tag tagname=abc")
        tdSql.error(f'alter table st_float_e23 set tag tagname="abc"')
        tdSql.error(f'alter table st_float_e24 set tag tagname=" "')
        tdSql.error(f"alter table st_float_e25 set tag tagname=''")
