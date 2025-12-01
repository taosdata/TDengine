from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatatypeBigInt:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_datatype_bigint(self):
        """DataTypes: bigint

        1. Create table
        2. Insert data
        3. Auto-create table
        4. Alter tag value
        5. Handle illegal input

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_bigint.sim

        """
        self.create_table()
        self.insert_data()
        self.auto_create_table()
        self.alter_tag_value()
        self.illegal_input()

    def create_table(self):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table mt_bigint (ts timestamp, c bigint) tags(tagname bigint)"
        )

        tdLog.info(f"case 0: static create table for test tag values")

        tdSql.execute(f"create table st_bigint_0 using mt_bigint tags(NULL)")
        tdSql.query(f"show create table st_bigint_0")
        tdSql.query(f"show tags from st_bigint_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_bigint_1 using mt_bigint tags(NULL)")
        tdSql.query(f"show tags from st_bigint_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_bigint_2 using mt_bigint tags('NULL')")
        tdSql.query(f"show tags from st_bigint_2")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_bigint_3 using mt_bigint tags('NULL')")
        tdSql.query(f"show tags from st_bigint_3")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_bigint_4 using mt_bigint tags("NULL")')
        tdSql.query(f"show tags from st_bigint_4")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_bigint_5 using mt_bigint tags("NULL")')
        tdSql.query(f"show tags from st_bigint_5")
        tdSql.checkData(0, 5, None)

        tdSql.execute(
            f"create table st_bigint_6 using mt_bigint tags(-9223372036854775807)"
        )
        tdSql.query(f"show tags from st_bigint_6")
        tdSql.checkData(0, 5, -9223372036854775807)

        tdSql.execute(
            f"create table st_bigint_7 using mt_bigint tags(9223372036854775807)"
        )
        tdSql.query(f"show tags from st_bigint_7")
        tdSql.checkData(0, 5, 9223372036854775807)

        tdSql.execute(f"create table st_bigint_8 using mt_bigint tags(37)")
        tdSql.query(f"show tags from st_bigint_8")
        tdSql.checkData(0, 5, 37)

        tdSql.execute(f"create table st_bigint_9 using mt_bigint tags(-100)")
        tdSql.query(f"show tags from st_bigint_9")
        tdSql.checkData(0, 5, -100)

        tdSql.execute(f"create table st_bigint_10 using mt_bigint tags(+113)")
        tdSql.query(f"show tags from st_bigint_10")
        tdSql.checkData(0, 5, 113)

        tdSql.execute(f"create table st_bigint_11 using mt_bigint tags('-100')")
        tdSql.query(f"show tags from st_bigint_11")
        tdSql.checkData(0, 5, -100)

        tdSql.execute(f'create table st_bigint_12 using mt_bigint tags("+78")')
        tdSql.query(f"show tags from st_bigint_12")
        tdSql.checkData(0, 5, 78)

        tdSql.execute(f"create table st_bigint_13 using mt_bigint tags(+0078)")
        tdSql.query(f"show tags from st_bigint_13")
        tdSql.checkData(0, 5, 78)

        tdSql.execute(f"create table st_bigint_14 using mt_bigint tags(-00078)")
        tdSql.query(f"show tags from st_bigint_14")
        tdSql.checkData(0, 5, -78)

    def insert_data(self):
        tdLog.info(f"case 1: insert values for test column values")

        tdSql.execute(f"insert into st_bigint_0 values(now, NULL)")
        tdSql.query(f"select * from st_bigint_0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_bigint_1 values(now, NULL)")
        tdSql.query(f"select * from st_bigint_1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_bigint_2 values(now, NULL)")
        tdSql.query(f"select * from st_bigint_2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_bigint_3 values(now, NULL)")
        tdSql.query(f"select * from st_bigint_3")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_bigint_4 values(now, NULL)")
        tdSql.query(f"select * from st_bigint_4")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_bigint_5 values(now, NULL)")
        tdSql.query(f"select * from st_bigint_5")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_bigint_6 values(now, 9223372036854775807)")
        tdSql.query(f"select * from st_bigint_6")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 9223372036854775807)

        tdSql.execute(f"insert into st_bigint_7 values(now, -9223372036854775807)")
        tdSql.query(f"select * from st_bigint_7")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -9223372036854775807)

        tdSql.execute(f"insert into st_bigint_8 values(now, +100)")
        tdSql.query(f"select * from st_bigint_8")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 100)

        tdSql.execute(f'insert into st_bigint_9 values(now, "-098")')
        tdSql.query(f"select * from st_bigint_9")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -98)

        tdSql.execute(f"insert into st_bigint_10 values(now, '0')")
        tdSql.query(f"select * from st_bigint_10")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_bigint_11 values(now, -0)")
        tdSql.query(f"select * from st_bigint_11")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f'insert into st_bigint_12 values(now, "+056")')
        tdSql.query(f"select * from st_bigint_12")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 56)

        tdSql.execute(f"insert into st_bigint_13 values(now, +056)")
        tdSql.query(f"select * from st_bigint_13")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 56)

        tdSql.execute(f"insert into st_bigint_14 values(now, -056)")
        tdSql.query(f"select * from st_bigint_14")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, -56)

    def auto_create_table(self):
        tdLog.info(f"case 2: dynamic create table for test tag values")

        tdSql.execute(
            f"insert into st_bigint_16 using mt_bigint tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show create table st_bigint_16")
        tdSql.query(f"show tags from st_bigint_16")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_bigint_16")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_bigint_17 using mt_bigint tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_bigint_17")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_bigint_17")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_bigint_18 using mt_bigint tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_bigint_18")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_bigint_18")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_bigint_19 using mt_bigint tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_bigint_19")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_bigint_19")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_bigint_20 using mt_bigint tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_bigint_20")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_bigint_20")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_bigint_21 using mt_bigint tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_bigint_21")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_bigint_21")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_bigint_22 using mt_bigint tags(9223372036854775807) values(now, 9223372036854775807)"
        )
        tdSql.query(f"show tags from st_bigint_22")
        tdSql.checkData(0, 5, 9223372036854775807)

        tdSql.query(f"select * from st_bigint_22")
        tdSql.checkData(0, 1, 9223372036854775807)

        tdSql.execute(
            f"insert into st_bigint_23 using mt_bigint tags(-9223372036854775807) values(now, -9223372036854775807)"
        )
        tdSql.query(f"show tags from st_bigint_23")
        tdSql.checkData(0, 5, -9223372036854775807)

        tdSql.query(f"select * from st_bigint_23")
        tdSql.checkData(0, 1, -9223372036854775807)

        tdSql.execute(
            f"insert into st_bigint_24 using mt_bigint tags(10) values(now, 10)"
        )
        tdSql.query(f"show tags from st_bigint_24")
        tdSql.checkData(0, 5, 10)

        tdSql.query(f"select * from st_bigint_24")
        tdSql.checkData(0, 1, 10)

        tdSql.execute(
            f'insert into st_bigint_25 using mt_bigint tags("-0") values(now, "-0")'
        )
        tdSql.query(f"show tags from st_bigint_25")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_bigint_25")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_bigint_26 using mt_bigint tags('123') values(now, '123')"
        )
        tdSql.query(f"show tags from st_bigint_26")
        tdSql.checkData(0, 5, 123)

        tdSql.query(f"select * from st_bigint_26")
        tdSql.checkData(0, 1, 123)

        tdSql.execute(
            f"insert into st_bigint_27 using mt_bigint tags(+056) values(now, +00056)"
        )
        tdSql.query(f"show tags from st_bigint_27")
        tdSql.checkData(0, 5, 56)

        tdSql.query(f"select * from st_bigint_27")
        tdSql.checkData(0, 1, 56)

        tdSql.execute(
            f"insert into st_bigint_28 using mt_bigint tags(-056) values(now, -0056)"
        )
        tdSql.query(f"show tags from st_bigint_28")
        tdSql.checkData(0, 5, -56)

        tdSql.query(f"select * from st_bigint_28")
        tdSql.checkData(0, 1, -56)

    def alter_tag_value(self):
        tdLog.info(f"case 3: alter tag value")

        tdSql.execute(f"alter table st_bigint_0 set tag tagname=9223372036854775807")
        tdSql.query(f"show tags from  st_bigint_0")
        tdSql.checkData(0, 5, 9223372036854775807)

        tdSql.execute(f"alter table st_bigint_0 set tag tagname=-9223372036854775807")
        tdSql.query(f"show tags from  st_bigint_0")
        tdSql.checkData(0, 5, -9223372036854775807)

        tdSql.execute(f"alter table st_bigint_0 set tag tagname=+100")
        tdSql.query(f"show tags from  st_bigint_0")
        tdSql.checkData(0, 5, 100)

        tdSql.execute(f"alter table st_bigint_0 set tag tagname=-33")
        tdSql.query(f"show tags from  st_bigint_0")
        tdSql.checkData(0, 5, -33)

        tdSql.execute(f"alter table st_bigint_0 set tag tagname='+98'")
        tdSql.query(f"show tags from  st_bigint_0")
        tdSql.checkData(0, 5, 98)

        tdSql.execute(f"alter table st_bigint_0 set tag tagname='-076'")
        tdSql.query(f"show tags from  st_bigint_0")
        tdSql.checkData(0, 5, -76)

        tdSql.execute(f"alter table st_bigint_0 set tag tagname=+0012")
        tdSql.query(f"show tags from  st_bigint_0")
        tdSql.checkData(0, 5, 12)

        tdSql.execute(f"alter table st_bigint_0 set tag tagname=-00063")
        tdSql.query(f"show tags from  st_bigint_0")
        tdSql.checkData(0, 5, -63)

    def illegal_input(self):
        tdLog.info(f"case 4: illegal input")

        tdSql.error(
            f"create table st_bigint_e0  using mt_bigint tags(9223372036854775808)"
        )
        tdSql.execute(
            f"create table st_bigint_e0_1 using mt_bigint tags(-9223372036854775808)"
        )
        tdSql.error(
            f"create table st_bigint_e0_2 using mt_bigint tags(92233720368547758080)"
        )
        tdSql.error(
            f"create table st_bigint_e0_3 using mt_bigint tags(-9223372036854775809)"
        )

        # truncate integer part
        tdSql.execute(f"create table st_bigint_e0 using mt_bigint tags(12.80)")
        tdSql.execute(f"drop table st_bigint_e0")
        tdSql.execute(f"create table st_bigint_e0 using mt_bigint tags(-11.80)")
        tdSql.execute(f"drop table st_bigint_e0")
        tdSql.error(f"create table st_bigint_e0 using mt_bigint tags(123abc)")
        tdSql.error(f'create table st_bigint_e0 using mt_bigint tags("123abc")')
        tdSql.error(f"create table st_bigint_e0 using mt_bigint tags(abc)")
        tdSql.error(f'create table st_bigint_e0 using mt_bigint tags("abc")')
        tdSql.error(f'create table st_bigint_e0 using mt_bigint tags(" ")')
        tdSql.error(f"create table st_bigint_e0_error using mt_bigint tags('')")

        tdSql.execute(f"create table st_bigint_e0 using mt_bigint tags(123)")
        tdSql.execute(f"create table st_bigint_e1 using mt_bigint tags(123)")
        tdSql.execute(f"create table st_bigint_e2 using mt_bigint tags(123)")
        tdSql.execute(f"create table st_bigint_e3 using mt_bigint tags(123)")
        tdSql.execute(f"create table st_bigint_e4 using mt_bigint tags(123)")
        tdSql.execute(f"create table st_bigint_e5 using mt_bigint tags(123)")
        tdSql.execute(f"create table st_bigint_e6 using mt_bigint tags(123)")
        tdSql.execute(f"create table st_bigint_e7 using mt_bigint tags(123)")
        tdSql.execute(f"create table st_bigint_e8 using mt_bigint tags(123)")
        tdSql.execute(f"create table st_bigint_e9 using mt_bigint tags(123)")
        tdSql.execute(f"create table st_bigint_e10 using mt_bigint tags(123)")
        tdSql.execute(f"create table st_bigint_e11 using mt_bigint tags(123)")
        tdSql.execute(f"create table st_bigint_e12 using mt_bigint tags(123)")

        tdSql.error(f"insert into st_bigint_e0 values(now, 9223372036854775808)")
        tdSql.execute(f"insert into st_bigint_e1 values(now, -9223372036854775808)")
        tdSql.error(f"insert into st_bigint_e2 values(now, 9223372036854775809)")
        tdSql.execute(f"insert into st_bigint_e3 values(now, -9223372036854775808)")
        tdSql.execute(f"insert into st_bigint_e4 values(now, 922337203.6854775808)")
        tdSql.execute(f"insert into st_bigint_e5 values(now, -922337203685477580.9)")
        tdSql.error(f"insert into st_bigint_e6 values(now, 123abc)")
        tdSql.error(f'insert into st_bigint_e7 values(now, "123abc")')
        tdSql.error(f"insert into st_bigint_e9 values(now, abc)")
        tdSql.error(f'insert into st_bigint_e10 values(now, "abc")')
        tdSql.error(f'insert into st_bigint_e11 values(now, " ")')
        tdSql.error(f"insert into st_bigint_e12 values(now, '')")

        tdSql.error(
            f"insert into st_bigint_e13 using mt_bigint tags(033) values(now, 9223372036854775808)"
        )
        tdSql.execute(
            f"insert into st_bigint_e14 using mt_bigint tags(033) values(now, -9223372036854775808)"
        )
        tdSql.error(
            f"insert into st_bigint_e15 using mt_bigint tags(033) values(now, 9223372036854775818)"
        )
        tdSql.error(
            f"insert into st_bigint_e16 using mt_bigint tags(033) values(now, -9923372036854775808)"
        )
        tdSql.execute(
            f"insert into st_bigint_e17 using mt_bigint tags(033) values(now, 92233720368547758.08)"
        )
        tdSql.execute(
            f"insert into st_bigint_e18 using mt_bigint tags(033) values(now, -92233720368547.75808)"
        )
        tdSql.error(
            f"insert into st_bigint_e19 using mt_bigint tags(033) values(now, 123abc)"
        )
        tdSql.error(
            f'insert into st_bigint_e20 using mt_bigint tags(033) values(now, "123abc")'
        )
        tdSql.error(
            f"insert into st_bigint_e22 using mt_bigint tags(033) values(now, abc)"
        )
        tdSql.error(
            f'insert into st_bigint_e23 using mt_bigint tags(033) values(now, "abc")'
        )
        tdSql.error(
            f'insert into st_bigint_e24 using mt_bigint tags(033) values(now, " ")'
        )
        tdSql.error(
            f"insert into st_bigint_e25 using mt_bigint tags(033) values(now, '')"
        )

        tdSql.error(
            f"insert into st_bigint_e13_0 using mt_bigint tags(9223372036854775808) values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_bigint_e14_0 using mt_bigint tags(-9223372036854775808) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_bigint_e15_0 using mt_bigint tags(9223372036854775809) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_bigint_e16_0 using mt_bigint tags(-9223372036854775898) values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_bigint_e17 using mt_bigint tags(12.80) values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_bigint_e18 using mt_bigint tags(-11.80) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_bigint_e19 using mt_bigint tags(123abc) values(now, -033)"
        )
        tdSql.error(
            f'insert into st_bigint_e20 using mt_bigint tags("123abc") values(now, -033)'
        )
        tdSql.error(
            f"insert into st_bigint_e22 using mt_bigint tags(abc) values(now, -033)"
        )
        tdSql.error(
            f'insert into st_bigint_e23 using mt_bigint tags("abc") values(now, -033)'
        )
        tdSql.error(
            f'insert into st_bigint_e24 using mt_bigint tags(" ") values(now, -033)'
        )
        tdSql.error(
            f"insert into st_bigint_e25 using mt_bigint tags(" ") values(now, -033)"
        )

        tdSql.execute(
            f"insert into st_bigint_e13 using mt_bigint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_bigint_e14 using mt_bigint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_bigint_e15 using mt_bigint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_bigint_e16 using mt_bigint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_bigint_e17 using mt_bigint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_bigint_e18 using mt_bigint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_bigint_e19 using mt_bigint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_bigint_e20 using mt_bigint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_bigint_e21 using mt_bigint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_bigint_e22 using mt_bigint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_bigint_e23 using mt_bigint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_bigint_e24 using mt_bigint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_bigint_e25 using mt_bigint tags(033) values(now, 00062)"
        )

        tdSql.error(f"alter table st_bigint_e13 set tag tagname=9223372036854775808")
        tdSql.execute(f"alter table st_bigint_e13 set tag tagname=9223372036854775807")
        tdSql.error(f"alter table st_bigint_e14 set tag tagname=-9223372036854775809")
        tdSql.execute(f"alter table st_bigint_e14 set tag tagname=-9223372036854775808")
        tdSql.error(f"alter table st_bigint_e15 set tag tagname=92233720368547758080")
        tdSql.error(f"alter table st_bigint_e16 set tag tagname=-92233720368547758080")
        tdSql.error(f"alter table st_bigint_e19 set tag tagname=123abc")
        tdSql.error(f'alter table st_bigint_e20 set tag tagname="123abc"')
        tdSql.error(f"alter table st_bigint_e22 set tag tagname=abc")
        tdSql.error(f'alter table st_bigint_e23 set tag tagname="abc"')
        tdSql.error(f'alter table st_bigint_e24 set tag tagname=" "')
        tdSql.error(f"alter table st_bigint_e25 set tag tagname=''")
