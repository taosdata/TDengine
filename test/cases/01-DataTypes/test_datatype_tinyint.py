from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatatypeTinyint:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_datatype_tinyint(self):
        """DataTypes: tinyint

        1. Create table
        2. Insert data
        3. Auto-create table
        4. Alter tag value
        5. Handle illegal input

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_tinyint.sim

        """
        self.create_table()
        self.insert_data()
        self.auto_create_table()
        self.alter_tag_value()
        self.illegal_input()

    def create_table(self):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table mt_tinyint (ts timestamp, c tinyint) tags(tagname tinyint)"
        )

        tdLog.info(f"case 0: static create table for test tag values")
        tdSql.execute(f"create table st_tinyint_0 using mt_tinyint tags(NULL)")
        tdSql.query(f"show create table st_tinyint_0")
        tdSql.query(f"show tags from st_tinyint_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_tinyint_1 using mt_tinyint tags(NULL)")
        tdSql.query(f"show tags from st_tinyint_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_tinyint_2 using mt_tinyint tags('NULL')")
        tdSql.query(f"show tags from st_tinyint_2")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_tinyint_3 using mt_tinyint tags('NULL')")
        tdSql.query(f"show tags from st_tinyint_3")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_tinyint_4 using mt_tinyint tags("NULL")')
        tdSql.query(f"show tags from st_tinyint_4")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_tinyint_5 using mt_tinyint tags("NULL")')
        tdSql.query(f"show tags from st_tinyint_5")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_tinyint_6 using mt_tinyint tags(-127)")
        tdSql.query(f"show tags from st_tinyint_6")
        tdSql.checkData(0, 5, -127)

        tdSql.execute(f"create table st_tinyint_7 using mt_tinyint tags(127)")
        tdSql.query(f"show tags from st_tinyint_7")
        tdSql.checkData(0, 5, 127)

        tdSql.execute(f"create table st_tinyint_8 using mt_tinyint tags(37)")
        tdSql.query(f"show tags from st_tinyint_8")
        tdSql.checkData(0, 5, 37)

        tdSql.execute(f"create table st_tinyint_9 using mt_tinyint tags(-100)")
        tdSql.query(f"show tags from st_tinyint_9")
        tdSql.checkData(0, 5, -100)

        tdSql.execute(f"create table st_tinyint_10 using mt_tinyint tags(+113)")
        tdSql.query(f"show tags from st_tinyint_10")
        tdSql.checkData(0, 5, 113)

        tdSql.execute(f"create table st_tinyint_11 using mt_tinyint tags('-100')")
        tdSql.query(f"show tags from st_tinyint_11")
        tdSql.checkData(0, 5, -100)

        tdSql.execute(f'create table st_tinyint_12 using mt_tinyint tags("+78")')
        tdSql.query(f"show tags from st_tinyint_12")
        tdSql.checkData(0, 5, 78)

        tdSql.execute(f"create table st_tinyint_13 using mt_tinyint tags(+0078)")
        tdSql.query(f"show tags from st_tinyint_13")
        tdSql.checkData(0, 5, 78)

        tdSql.execute(f"create table st_tinyint_14 using mt_tinyint tags(-00078)")
        tdSql.query(f"show tags from st_tinyint_14")
        tdSql.checkData(0, 5, -78)

    def insert_data(self):
        tdLog.info(f"case 1: insert values for test column values")

        tdSql.execute(f"insert into st_tinyint_0 values(now, NULL)")
        tdSql.query(f"select * from st_tinyint_0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_tinyint_1 values(now, NULL)")
        tdSql.query(f"select * from st_tinyint_1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_tinyint_2 values(now, NULL)")
        tdSql.query(f"select * from st_tinyint_2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_tinyint_3 values(now, NULL)")
        tdSql.query(f"select * from st_tinyint_3")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_tinyint_4 values(now, NULL)")
        tdSql.query(f"select * from st_tinyint_4")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_tinyint_5 values(now, NULL)")
        tdSql.query(f"select * from st_tinyint_5")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_tinyint_6 values(now, 127)")
        tdSql.query(f"select * from st_tinyint_6")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 127)

        tdSql.execute(f"insert into st_tinyint_7 values(now, -127)")
        tdSql.query(f"select * from st_tinyint_7")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -127)

        tdSql.execute(f"insert into st_tinyint_8 values(now, +100)")
        tdSql.query(f"select * from st_tinyint_8")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 100)

        tdSql.execute(f'insert into st_tinyint_9 values(now, "-098")')
        tdSql.query(f"select * from st_tinyint_9")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -98)

        tdSql.execute(f"insert into st_tinyint_10 values(now, '0')")
        tdSql.query(f"select * from st_tinyint_10")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_tinyint_11 values(now, -0)")
        tdSql.query(f"select * from st_tinyint_11")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f'insert into st_tinyint_12 values(now, "+056")')
        tdSql.query(f"select * from st_tinyint_12")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 56)

        tdSql.execute(f"insert into st_tinyint_13 values(now, +056)")
        tdSql.query(f"select * from st_tinyint_13")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 56)

        tdSql.execute(f"insert into st_tinyint_14 values(now, -056)")
        tdSql.query(f"select * from st_tinyint_14")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, -56)

    def auto_create_table(self):
        tdLog.info(f"case 2: dynamic create table for test tag values")

        tdSql.execute(
            f"insert into st_tinyint_16 using mt_tinyint tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show create table st_tinyint_16")
        tdSql.query(f"show tags from st_tinyint_16")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_tinyint_16")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_tinyint_17 using mt_tinyint tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_tinyint_17")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_tinyint_17")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_tinyint_18 using mt_tinyint tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_tinyint_18")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_tinyint_18")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_tinyint_19 using mt_tinyint tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_tinyint_19")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_tinyint_19")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_tinyint_20 using mt_tinyint tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_tinyint_20")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_tinyint_20")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_tinyint_21 using mt_tinyint tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_tinyint_21")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_tinyint_21")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_tinyint_22 using mt_tinyint tags(127) values(now, 127)"
        )
        tdSql.query(f"show tags from st_tinyint_22")
        tdSql.checkData(0, 5, 127)

        tdSql.query(f"select * from st_tinyint_22")
        tdSql.checkData(0, 1, 127)

        tdSql.execute(
            f"insert into st_tinyint_23 using mt_tinyint tags(-127) values(now, -127)"
        )
        tdSql.query(f"show tags from st_tinyint_23")
        tdSql.checkData(0, 5, -127)

        tdSql.query(f"select * from st_tinyint_23")
        tdSql.checkData(0, 1, -127)

        tdSql.execute(
            f"insert into st_tinyint_24 using mt_tinyint tags(10) values(now, 10)"
        )
        tdSql.query(f"show tags from st_tinyint_24")
        tdSql.checkData(0, 5, 10)

        tdSql.query(f"select * from st_tinyint_24")
        tdSql.checkData(0, 1, 10)

        tdSql.execute(
            f'insert into st_tinyint_25 using mt_tinyint tags("-0") values(now, "-0")'
        )
        tdSql.query(f"show tags from st_tinyint_25")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select * from st_tinyint_25")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_tinyint_26 using mt_tinyint tags('123') values(now, '123')"
        )
        tdSql.query(f"show tags from st_tinyint_26")
        tdSql.checkData(0, 5, 123)

        tdSql.query(f"select * from st_tinyint_26")
        tdSql.checkData(0, 1, 123)

        tdSql.execute(
            f"insert into st_tinyint_27 using mt_tinyint tags(+056) values(now, +00056)"
        )
        tdSql.query(f"show tags from st_tinyint_27")
        tdSql.checkData(0, 5, 56)

        tdSql.query(f"select * from st_tinyint_27")
        tdSql.checkData(0, 1, 56)

        tdSql.execute(
            f"insert into st_tinyint_28 using mt_tinyint tags(-056) values(now, -0056)"
        )
        tdSql.query(f"show tags from st_tinyint_28")
        tdSql.checkData(0, 5, -56)

        tdSql.query(f"select * from st_tinyint_28")
        tdSql.checkData(0, 1, -56)

    def alter_tag_value(self):
        tdLog.info(f"case 3: alter tag value")

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname=127")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, 127)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname=-127")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, -127)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname=+100")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, 100)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname=-33")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, -33)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname='+98'")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, 98)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname='-076'")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, -76)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname=+0012")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, 12)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname=-00063")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, -63)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname=127")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, 127)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname=-127")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, -127)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname=+100")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, 100)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname=-33")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, -33)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname='+98'")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, 98)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname='-076'")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, -76)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname=+0012")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, 12)

        tdSql.execute(f"alter table st_tinyint_0 set tag tagname=-00063")
        tdSql.query(f"show tags from  st_tinyint_0")
        tdSql.checkData(0, 5, -63)

    def illegal_input(self):
        tdLog.info(f"case 4: illegal input")

        tdSql.error(f"create table st_tinyint_e0 using mt_tinyint tags(128)")
        tdSql.execute(f"create table st_tinyint_e0_1 using mt_tinyint tags(-128)")
        tdSql.error(f"create table st_tinyint_e0 using mt_tinyint tags(1280)")
        tdSql.error(f"create table st_tinyint_e0 using mt_tinyint tags(-1280)")
        tdSql.error(f"create table st_tinyint_e0 using mt_tinyint tags(123abc)")
        tdSql.error(f'create table st_tinyint_e0 using mt_tinyint tags("123abc")')
        tdSql.error(f"create table st_tinyint_e0 using mt_tinyint tags(abc)")
        tdSql.error(f'create table st_tinyint_e0 using mt_tinyint tags("abc")')
        tdSql.error(f'create table st_tinyint_e0 using mt_tinyint tags(" ")')
        tdSql.error(f"create table st_tinyint_e0_2 using mt_tinyint tags('')")

        tdSql.execute(f"create table st_tinyint_e0 using mt_tinyint tags(123)")
        tdSql.execute(f"create table st_tinyint_e1 using mt_tinyint tags(123)")
        tdSql.execute(f"create table st_tinyint_e2 using mt_tinyint tags(123)")
        tdSql.execute(f"create table st_tinyint_e3 using mt_tinyint tags(123)")
        tdSql.execute(f"create table st_tinyint_e4 using mt_tinyint tags(123)")
        tdSql.execute(f"create table st_tinyint_e5 using mt_tinyint tags(123)")
        tdSql.execute(f"create table st_tinyint_e6 using mt_tinyint tags(123)")
        tdSql.execute(f"create table st_tinyint_e7 using mt_tinyint tags(123)")
        tdSql.execute(f"create table st_tinyint_e8 using mt_tinyint tags(123)")
        tdSql.execute(f"create table st_tinyint_e9 using mt_tinyint tags(123)")
        tdSql.execute(f"create table st_tinyint_e10 using mt_tinyint tags(123)")
        tdSql.execute(f"create table st_tinyint_e11 using mt_tinyint tags(123)")
        tdSql.execute(f"create table st_tinyint_e12 using mt_tinyint tags(123)")

        tdSql.error(f"insert into st_tinyint_e0 values(now, 128)")
        tdSql.execute(f"insert into st_tinyint_e1 values(now, -128)")
        tdSql.error(f"insert into st_tinyint_e2 values(now, 1280)")
        tdSql.error(f"insert into st_tinyint_e3 values(now, -1280)")
        tdSql.error(f"insert into st_tinyint_e6 values(now, 123abc)")
        tdSql.error(f'insert into st_tinyint_e7 values(now, "123abc")')
        tdSql.error(f"insert into st_tinyint_e9 values(now, abc)")
        tdSql.error(f'insert into st_tinyint_e10 values(now, "abc")')
        tdSql.error(f'insert into st_tinyint_e11 values(now, " ")')
        tdSql.error(f"insert into st_tinyint_e12 values(now, '')")

        tdSql.error(
            f"insert into st_tinyint_e13 using mt_tinyint tags(033) values(now, 128)"
        )
        tdSql.execute(
            f"insert into st_tinyint_e14_1 using mt_tinyint tags(033) values(now, -128)"
        )
        tdSql.error(
            f"insert into st_tinyint_e15 using mt_tinyint tags(033) values(now, 1280)"
        )
        tdSql.error(
            f"insert into st_tinyint_e16 using mt_tinyint tags(033) values(now, -1280)"
        )
        tdSql.error(
            f"insert into st_tinyint_e19 using mt_tinyint tags(033) values(now, 123abc)"
        )
        tdSql.error(
            f'insert into st_tinyint_e20 using mt_tinyint tags(033) values(now, "123abc")'
        )
        tdSql.error(
            f"insert into st_tinyint_e22 using mt_tinyint tags(033) values(now, abc)"
        )
        tdSql.error(
            f'insert into st_tinyint_e23 using mt_tinyint tags(033) values(now, "abc")'
        )
        tdSql.error(
            f'insert into st_tinyint_e24 using mt_tinyint tags(033) values(now, " ")'
        )
        tdSql.error(
            f"insert into st_tinyint_e25_2 using mt_tinyint tags(033) values(now, '')"
        )

        tdSql.error(
            f"insert into st_tinyint_e13 using mt_tinyint tags(128) values(now, -033)"
        )
        tdSql.execute(
            f"insert into st_tinyint_e14 using mt_tinyint tags(-128) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_tinyint_e15 using mt_tinyint tags(1280) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_tinyint_e16 using mt_tinyint tags(-1280) values(now, -033)"
        )
        tdSql.error(
            f"insert into st_tinyint_e19 using mt_tinyint tags(123abc) values(now, -033)"
        )
        tdSql.error(
            f'insert into st_tinyint_e20 using mt_tinyint tags("123abc") values(now, -033)'
        )
        tdSql.error(
            f"insert into st_tinyint_e22 using mt_tinyint tags(abc) values(now, -033)"
        )
        tdSql.error(
            f'insert into st_tinyint_e23 using mt_tinyint tags("abc") values(now, -033)'
        )
        tdSql.error(
            f'insert into st_tinyint_e24 using mt_tinyint tags(" ") values(now, -033)'
        )
        tdSql.error(
            f"insert into st_tinyint_e25 using mt_tinyint tags('') values(now, -033)"
        )

        tdSql.execute(
            f"insert into st_tinyint_e13 using mt_tinyint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_tinyint_e14 using mt_tinyint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_tinyint_e15 using mt_tinyint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_tinyint_e16 using mt_tinyint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_tinyint_e17 using mt_tinyint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_tinyint_e18 using mt_tinyint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_tinyint_e19 using mt_tinyint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_tinyint_e20 using mt_tinyint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_tinyint_e21 using mt_tinyint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_tinyint_e22 using mt_tinyint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_tinyint_e23 using mt_tinyint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_tinyint_e24 using mt_tinyint tags(033) values(now, 00062)"
        )
        tdSql.execute(
            f"insert into st_tinyint_e25 using mt_tinyint tags(033) values(now, 00062)"
        )

        tdSql.error(f"alter table st_tinyint_e13 set tag tagname=128")
        tdSql.execute(f"alter table st_tinyint_e14 set tag tagname=-128")
        tdSql.error(f"alter table st_tinyint_e15 set tag tagname=1280")
        tdSql.error(f"alter table st_tinyint_e16 set tag tagname=-1280")
        tdSql.error(f"alter table st_tinyint_e19 set tag tagname=123abc")
        tdSql.error(f'alter table st_tinyint_e20 set tag tagname="123abc"')
        tdSql.error(f"alter table st_tinyint_e22 set tag tagname=abc")
        tdSql.error(f'alter table st_tinyint_e23 set tag tagname="abc"')
        tdSql.error(f'alter table st_tinyint_e24 set tag tagname=" "')
        tdSql.error(f"alter table st_tinyint_e25 set tag tagname=''")
