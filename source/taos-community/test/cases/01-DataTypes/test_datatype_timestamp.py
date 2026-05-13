from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatatypeTimestamp:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_datatype_timestamp(self):
        """DataTypes: timestamp

        1. Create table
        2. Insert data
        3. Auto-create table
        4. Alter tag value
        5. Handle illegal input

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_timestamp.sim

        """
        self.create_table()
        self.insert_data()
        self.auto_create_table()
        self.alter_tag_value()
        self.illegal_input()

    def create_table(self):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table mt_timestamp (ts timestamp, c timestamp) tags(tagname timestamp)"
        )

        tdLog.info(f"case 0: static create table for test tag values")

        tdSql.execute(f"create table st_timestamp_0 using mt_timestamp tags(NULL)")
        tdSql.execute(f"show create table st_timestamp_0")
        tdSql.query(f"show tags from st_timestamp_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_timestamp_1 using mt_timestamp tags(NULL)")
        tdSql.query(f"show tags from st_timestamp_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_timestamp_2 using mt_timestamp tags('NULL')")
        tdSql.query(f"show tags from st_timestamp_2")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_timestamp_3 using mt_timestamp tags('NULL')")
        tdSql.query(f"show tags from st_timestamp_3")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_timestamp_4 using mt_timestamp tags("NULL")')
        tdSql.query(f"show tags from st_timestamp_4")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_timestamp_5 using mt_timestamp tags("NULL")')
        tdSql.query(f"show tags from st_timestamp_5")
        tdSql.checkData(0, 5, None)

        tdSql.execute(
            f"create table st_timestamp_6 using mt_timestamp tags(-2147483647)"
        )
        tdSql.query(f"show tags from st_timestamp_6")
        tdSql.checkData(0, 5, -2147483647)

        tdSql.execute(
            f"create table st_timestamp_7 using mt_timestamp tags(2147483647)"
        )
        tdSql.query(f"show tags from st_timestamp_7")
        tdSql.checkData(0, 5, 2147483647)

        tdSql.execute(f"create table st_timestamp_8 using mt_timestamp tags(37)")
        tdSql.query(f"show tags from st_timestamp_8")
        tdSql.checkData(0, 5, 37)

        tdSql.execute(f"create table st_timestamp_9 using mt_timestamp tags(-100)")
        tdSql.query(f"show tags from st_timestamp_9")
        tdSql.checkData(0, 5, -100)

        tdSql.execute(f"create table st_timestamp_10 using mt_timestamp tags(+113)")
        tdSql.query(f"show tags from st_timestamp_10")
        tdSql.checkData(0, 5, 113)

        tdSql.execute(f"create table st_timestamp_11 using mt_timestamp tags('-100')")
        tdSql.query(f"show tags from st_timestamp_11")
        tdSql.checkData(0, 5, -100)

        tdSql.execute(f'create table st_timestamp_12 using mt_timestamp tags("-0")')
        tdSql.query(f"show tags from st_timestamp_12")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"create table st_timestamp_13 using mt_timestamp tags(+0078)")
        tdSql.query(f"show tags from st_timestamp_13")
        tdSql.checkData(0, 5, 78)

        tdSql.execute(f"create table st_timestamp_14 using mt_timestamp tags(-00078)")
        tdSql.query(f"show tags from st_timestamp_14")
        tdSql.checkData(0, 5, -78)

        tdSql.execute(f'create table st_timestamp_15 using mt_timestamp tags("0x01")')
        tdSql.query(f"show tags from st_timestamp_15")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'create table st_timestamp_16 using mt_timestamp tags("0b01")')
        tdSql.query(f"show tags from st_timestamp_16")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'create table st_timestamp_17 using mt_timestamp tags("+0x01")')
        tdSql.query(f"show tags from st_timestamp_17")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'create table st_timestamp_18 using mt_timestamp tags("-0b01")')
        tdSql.query(f"show tags from st_timestamp_18")
        tdSql.checkData(0, 5, -1)

        tdSql.execute(f"create table st_timestamp_19 using mt_timestamp tags( 0x01)")
        tdSql.query(f"show tags from st_timestamp_19")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"create table st_timestamp_20 using mt_timestamp tags(0b01 )")
        tdSql.query(f"show tags from st_timestamp_20")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"create table st_timestamp_21 using mt_timestamp tags(+0x01)")
        tdSql.query(f"show tags from st_timestamp_21")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"create table st_timestamp_22 using mt_timestamp tags( -0b01 )")
        tdSql.query(f"show tags from st_timestamp_22")
        tdSql.checkData(0, 5, -1)

        tdSql.execute(f"create table st_timestamp_23 using mt_timestamp tags(1+ 1d )")
        tdSql.query(f"show tags from st_timestamp_23")
        tdSql.checkData(0, 5, 86400001)

        tdSql.execute(f"create table st_timestamp_24 using mt_timestamp tags(-0 + 1d)")
        tdSql.query(f"show tags from st_timestamp_24")
        tdSql.checkData(0, 5, 86400000)

        tdSql.execute(f'create table st_timestamp_25 using mt_timestamp tags("-0" -1s)')
        tdSql.query(f"show tags from st_timestamp_25")
        tdSql.checkData(0, 5, -1000)

        tdSql.execute(f"create table st_timestamp_26 using mt_timestamp tags(0b01 -1a)")
        tdSql.query(f"show tags from st_timestamp_26")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"create table st_timestamp_27 using mt_timestamp tags(0b01 -1s)")
        tdSql.query(f"show tags from st_timestamp_27")
        tdSql.checkData(0, 5, -999)

        tdSql.execute(
            f'create table st_timestamp_28 using mt_timestamp tags("0x01" +1u)'
        )
        tdSql.query(f"show tags from st_timestamp_28")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"create table st_timestamp_29 using mt_timestamp tags(0x01 +1b)")
        tdSql.query(f"show tags from st_timestamp_29")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(
            f"create table st_timestamp_30 using mt_timestamp tags(-0b00 -0a)"
        )
        tdSql.query(f"show tags from st_timestamp_30")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(
            f'create table st_timestamp_31 using mt_timestamp tags("-0x00" +1u)'
        )
        tdSql.query(f"show tags from st_timestamp_31")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(
            f"create table st_timestamp_32 using mt_timestamp tags(-0x00 +1b)"
        )
        tdSql.query(f"show tags from st_timestamp_32")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"create table st_timestamp_33 using mt_timestamp tags(now +1b)")
        tdSql.query(f"show tags from st_timestamp_33")
        tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

        tdSql.execute(
            f'create table st_timestamp_34 using mt_timestamp tags("now()" +1b)'
        )
        tdSql.query(f"show tags from st_timestamp_34")
        tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

        tdSql.execute(
            f"create table st_timestamp_35 using mt_timestamp tags(today() +1d)"
        )
        tdSql.query(f"show tags from st_timestamp_35")
        tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

        tdSql.execute(
            f'create table st_timestamp_36 using mt_timestamp tags("today()" +1d)'
        )
        tdSql.query(f"show tags from st_timestamp_36")
        tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

    def insert_data(self):
        tdLog.info(f"case 1: insert values for test column values")

        tdSql.execute(f"insert into st_timestamp_0 values(now,NULL)")
        tdSql.query(f"select * from st_timestamp_0")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_timestamp_1 values(now,NULL)")
        tdSql.query(f"select * from st_timestamp_1")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_timestamp_2 values(now,'NULL')")
        tdSql.query(f"select * from st_timestamp_2")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_timestamp_3 values(now,'NULL')")
        tdSql.query(f"select * from st_timestamp_3")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f'insert into st_timestamp_4 values(now,"NULL")')
        tdSql.query(f"select * from st_timestamp_4")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f'insert into st_timestamp_5 values(now,"NULL")')
        tdSql.query(f"select * from st_timestamp_5")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_timestamp_6 values(now,-2147483647)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_6")
        tdSql.checkData(0, 1, -2147483647)

        tdSql.execute(f"insert into st_timestamp_7 values(now,2147483647)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_7")
        tdSql.checkData(0, 1, 2147483647)

        tdSql.execute(f"insert into st_timestamp_8 values(now,37)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_8")
        tdSql.checkData(0, 1, 37)

        tdSql.execute(f"insert into st_timestamp_9 values(now,-100)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_9")
        tdSql.checkData(0, 1, -100)

        tdSql.execute(f"insert into st_timestamp_10 values(now,+113)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_10")
        tdSql.checkData(0, 1, 113)

        tdSql.execute(f"insert into st_timestamp_11 values(now,'-100')")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_11")
        tdSql.checkData(0, 1, -100)

        tdSql.execute(f'insert into st_timestamp_12 values(now,"-0")')
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_12")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_timestamp_13 values(now,+0078)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_13")
        tdSql.checkData(0, 1, 78)

        tdSql.execute(f"insert into st_timestamp_14 values(now,-00078)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_14")
        tdSql.checkData(0, 1, -78)

        tdSql.execute(f'insert into st_timestamp_15 values(now,"0x01")')
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_15")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_timestamp_16 values(now,"0b01")')
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_16")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_timestamp_17 values(now,"+0x01")')
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_17")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_timestamp_18 values(now,"-0b01")')
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_18")
        tdSql.checkData(0, 1, -1)

        tdSql.execute(f"insert into st_timestamp_19 values(now, 0x01)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_19")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_timestamp_20 values(now,0b01 )")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_20")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_timestamp_21 values(now,+0x01)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_21")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_timestamp_22 values(now, -0b01 )")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_22")
        tdSql.checkData(0, 1, -1)

        tdSql.execute(f"insert into st_timestamp_23 values(now,1+ 1d )")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_23")
        tdSql.checkData(0, 1, 86400001)

        tdSql.execute(f"insert into st_timestamp_24 values(now,-0 + 1d)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_24")
        tdSql.checkData(0, 1, 86400000)

        tdSql.execute(f'insert into st_timestamp_25 values(now,"-0" -1s)')
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_25")
        tdSql.checkData(0, 1, -1000)

        tdSql.execute(f"insert into st_timestamp_26 values(now,0b01 -1a)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_26")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_timestamp_27 values(now,+0b01 -1s)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_27")
        tdSql.checkData(0, 1, -999)

        tdSql.execute(f'insert into st_timestamp_28 values(now,"+0x01" +1u)')
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_28")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_timestamp_29 values(now,0x01 +1b)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_29")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into  st_timestamp_30 values(now,-0b00 -0a)")
        tdSql.query(f"show tags from st_timestamp_30")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f'insert into  st_timestamp_31 values(now,"-0x00" +1u)')
        tdSql.query(f"show tags from st_timestamp_31")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"insert into st_timestamp_32 values(now,-0x00 +1b)")
        tdSql.query(f"show tags from st_timestamp_32")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"insert into st_timestamp_33 values(now,now +1b)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_33")
        tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

        tdSql.execute(f'insert into st_timestamp_34 values(now,"now()" +1b)')
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_34")
        tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

        tdSql.execute(f"insert into st_timestamp_35 values(now,today() +1d)")
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_35")
        tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

        tdSql.execute(f'insert into st_timestamp_36 values(now,"today()" +1d)')
        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_36")
        tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

    def auto_create_table(self):
        tdLog.info(f"case 2: dynamic create table for test tag values")
        tdSql.execute(
            f"insert into st_timestamp_100 using mt_timestamp tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_timestamp_100")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_timestamp_100")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_timestamp_101 using mt_timestamp tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_timestamp_101")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_timestamp_101")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_timestamp_102 using mt_timestamp tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_timestamp_102")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_timestamp_102")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_timestamp_103 using mt_timestamp tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_timestamp_103")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_timestamp_103")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_timestamp_104 using mt_timestamp tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_timestamp_104")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_timestamp_104")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_timestamp_105 using mt_timestamp tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_timestamp_105")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_timestamp_105")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_timestamp_106 using mt_timestamp tags(-2147483647) values(now, -2147483647)"
        )
        tdSql.query(f"show tags from st_timestamp_106")
        tdSql.checkData(0, 5, -2147483647)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_106")
        tdSql.checkData(0, 1, -2147483647)

        tdSql.execute(
            f"insert into st_timestamp_107 using mt_timestamp tags(2147483647) values(now, 2147483647)"
        )
        tdSql.query(f"show tags from st_timestamp_107")
        tdSql.checkData(0, 5, 2147483647)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_107")
        tdSql.checkData(0, 1, 2147483647)

        tdSql.execute(
            f"insert into st_timestamp_108 using mt_timestamp tags(37) values(now, 37)"
        )
        tdSql.query(f"show tags from st_timestamp_108")
        tdSql.checkData(0, 5, 37)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_108")
        tdSql.checkData(0, 1, 37)

        tdSql.execute(
            f"insert into st_timestamp_109 using mt_timestamp tags(-100) values(now, -100)"
        )
        tdSql.query(f"show tags from st_timestamp_109")
        tdSql.checkData(0, 5, -100)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_109")
        tdSql.checkData(0, 1, -100)

        tdSql.execute(
            f"insert into st_timestamp_1010 using mt_timestamp tags(+113) values(now, +113)"
        )
        tdSql.query(f"show tags from st_timestamp_1010")
        tdSql.checkData(0, 5, 113)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1010")
        tdSql.checkData(0, 1, 113)

        tdSql.execute(
            f"insert into st_timestamp_1011 using mt_timestamp tags('-100') values(now, '-100')"
        )
        tdSql.query(f"show tags from st_timestamp_1011")
        tdSql.checkData(0, 5, -100)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1011")
        tdSql.checkData(0, 1, -100)

        tdSql.execute(
            f'insert into st_timestamp_1012 using mt_timestamp tags("-0") values(now, "-0")'
        )
        tdSql.query(f"show tags from st_timestamp_1012")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1012")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_timestamp_1013 using mt_timestamp tags(+0078) values(now, +0078)"
        )
        tdSql.query(f"show tags from st_timestamp_1013")
        tdSql.checkData(0, 5, 78)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1013")
        tdSql.checkData(0, 1, 78)

        tdSql.execute(
            f"insert into st_timestamp_1014 using mt_timestamp tags(-00078) values(now, -00078)"
        )
        tdSql.query(f"show tags from st_timestamp_1014")
        tdSql.checkData(0, 5, -78)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1014")
        tdSql.checkData(0, 1, -78)

        tdSql.execute(
            f'insert into st_timestamp_1015 using mt_timestamp tags("0x01") values(now, "0x01")'
        )
        tdSql.query(f"show tags from st_timestamp_1015")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1015")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_timestamp_1016 using mt_timestamp tags("0b01") values(now, "0b01")'
        )
        tdSql.query(f"show tags from st_timestamp_1016")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1016")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_timestamp_1017 using mt_timestamp tags("+0x01") values(now, "+0x01")'
        )
        tdSql.query(f"show tags from st_timestamp_1017")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(
            f'insert into st_timestamp_1018 using mt_timestamp tags("-0b01") values(now, "-0b01")'
        )
        tdSql.query(f"show tags from st_timestamp_1018")
        tdSql.checkData(0, 5, -1)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1018")
        tdSql.checkData(0, 1, -1)

        tdSql.execute(
            f"insert into st_timestamp_1019 using mt_timestamp tags( 0x01) values(now, 0x01)"
        )
        tdSql.query(f"show tags from st_timestamp_1019")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(
            f"insert into st_timestamp_1020 using mt_timestamp tags(0b01 ) values(now, 0b01 )"
        )
        tdSql.query(f"show tags from st_timestamp_1020")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1020")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_timestamp_1021 using mt_timestamp tags(+0x01) values(now, +0x01)"
        )
        tdSql.query(f"show tags from st_timestamp_1021")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1021")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_timestamp_1022 using mt_timestamp tags( -0b01 ) values(now,  -0b01)"
        )
        tdSql.query(f"show tags from st_timestamp_1022")
        tdSql.checkData(0, 5, -1)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1022")
        tdSql.checkData(0, 1, -1)

        tdSql.execute(
            f"insert into st_timestamp_1023 using mt_timestamp tags(+1+1d) values(now,+1+ 1d )"
        )
        tdSql.query(f"show tags from st_timestamp_1023")
        tdSql.checkData(0, 5, 86400001)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1023")
        tdSql.checkData(0, 1, 86400001)

        tdSql.execute(
            f"insert into st_timestamp_1024 using mt_timestamp tags(-0+1d) values(now,-0 + 1d)"
        )
        tdSql.query(f"show tags from st_timestamp_1024")
        tdSql.checkData(0, 5, 86400000)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1024")
        tdSql.checkData(0, 1, 86400000)

        tdSql.execute(
            f'insert into st_timestamp_1025 using mt_timestamp tags("-0" -1s) values(now,"-0" -1s)'
        )
        tdSql.query(f"show tags from st_timestamp_1025")
        tdSql.checkData(0, 5, -1000)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1025")
        tdSql.checkData(0, 1, -1000)

        tdSql.execute(
            f"insert into st_timestamp_1026 using mt_timestamp tags(+0b01-1a) values(now,+0b01 -1a)"
        )
        tdSql.query(f"show tags from st_timestamp_1026")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1026")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_timestamp_1027 using mt_timestamp tags(0b01-1s) values(now,0b01 -1s)"
        )
        tdSql.query(f"show tags from st_timestamp_1027")
        tdSql.checkData(0, 5, -999)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1027")
        tdSql.checkData(0, 1, -999)

        tdSql.execute(
            f'insert into st_timestamp_1028 using mt_timestamp tags("0x01" + 1u) values(now,"0x01" +1u)'
        )
        tdSql.query(f"show tags from st_timestamp_1028")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1028")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_timestamp_1029 using mt_timestamp tags(+0x01 +1b) values(now,+0x01 +1b)"
        )
        tdSql.query(f"show tags from st_timestamp_1029")
        tdSql.checkData(0, 5, 1)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1029")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_timestamp_1030 using mt_timestamp tags(-0b00 -0a) values(now,-0b00 -0a)"
        )
        tdSql.query(f"show tags from st_timestamp_1030")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1030")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f'insert into st_timestamp_1031 using mt_timestamp tags("-0x00" +1u) values(now,"-0x00" +1u)'
        )
        tdSql.query(f"show tags from st_timestamp_1031")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1031")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_timestamp_1032 using mt_timestamp tags(-0x00 +1b) values(now,-0x00 +1b)"
        )
        tdSql.query(f"show tags from st_timestamp_1032")
        tdSql.checkData(0, 5, 0)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1032")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_timestamp_1033 using mt_timestamp tags(now+1b) values(now,now +1b)"
        )
        tdSql.query(f"show tags from st_timestamp_1033")
        tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1033")
        tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

        tdSql.execute(
            f'insert into st_timestamp_1034 using mt_timestamp tags("now" +1b) values(now,"now()" +1b)'
        )
        tdSql.query(f"show tags from st_timestamp_1034")
        tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1034")
        tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

        tdSql.execute(
            f"insert into st_timestamp_1035 using mt_timestamp tags(today() + 1d) values(now,today() +1d)"
        )
        tdSql.query(f"show tags from st_timestamp_1035")
        tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1035")
        tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

        tdSql.execute(
            f'insert into st_timestamp_1036 using mt_timestamp tags("today" +1d) values(now,"today()" +1d)'
        )
        tdSql.query(f"show tags from st_timestamp_1036")
        tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

        tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1036")
        tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

    def alter_tag_value(self):
        tdLog.info(f"case 3: alter tag value")
        tdSql.execute(f"alter table st_timestamp_0 set tag tagname=NULL")
        tdSql.query(f"show tags from st_timestamp_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_timestamp_1 set tag tagname=NULL")
        tdSql.query(f"show tags from st_timestamp_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_timestamp_2 set tag tagname='NULL'")
        tdSql.query(f"show tags from st_timestamp_2")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_timestamp_3 set tag tagname='NULL'")
        tdSql.query(f"show tags from st_timestamp_3")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'alter table st_timestamp_4 set tag tagname="NULL"')
        tdSql.query(f"show tags from st_timestamp_4")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'alter table st_timestamp_5 set tag tagname="NULL"')
        tdSql.query(f"show tags from st_timestamp_5")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_timestamp_6 set tag tagname=-2147483647")
        tdSql.query(f"show tags from st_timestamp_6")
        tdSql.checkData(0, 5, -2147483647)

        tdSql.execute(f"alter table st_timestamp_7 set tag tagname=2147483647")
        tdSql.query(f"show tags from st_timestamp_7")
        tdSql.checkData(0, 5, 2147483647)

        tdSql.execute(f"alter table st_timestamp_8 set tag tagname=37")
        tdSql.query(f"show tags from st_timestamp_8")
        tdSql.checkData(0, 5, 37)

        tdSql.execute(f"alter table st_timestamp_9 set tag tagname=-100")
        tdSql.query(f"show tags from st_timestamp_9")
        tdSql.checkData(0, 5, -100)

        tdSql.execute(f"alter table st_timestamp_10 set tag tagname=+113")
        tdSql.query(f"show tags from st_timestamp_10")
        tdSql.checkData(0, 5, 113)

        tdSql.execute(f"alter table st_timestamp_11 set tag tagname='-100'")
        tdSql.query(f"show tags from st_timestamp_11")
        tdSql.checkData(0, 5, -100)

        tdSql.execute(f'alter table st_timestamp_12 set tag tagname="-0"')
        tdSql.query(f"show tags from st_timestamp_12")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"alter table st_timestamp_13 set tag tagname=+0078")
        tdSql.query(f"show tags from st_timestamp_13")
        tdSql.checkData(0, 5, 78)

        tdSql.execute(f"alter table st_timestamp_14 set tag tagname=-00078")
        tdSql.query(f"show tags from st_timestamp_14")
        tdSql.checkData(0, 5, -78)

        tdSql.execute(f'alter table st_timestamp_15 set tag tagname="0x01"')
        tdSql.query(f"show tags from st_timestamp_15")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'alter table st_timestamp_16 set tag tagname="0b01"')
        tdSql.query(f"show tags from st_timestamp_16")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'alter table st_timestamp_17 set tag tagname="+0x01"')
        tdSql.query(f"show tags from st_timestamp_17")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f'alter table st_timestamp_18 set tag tagname="-0b01"')
        tdSql.query(f"show tags from st_timestamp_18")
        tdSql.checkData(0, 5, -1)

        tdSql.execute(f"alter table st_timestamp_19 set tag tagname= 0x01")
        tdSql.query(f"show tags from st_timestamp_19")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"alter table st_timestamp_20 set tag tagname=0b01 ")
        tdSql.query(f"show tags from st_timestamp_20")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"alter table st_timestamp_21 set tag tagname=+0x01")
        tdSql.query(f"show tags from st_timestamp_21")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"alter table st_timestamp_22 set tag tagname= -0b01 ")
        tdSql.query(f"show tags from st_timestamp_22")
        tdSql.checkData(0, 5, -1)

        tdSql.execute(f"alter table st_timestamp_23 set tag tagname=1+ 1d ")
        tdSql.query(f"show tags from st_timestamp_23")
        tdSql.checkData(0, 5, 86400001)

        tdSql.execute(f"alter table st_timestamp_24 set tag tagname=-0 + 1d")
        tdSql.query(f"show tags from st_timestamp_24")
        tdSql.checkData(0, 5, 86400000)

        tdSql.execute(f'alter table st_timestamp_25 set tag tagname="-0" -1s')
        tdSql.query(f"show tags from st_timestamp_25")
        tdSql.checkData(0, 5, -1000)

        tdSql.execute(f"alter table st_timestamp_26 set tag tagname=+0b01 -1a")
        tdSql.query(f"show tags from st_timestamp_26")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"alter table st_timestamp_27 set tag tagname=0b01 -1s")
        tdSql.query(f"show tags from st_timestamp_27")
        tdSql.checkData(0, 5, -999)

        tdSql.execute(f'alter table st_timestamp_28 set tag tagname="0x01" +1u')
        tdSql.query(f"show tags from st_timestamp_28")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"alter table st_timestamp_29 set tag tagname=0x01 +1b")
        tdSql.query(f"show tags from st_timestamp_29")
        tdSql.checkData(0, 5, 1)

        tdSql.execute(f"alter table st_timestamp_30 set tag tagname==-0b00 -0a")
        tdSql.query(f"show tags from st_timestamp_30")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f'alter table st_timestamp_31 set tag tagname="-0x00" +1u')
        tdSql.query(f"show tags from st_timestamp_31")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"alter table st_timestamp_32 set tag tagname=-0x00 +1b")
        tdSql.query(f"show tags from st_timestamp_32")
        tdSql.checkData(0, 5, 0)

        tdSql.execute(f"alter table st_timestamp_33 set tag tagname=now +1b")
        tdSql.query(f"show tags from st_timestamp_33")
        tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

        tdSql.execute(f'alter table st_timestamp_34 set tag tagname="now()" +1b')
        tdSql.query(f"show tags from st_timestamp_34")
        tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

        tdSql.execute(f"alter table st_timestamp_35 set tag tagname=today( ) +1d")
        tdSql.query(f"show tags from st_timestamp_35")
        tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

        tdSql.execute(f'alter table st_timestamp_36 set tag tagname="today()" +1d')
        tdSql.query(f"show tags from st_timestamp_36")
        tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

    def illegal_input(self):
        tdLog.info(f"case 4: illegal input")

        tdSql.error(f"create table st_timestamp_e0 using mt_timestamp tags(123abc)")
        tdSql.error(f'create table st_timestamp_e0 using mt_timestamp tags("123abc")')
        tdSql.error(f"create table st_timestamp_e0 using mt_timestamp tags(abc)")
        tdSql.error(f'create table st_timestamp_e0 using mt_timestamp tags("abc")')
        tdSql.error(
            f"create table st_timestamp_e0 using mt_timestamp tags(now()+1d+1s)"
        )
        tdSql.error(f"create table st_timestamp_e0 using mt_timestamp tags(1+1y)")
        tdSql.error(f"create table st_timestamp_e0 using mt_timestamp tags(0x01+1b+1a)")
        tdSql.error(f'create table st_timestamp_e0 using mt_timestamp tags(" ")')
        tdSql.error(f"create table st_timestamp_e0 using mt_timestamp tags('')")
        tdSql.error(f'create table st_timestamp_104 using mt_timestamp tags("-123.1")')
        tdSql.error(f'create table st_timestamp_105 using mt_timestamp tags("+123.5")')
        tdSql.error(f'create table st_timestamp_106 using mt_timestamp tags("-1e-1")')
        tdSql.error(
            f'create table st_timestamp_107 using mt_timestamp tags("+0.1235e3")'
        )
        tdSql.error(
            f'create table st_timestamp_108 using mt_timestamp tags("-0.11e-30")'
        )
        tdSql.error(
            f'create table st_timestamp_109 using mt_timestamp tags("-1.1e-307")'
        )
        tdSql.error(f"create table st_timestamp_110 using mt_timestamp tags( -1e-1 )")
        tdSql.error(
            f"create table st_timestamp_111 using mt_timestamp tags( +0.1235e3 )"
        )
        tdSql.error(f"create table st_timestamp_112 using mt_timestamp tags(-0.11e-30)")
        tdSql.error(f"create table st_timestamp_113 using mt_timestamp tags(-1.1e-307)")
        tdSql.execute(
            f"create table st_timestamp_114 using mt_timestamp tags(9223372036854775807)"
        )
        tdSql.error(
            f"create table st_timestamp_115 using mt_timestamp tags(9223372036854775808)"
        )
        tdSql.execute(
            f"create table st_timestamp_116 using mt_timestamp tags(-9223372036854775808)"
        )
        tdSql.error(
            f"create table st_timestamp_117 using mt_timestamp tags(-9223372036854775809)"
        )
        tdSql.error(
            f"insert into st_timestamp_118 using mt_timestamp tags(9223372036854775807) values(9223372036854775807, 9223372036854775807)"
        )
        tdSql.error(
            f"insert into st_timestamp_119 using mt_timestamp tags(1+1s-1s) values(now, now)"
        )
        tdSql.error(
            f"insert into st_timestamp_120 using mt_timestamp tags(1-1s) values(now, now-1s+1d)"
        )
