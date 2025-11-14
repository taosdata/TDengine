from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNullTag:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_null_tag(self):
        """NULL: tag

        1. Create table with NULL tags
        2. Select tags
        3. Alter tags with NULL
        4. Insert data with NULL tags
        5. Query data with NULL tags
        

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/null_char.sim

        """

        tdLog.info(f"========== NULL_char.sim")

        db = "db"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")

        #### case 0: field NULL, or 'NULL'
        tdSql.execute(
            f"create table mt1 (ts timestamp, col1 int, col2 bigint, col3 float, col4 double, col5 binary(8), col6 bool, col7 smallint, col8 tinyint, col9 nchar(8)) tags (tag1 binary(8), tag2 nchar(8), tag3 int, tag4 bigint, tag5 bool, tag6 float)"
        )
        tdSql.execute(
            f"create table st1 using mt1 tags (NULL, 'NULL', 100, 1000, 'false', 9.123)"
        )
        tdSql.query(f"show create table st1")
        tdSql.execute(
            f"insert into st1 values ('2019-01-01 09:00:00.000', 123, -123, 3.0, 4.0, 'binary', true, 1000, 121, 'nchar')"
        )
        tdSql.execute(
            f"insert into st1 values ('2019-01-01 09:00:01.000', '456', '456', '3.33', '4.444', 'binary', 'true', '1001', '122', 'nchar')"
        )
        tdSql.execute(
            f"insert into st1 values ('2019-01-01 09:00:02.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        tdSql.execute(
            f"insert into st1 values ('2019-01-01 09:00:03.000', NULL, NULL, NULL, NULL, 'NULL', 'NULL', 2002, 127, 'NULL')"
        )

        tdSql.query(f"select * from mt1")
        tdSql.checkRows(4)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(2, 3, None)
        tdSql.checkData(2, 4, None)
        tdSql.checkData(2, 5, None)
        tdSql.checkData(2, 6, None)
        tdSql.checkData(2, 7, None)
        tdSql.checkData(2, 8, None)
        tdSql.checkData(2, 9, None)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, None)

        # if $tdSql.getData(3,3) != 0.00000 then
        #  print === expect 0.00000, actually $tdSql.getData(3,3)
        #  return -1
        # endi
        # if $tdSql.getData(3,4) != 0.000000000 then
        #  print === expect 0.00000, actually $tdSql.getData(3,4)
        #  return -1
        # endi
        tdSql.checkData(3, 5, "NULL")
        tdSql.checkData(3, 6, None)
        tdSql.checkData(3, 9, "NULL")

        #### case 1: tag NULL, or 'NULL'
        tdSql.execute(
            f"create table mt2 (ts timestamp, col1 int, col3 float, col5 binary(8), col6 bool, col9 nchar(8)) tags (tag1 binary(8), tag2 nchar(8), tag3 int, tag5 bool)"
        )
        tdSql.execute(f"create table st2 using mt2 tags (NULL, 'NULL', 102, 'true')")
        tdSql.execute(f"insert into st2 (ts, col1) values(now, 1)")

        tdSql.query(f"select tag1, tag2, tag3, tag5 from st2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, "NULL")
        tdSql.checkData(0, 2, 102)
        tdSql.checkData(0, 3, 1)

        tdSql.query(f"select tag1 from st2 limit 20 offset 1")
        tdSql.checkRows(0)

        tdSql.query(f"select tag1 from st2 limit 10 offset 2")
        tdSql.checkRows(0)

        tdSql.query(f"select tag1 from st2 limit 0 offset 0")
        tdSql.checkRows(0)

        tdSql.execute(f"create table st3 using mt2 tags (NULL, 'ABC', 103, 'FALSE')")
        tdSql.query(f"show create table st3")
        tdSql.execute(f"insert into st3 (ts, col1) values(now, 1)")
        tdSql.query(f"select tag1, tag2, tag3, tag5 from st3")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, "ABC")
        tdSql.checkData(0, 2, 103)
        tdSql.checkData(0, 3, 0)

        ### bool:
        tdSql.execute(f"create table stx using mt2 tags ('NULL', '123aBc', 104, '123')")
        tdSql.error(f"create table sty using mt2 tags ('NULL', '123aBc', 104, 'xtz')")
        tdSql.execute(
            f"create table st4 using mt2 tags ('NULL', '123aBc', 104, 'NULL')"
        )
        tdSql.execute(f"insert into st4 (ts, col1) values(now, 1)")
        tdSql.query(f"select tag1,tag2,tag3,tag5 from st4")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "NULL")
        tdSql.checkData(0, 1, "123aBc")
        tdSql.checkData(0, 2, 104)
        tdSql.checkData(0, 3, None)

        tdSql.execute(f"create table st5 using mt2 tags ('NULL', '123aBc', 105, NULL)")
        tdSql.execute(f"insert into st5 (ts, col1) values(now, 1)")
        tdSql.query(f"select tag1,tag2,tag3,tag5 from st5")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "NULL")
        tdSql.checkData(0, 1, "123aBc")
        tdSql.checkData(0, 2, 105)
        tdSql.checkData(0, 3, None)

        #### case 2: dynamic create table using super table when insert into
        tdSql.execute(
            f"create table mt3 (ts timestamp, col1 int, col3 float, col5 binary(8), col6 bool, col9 nchar(8)) tags (tag1 binary(8), tag2 nchar(8), tag3 int, tag5 bool)"
        )
        tdSql.execute(
            f"insert into st31 using mt3 tags (NULL, 'NULL', 102, 'true')     values (now+1s, 31, 31, 'bin_31', '123', 'nchar_31')"
        )
        tdSql.error(
            f"insert into st32 using mt3 tags (NULL, 'ABC', 103, 'FALSE')     values (now+2s, 32, 32.12345, 'bin_32', 'abc', 'nchar_32')"
        )
        tdSql.error(
            f"insert into st33 using mt3 tags ('NULL', '123aBc', 104, 'NULL') values (now+3s, 33, 33, 'bin_33', 'false123', 'nchar_33')"
        )
        tdSql.error(
            f"insert into st34 using mt3 tags ('NULL', '123aBc', 105, NULL)   values (now+4s, 34, 34.12345, 'bin_34', 'true123', 'nchar_34')"
        )

        #### case 3: set tag value
        tdSql.execute(
            f"create table mt4 (ts timestamp, c1 int) tags (tag_binary binary(16), tag_nchar nchar(16), tag_int int, tag_bool bool, tag_float float, tag_double double)"
        )
        tdSql.execute(
            f"create table st41 using mt4 tags (\"beijing\", 'nchar_tag', 100, false, 9.12345, 7.123456789)"
        )
        tdSql.execute(f"insert into st41 (ts, c1) values(now, 1)")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from st41"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "beijing")
        tdSql.checkData(0, 1, "nchar_tag")
        tdSql.checkData(0, 2, 100)
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, 9.12345)
        tdSql.checkData(0, 5, 7.123456789)

        ################# binary
        tdSql.execute(f'alter table st41 set tag tag_binary = "shanghai"')
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 0, "shanghai")

        ##### test 'space' case
        tdSql.execute(f'alter table st41 set tag tag_binary = ""')
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 0, "")

        tdSql.execute(f'alter table st41 set tag tag_binary = "NULL"')
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 0, "NULL")

        tdSql.execute(f"alter table st41 set tag tag_binary = NULL")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 0, None)

        ################### nchar
        tdSql.execute(f'alter table st41 set tag tag_nchar = "��˼����"')
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )

        # if $tdSql.getData(0,1) != ��˼���� then
        #  print ==== expect ��˼����, actually $tdSql.getData(0,1)
        #  return -1
        # endi
        ##### test 'space' case
        # $tagvalue = '
        # $tagvalue = $tagvalue '
        tdSql.execute(f"alter table st41 set tag tag_nchar = ''")
        # sql select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41
        # if $tdSql.getData(0,1) != $tagvalue then
        #  return -1
        # endi
        tdSql.execute(f'alter table st41 set tag tag_nchar = "NULL"')
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 1, "NULL")

        tdSql.execute(f"alter table st41 set tag tag_nchar = NULL")
        # sql select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41
        # if $tdSql.getData(0,1) !=   then
        #  print ==9== expect  , actually $tdSql.getData(0,1)
        #   return -1
        # endi

        ################### int
        tdSql.execute(f"alter table st41 set tag tag_int = -2147483647")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 2, -2147483647)

        tdSql.execute(f"alter table st41 set tag tag_int = 2147483647")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 2, 2147483647)

        tdSql.execute(f"alter table st41 set tag tag_int = -2147483648")
        tdSql.error(f"alter table st41 set tag tag_int = 2147483648")

        tdSql.execute(f"alter table st41 set tag tag_int = '-379'")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 2, -379)

        tdSql.execute(f"alter table st41 set tag tag_int = -2000")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 2, -2000)

        tdSql.execute(f"alter table st41 set tag tag_int = NULL")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 2, None)

        tdSql.execute(f"alter table st41 set tag tag_int = 'NULL'")
        tdSql.error(f"alter table st41 set tag tag_int = ''")
        tdSql.error(f"alter table st41 set tag tag_int = abc379")

        ################### bool
        tdSql.execute(f"alter table st41 set tag tag_bool = 'true'")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 3, 1)

        tdSql.execute(f"alter table st41 set tag tag_bool = 'false'")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 3, 0)

        tdSql.execute(f"alter table st41 set tag tag_bool = 0")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 3, 0)

        tdSql.execute(f"alter table st41 set tag tag_bool = 123")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 3, 1)

        tdSql.execute(f"alter table st41 set tag tag_bool = 'NULL'")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 3, None)

        tdSql.execute(f"alter table st41 set tag tag_bool = NULL")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 3, None)

        tdSql.execute(f"alter table st41 set tag tag_bool = '123'")
        tdSql.error(f"alter table st41 set tag tag_bool = ''")
        tdSql.error(f"alter table st41 set tag tag_bool = abc379")

        ################### float
        tdSql.execute(f"alter table st41 set tag tag_float = -32")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 4, -32.00000)

        tdSql.execute(f"alter table st41 set tag tag_float = 54.123456")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 4, 54.123455)

        tdSql.execute(f"alter table st41 set tag tag_float = 54.12345")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 4, 54.12345)

        tdSql.execute(f"alter table st41 set tag tag_float = 54.12345678")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 4, 54.12346)

        tdSql.execute(f"alter table st41 set tag tag_float = NULL")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 4, None)

        tdSql.execute(f"alter table st41 set tag tag_float = 'NULL'")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 4, None)

        tdSql.execute(f"alter table st41 set tag tag_float = '54.123456'")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 4, 54.12346)

        tdSql.execute(f"alter table st41 set tag tag_float = '-54.123456'")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 4, -54.12346)

        tdSql.error(f"alter table st41 set tag tag_float = ''")

        tdSql.error(f"alter table st41 set tag tag_float = 'abc'")
        tdSql.error(f"alter table st41 set tag tag_float = '123abc'")
        tdSql.error(f"alter table st41 set tag tag_float = abc")

        ################### double
        tdSql.execute(f"alter table st41 set tag tag_double = -92")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 5, -92.000000000)

        tdSql.execute(f"alter table st41 set tag tag_double = 184")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 5, 184.000000000)

        tdSql.execute(f"alter table st41 set tag tag_double = '-2456'")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 5, -2456.000000000)

        tdSql.execute(f"alter table st41 set tag tag_double = NULL")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st41 set tag tag_double = 'NULL'")
        tdSql.query(
            f"select tag_binary, tag_nchar, tag_int, tag_bool, tag_float, tag_double from  st41"
        )
        tdSql.checkData(0, 5, None)

        tdSql.error(f"alter table st41 set tag tag_double = ''")
        tdSql.execute(f"alter table st41 set tag tag_double = '123'")
        tdSql.error(f"alter table st41 set tag tag_double = 'abc'")
        tdSql.error(f"alter table st41 set tag tag_double = '123abc'")
        tdSql.error(f"alter table st41 set tag tag_double = abc")

        ################### bigint smallint tinyint
        tdSql.execute(
            f"create table mt5 (ts timestamp, c1 int) tags (tag_bigint bigint, tag_smallint smallint, tag_tinyint tinyint)"
        )
        tdSql.execute(f"create table st51 using mt5 tags (1, 2, 3)")
        tdSql.execute(f"insert into st51 values(now, 1)")
        tdSql.execute(f"alter table st51 set tag tag_bigint = '-379'")
        tdSql.execute(f"alter table st51 set tag tag_bigint = -2000")
        tdSql.execute(f"alter table st51 set tag tag_bigint = NULL")
        tdSql.execute(f"alter table st51 set tag tag_bigint = 9223372036854775807")
        tdSql.query(f"select tag_bigint, tag_smallint, tag_tinyint from st51")
        tdSql.checkData(0, 0, 9223372036854775807)

        tdSql.error(f"alter table st51 set tag tag_bigint = 9223372036854775808")

        tdSql.execute(f"alter table st51 set tag tag_bigint = -9223372036854775807")
        tdSql.query(f"select tag_bigint, tag_smallint, tag_tinyint from st51")
        tdSql.checkData(0, 0, -9223372036854775807)

        tdSql.execute(f"alter table st51 set tag tag_bigint = -9223372036854775808")

        tdSql.execute(f"alter table st51 set tag tag_bigint = 'NULL'")
        tdSql.error(f"alter table st51 set tag tag_bigint = ''")
        tdSql.error(f"alter table st51 set tag tag_bigint = abc379")

        ####
        tdSql.execute(f"alter table st51 set tag tag_smallint = -2000")
        tdSql.execute(f"alter table st51 set tag tag_smallint = NULL")
        tdSql.execute(f"alter table st51 set tag tag_smallint = 32767")
        tdSql.query(f"select tag_bigint, tag_smallint, tag_tinyint from st51")
        tdSql.checkData(0, 1, 32767)

        tdSql.error(f"alter table st51 set tag tag_smallint = 32768")

        tdSql.execute(f"alter table st51 set tag tag_smallint = -32767")
        tdSql.query(f"select tag_bigint, tag_smallint, tag_tinyint from st51")
        tdSql.checkData(0, 1, -32767)

        tdSql.execute(f"alter table st51 set tag tag_smallint = -32768")

        tdSql.execute(f"alter table st51 set tag tag_smallint = 'NULL'")
        tdSql.error(f"alter table st51 set tag tag_smallint = ''")
        tdSql.error(f"alter table st51 set tag tag_smallint = abc379")

        ####
        tdSql.execute(f"alter table st51 set tag tag_tinyint = -127")
        tdSql.execute(f"alter table st51 set tag tag_tinyint = NULL")
        tdSql.execute(f"alter table st51 set tag tag_tinyint = 127")
        tdSql.query(f"select tag_bigint, tag_smallint, tag_tinyint from st51")
        tdSql.checkData(0, 2, 127)

        tdSql.execute(f"alter table st51 set tag tag_tinyint = -127")
        tdSql.query(f"select tag_bigint, tag_smallint, tag_tinyint from st51")
        tdSql.checkData(0, 2, -127)

        tdSql.execute(f"alter table st51 set tag tag_tinyint = '-128'")
        tdSql.error(f"alter table st51 set tag tag_tinyint = 128")
        tdSql.execute(f"alter table st51 set tag tag_tinyint = 'NULL'")
        tdSql.error(f"alter table st51 set tag tag_tinyint = ''")
        tdSql.error(f"alter table st51 set tag tag_tinyint = abc379")
