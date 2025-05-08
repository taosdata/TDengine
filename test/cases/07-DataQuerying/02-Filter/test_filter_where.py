from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFilterWhere:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_filter_where(self):
        """filter where

        1. -

        Catalog:
            - Query:Filter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/where.sim

        """

        dbPrefix = "wh_db"
        tbPrefix = "wh_tb"
        mtPrefix = "wh_mt"
        tbNum = 10
        rowNum = 1000
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== where.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"create database if not exists {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int)"
        )

        half = tbNum / 2

        i = 0
        while i < half:
            tb = tbPrefix + str(i)
            nextSuffix = i + int(half)
            tb1 = tbPrefix + str(nextSuffix)

            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            tdSql.execute(f"create table {tb1} using {mt} tags( {nextSuffix} )")

            x = 0
            while x < rowNum:
                y = x * 60000
                ms = 1600099200000 + y
                c = x % 100
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ({ms} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )  {tb1} values ({ms} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                x = x + 1

            i = i + 1

        i = 1
        tb = tbPrefix + str(i)

        ##
        tdSql.query(f"select * from {tb} where c7")

        # TBASE-654 : invalid filter expression cause server crashed
        tdSql.query(f"select count(*) from {tb} where c1<10 and c1<>2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 90)

        tdSql.query(f"select * from {tb} where c7 = false")
        val = rowNum / 100
        tdSql.checkRows(val)

        tdSql.query(f"select * from {mt} where c7 = false")
        val = totalNum / 100
        tdSql.checkRows(val)

        tdSql.query(f"select distinct tbname from {mt}")
        tdSql.checkRows(tbNum)

        tdSql.query(f"select distinct tbname from {mt} where t1 < 2")
        tdSql.checkRows(2)

        # print $tbPrefix
        # $tb = $tbPrefix . 0
        # if $tdSql.getData(0,0,wh_tb1 then
        #  print expect wh_tb1, actual:$tdSql.getData(0,0)
        #  return -1
        # endi
        # $tb = $tbPrefix . 1
        # if $tdSql.getData(1,0,wh_tb0 then
        #  print expect wh_tb0, actual:$tdSql.getData(0,0)
        #  return -1
        # endi

        ## select specified columns

        tdLog.info(f"select c1 from {mt}")
        tdSql.query(f"select c1 from {mt}")

        tdLog.info(f"rows {tdSql.getRows()})")
        tdLog.info(f"totalNum {totalNum}")
        tdSql.checkRows(totalNum)

        tdSql.query(f"select count(c1) from {mt}")
        tdSql.checkData(0, 0, totalNum)

        tdSql.query(f"select c1 from {mt} where c1 >= 0")
        tdSql.checkRows(totalNum)

        tdSql.query(f"select count(c1) from {mt} where c1 <> -1")
        tdSql.checkData(0, 0, totalNum)

        tdSql.query(f"select count(c1) from {mt} where c1 <> -1 group by t1")
        tdSql.checkRows(tbNum)

        tdSql.checkData(0, 0, rowNum)

        ## like
        tdSql.error(f"select * from {mt} where c1 like 1")
        # sql_error select * from $mt where t1 like 1

        ## [TBASE-593]
        tdSql.execute(
            f"create table wh_mt1 (ts timestamp, c1 smallint, c2 int, c3 bigint, c4 float, c5 double, c6 tinyint, c7 binary(10), c8 nchar(10), c9 bool, c10 timestamp) tags (t1 binary(10), t2 smallint, t3 int, t4 bigint, t5 float, t6 double)"
        )
        tdSql.execute(
            f"create table wh_mt1_tb1 using wh_mt1 tags ('tb11', 1, 1, 1, 1, 1)"
        )
        tdSql.execute(
            f"insert into wh_mt1_tb1 values (now, 1, 1, 1, 1, 1, 1, 'binary', 'nchar', true, '2019-01-01 00:00:00.000')"
        )
        # sql_error select last(*) from wh_mt1 where c1 in ('1')
        # sql_error select last(*) from wh_mt1_tb1 where c1 in ('1')
        # sql_error select last(*) from wh_mt1 where c2 in ('1')
        # sql_error select last(*) from wh_mt1_tb1 where c2 in ('1')
        # sql_error select last(*) from wh_mt1 where c3 in ('1')
        # sql_error select last(*) from wh_mt1_tb1 where c3 in ('1')
        # sql_error select last(*) from wh_mt1 where c4 in ('1')
        # sql_error select last(*) from wh_mt1_tb1 where c4 in ('1')
        # sql_error select last(*) from wh_mt1 where c5 in ('1')
        # sql_error select last(*) from wh_mt1_tb1 where c5 in ('1')
        # sql_error select last(*) from wh_mt1 where c6 in ('1')
        # sql_error select last(*) from wh_mt1_tb1 where c6 in ('1')
        # sql_error select last(*) from wh_mt1 where c7 in ('binary')
        # sql_error select last(*) from wh_mt1_tb1 where c7 in ('binary')
        # sql_error select last(*) from wh_mt1 where c8 in ('nchar')
        # sql_error select last(*) from wh_mt1_tb1 where c9 in (true, false)
        # sql_error select last(*) from wh_mt1 where c10 in ('2019-01-01 00:00:00.000')
        # sql_error select last(*) from wh_mt1_tb1 where c10 in ('2019-01-01 00:00:00.000')
        tdSql.query(f"select last(*) from wh_mt1 where c1 = 1")
        tdSql.checkRows(1)

        ## [TBASE-597]
        tdSql.execute(
            f"create table wh_mt2 (ts timestamp, c1 timestamp, c2 binary(10), c3 nchar(10)) tags (t1 binary(10))"
        )
        tdSql.execute(f"create table wh_mt2_tb1 using wh_mt2 tags ('wh_mt2_tb1')")

        # 2019-01-01 00:00:00.000     1546272000000
        # 2019-01-01 00:10:00.000     1546272600000
        # 2019-01-01 09:00:00.000     1546304400000
        # 2019-01-01 09:10:00.000     1546305000000
        tdSql.execute(
            f"insert into wh_mt2_tb1 values ('2019-01-01 00:00:00.000', '2019-01-01 09:00:00.000', 'binary10', 'nchar10')"
        )
        tdSql.execute(
            f"insert into wh_mt2_tb1 values ('2019-01-01 00:10:00.000', '2019-01-01 09:10:00.000', 'binary10', 'nchar10')"
        )

        tdSql.query(f"select * from wh_mt2_tb1 where c1 > 1546304400000")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2019-01-01 00:10:00.000")

        tdSql.checkData(0, 1, "2019-01-01 09:10:00.000")

        tdSql.query(f"select * from wh_mt2_tb1 where c1 > '2019-01-01 09:00:00.000'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2019-01-01 00:10:00.000")

        tdSql.checkData(0, 1, "2019-01-01 09:10:00.000")

        tdSql.query(
            f"select * from wh_mt2_tb1 where c1 > 1546304400000 and ts < '2019-01-01 00:10:00.000'"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from wh_mt2_tb1 where c1 > '2019-01-01 09:00:00.000' and ts < '2019-01-01 00:10:00.000'"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from wh_mt2_tb1 where c1 >= 1546304400000 and c1 <= '2019-01-01 09:10:00.000'"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2019-01-01 00:00:00.000")

        tdSql.checkData(0, 1, "2019-01-01 09:00:00.000")

        tdSql.checkData(1, 0, "2019-01-01 00:10:00.000")

        tdSql.checkData(1, 1, "2019-01-01 09:10:00.000")

        tdSql.query(
            f"select * from wh_mt2_tb1 where c1 >= '2019-01-01 09:00:00.000' and c1 <= '2019-01-01 09:10:00.000'"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2019-01-01 00:00:00.000")

        tdSql.checkData(0, 1, "2019-01-01 09:00:00.000")

        tdSql.checkData(1, 0, "2019-01-01 00:10:00.000")

        tdSql.checkData(1, 1, "2019-01-01 09:10:00.000")

        tdSql.execute(
            f"create table tb_where_NULL (ts timestamp, c1 float, c2 binary(10))"
        )

        tdLog.info(f"===================>td-1604")
        tdSql.error(f"insert into tb_where_NULL values(?, ?, ?)")
        tdSql.error(f"insert into tb_where_NULL values(now, 1, ?)")
        tdSql.error(f"insert into tb_where_NULL values(?, 1, '')")
        tdSql.error(f"insert into tb_where_NULL values(now, ?, '12')")

        tdSql.execute(
            f"insert into tb_where_NULL values ('2019-01-01 09:00:00.000', 1, 'val1')"
        )
        tdSql.execute(
            f"insert into tb_where_NULL values ('2019-01-01 09:00:01.000', NULL, NULL)"
        )
        tdSql.execute(
            f"insert into tb_where_NULL values ('2019-01-01 09:00:02.000', 2, 'val2')"
        )
        tdSql.query(f"select * from tb_where_NULL where c1 = NULL")
        tdSql.query(f"select * from tb_where_NULL where c1 <> NULL")
        tdSql.query(f"select * from tb_where_NULL where c1 < NULL")
        tdSql.query(f'select * from tb_where_NULL where c1 = "NULL"')
        tdSql.query(f'select * from tb_where_NULL where c1 <> "NULL"')
        tdSql.query(f'select * from tb_where_NULL where c1 <> "nulL"')
        tdSql.query(f'select * from tb_where_NULL where c1 > "NULL"')
        tdSql.query(f'select * from tb_where_NULL where c1 >= "NULL"')
        tdSql.query(f'select * from tb_where_NULL where c2 = "NULL"')
        tdSql.checkRows(0)

        tdSql.query(f'select * from tb_where_NULL where c2 <> "NULL"')
        tdSql.checkRows(2)

        tdSql.query(f'select * from tb_where_NULL where c2 <> "nUll"')
        tdSql.checkRows(2)

        tdLog.info(f"==========tbase-1363")
        # sql create table $mt (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int)

        i = 0
        while i < 1:
            tb = "test_null_filter"
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            while x < 10000:
                y = x * 60000
                ms = 1601481600000 + y
                c = x % 100
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ({ms} , null , null , null , null , null , null , null , null , null )"
                )
                x = x + 1

            i = i + 1

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(
            f"select * from wh_mt0 where c3 = 'abc' and tbname in ('test_null_filter');"
        )

        tdSql.query(
            f"select * from wh_mt0 where c3 = '1' and tbname in ('test_null_filter');"
        )
        tdSql.checkRows(0)

        tdSql.query(f"select * from wh_mt0 where c3 = 1;")
        tdLog.info(f"{tdSql.getRows()}) -> 100")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkRows(100)

        tdSql.query(
            f"select * from wh_mt0 where c3 is null and tbname in ('test_null_filter');"
        )
        tdSql.checkRows(10000)

        tdSql.query(
            f"select * from wh_mt0 where c3 is not null and tbname in ('test_null_filter');"
        )
        tdSql.checkRows(0)

        tdLog.info(f"==========================>td-3318")
        tdSql.execute(f"create table tu(ts timestamp,  k int, b binary(12))")
        tdSql.execute(f"insert into tu values(now, 1, 'abc')")
        tdSql.query(f"select stddev(k) from tu where b <>'abc' interval(1s)")
        tdSql.checkRows(0)

        tdLog.info(f"==========================> td-4783,td-4792")
        tdSql.execute(f"create table where_ts(ts timestamp, f int)")
        tdSql.execute(f"insert into where_ts values('2021-06-19 16:22:00', 1);")
        tdSql.execute(f"insert into where_ts values('2021-06-19 16:23:00', 2);")
        tdSql.execute(f"insert into where_ts values('2021-06-19 16:24:00', 3);")
        tdSql.execute(f"insert into where_ts values('2021-06-19 16:25:00', 1);")
        tdSql.query(
            f"select * from (select * from where_ts) where ts<'2021-06-19 16:25:00' and ts>'2021-06-19 16:22:00' order by ts;"
        )
        tdSql.checkRows(2)

        tdLog.info(f"{tdSql.getData(0,0)}, {tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, 2)

        tdSql.execute(f"insert into where_ts values(now, 5);")
        tdSql.query(f"select * from (select * from where_ts) where ts<now;")
        tdSql.checkRows(5)
