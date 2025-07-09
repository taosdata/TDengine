from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncTopBottom:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_top_bottom(self):
        """Top Bottom

        1. -

        Catalog:
            - Function:Selection

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/topbot.sim

        """

        dbPrefix = "tb_db"
        tbPrefix = "tb_tb"
        stbPrefix = "tb_stb"
        tbNum = 10
        rowNum = 1000
        totalNum = tbNum * rowNum
        loops = 200000
        log = 10000
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== topbot.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db} maxrows 4096 keep 36500")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int)"
        )

        i = 0
        ts = ts0
        halfNum = tbNum / 2
        while i < halfNum:
            tbId = i + int(halfNum)
            tb = tbPrefix + str(i)
            tb1 = tbPrefix + str(tbId)
            tdSql.execute(f"create table {tb} using {stb} tags( {i} )")
            tdSql.execute(f"create table {tb1} using {stb} tags( {tbId} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                c = x % 10
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )"
                )
                tdSql.execute(
                    f"insert into {tb1} values ( {ts} , {c} , NULL , {c} , NULL , {c} , {c} , true, {binary} , {nchar} )"
                )
                x = x + 1

            i = i + 1

        tdLog.info(f"====== tables created")

        tdSql.execute(f"use {db}")
        ##### select from table
        tdLog.info(f"====== select top/bot from table and check num of rows returned")
        tdSql.query(f"select top(c1, 100) from tb_stb0")
        tdSql.checkRows(100)

        tdSql.query(f"select bottom(c1, 100) from tb_stb0")
        tdSql.checkRows(100)

        tdSql.query(f"select _wstart, bottom(c3, 5) from tb_tb1 interval(1y);")
        tdSql.checkRows(5)

        tdSql.checkData(0, 1, 0.00000)

        tdSql.checkData(1, 1, 0.00000)

        tdSql.checkData(2, 1, 0.00000)

        tdSql.checkData(3, 1, 0.00000)

        tdSql.query(f"select _wstart, top(c4, 5) from tb_tb1 interval(1y);")
        tdSql.checkRows(5)

        tdSql.checkData(0, 1, 9.000000000)

        tdSql.checkData(1, 1, 9.000000000)

        tdSql.checkData(2, 1, 9.000000000)

        tdSql.checkData(3, 1, 9.000000000)

        tdSql.query(f"select _wstart, top(c3, 5) from tb_tb1 interval(40h)")
        tdSql.checkRows(25)

        tdSql.checkData(0, 1, 9.00000)

        tdSql.query(f"select last(*) from tb_tb9")
        tdSql.checkRows(1)

        tdSql.query(f"select last(c2) from tb_tb9")
        tdSql.checkRows(0)

        tdSql.query(f"select first(c2), last(c2) from tb_tb9")
        tdSql.checkRows(0)

        tdSql.execute(
            f"create table test(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, col7 bool, col8 binary(20), col9 nchar(20)) tags(loc nchar(20));"
        )
        tdSql.execute(f"create table test1 using test tags('beijing');")
        tdSql.execute(
            f"insert into test1 values(1537146000000, 1, 1, 1, 1, 0.100000, 0.100000, 0, 'taosdata1', '涛思数据1');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000001, 2, 2, 2, 2, 1.100000, 1.100000, 1, 'taosdata2', '涛思数据2');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000002, 3, 3, 3, 3, 2.100000, 2.100000, 0, 'taosdata3', '涛思数据3');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000003, 4, 4, 4, 4, 3.100000, 3.100000, 1, 'taosdata4', '涛思数据4');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000004, 5, 5, 5, 5, 4.100000, 4.100000, 0, 'taosdata5', '涛思数据5');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000005, 6, 6, 6, 6, 5.100000, 5.100000, 1, 'taosdata6', '涛思数据6');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000006, 7, 7, 7, 7, 6.100000, 6.100000, 0, 'taosdata7', '涛思数据7');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000007, 8, 8, 8, 8, 7.100000, 7.100000, 1, 'taosdata8', '涛思数据8');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000008, 9, 9, 9, 9, 8.100000, 8.100000, 0, 'taosdata9', '涛思数据9');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000009, 10, 10, 10, 10, 9.100000, 9.100000, 1, 'taosdata10', '涛思数据10');"
        )
        tdSql.query(f"select ts, bottom(col5, 10) from test order by col5;")
        tdSql.checkRows(10)

        tdSql.checkData(0, 1, 0.10000)

        tdSql.checkData(1, 1, 1.10000)

        tdSql.checkData(2, 1, 2.10000)

        tdLog.info(f"=====================td-1302 case")
        tdSql.execute(f"create database t1 keep 36500")
        tdSql.execute(f"use t1;")
        tdSql.execute(f"create table test(ts timestamp, k int);")
        tdSql.execute(f"insert into test values(29999, 1)(70000, 2)(80000, 3)")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")
        tdSql.connect("root")

        tdSql.query(
            f"select count(*) from t1.test where ts > 10000 and ts < 90000 interval(5000a)"
        )
        tdSql.checkRows(3)

        tdLog.info(f"==============>td-1308")
        tdSql.execute(f"create database db keep 36500")
        tdSql.execute(f"use db;")

        tdSql.execute(
            f"create table stb (ts timestamp, c1 int, c2 binary(10)) tags(t1 binary(10));"
        )
        tdSql.execute(f"create table tb1 using stb tags('a1');")

        tdSql.execute(f"insert into tb1 values('2020-09-03 15:30:48.812', 0, 'tb1');")
        tdSql.query(
            f"select count(*) from stb where ts > '2020-09-03 15:30:44' interval(4s);"
        )
        tdSql.checkRows(1)

        tdSql.execute(f"create table tb4 using stb tags('a4');")
        tdSql.query(
            f"select count(*) from stb where ts > '2020-09-03 15:30:44' interval(4s);"
        )
        tdSql.checkRows(1)

        tdLog.info(f"=======================>td-1446")
        tdSql.execute(f"create table t(ts timestamp, k int)")
        ts = 6000
        while ts < 7000:
            tdSql.execute(f"insert into t values ( {ts} , {ts} )")
            ts = ts + 1

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.connect("root")
        tdSql.execute(f"use db;")

        ts = 1000
        while ts < 5096:
            tdSql.execute(f"insert into t values ( {ts} , {ts} )")
            ts = ts + 1

        tdSql.query(f"select * from t where ts < 6500")
        tdSql.checkRows(4596)

        tdSql.query(f"select * from t where ts < 7000")
        tdSql.checkRows(5096)

        tdSql.query(f"select * from t where ts <= 6000")
        tdSql.checkRows(4097)

        tdSql.query(f"select * from t where ts <= 6001")
        tdSql.checkRows(4098)

        tdLog.info(f"======================>td-1454")
        tdSql.query(f"select count(*)/10, count(*)+99 from t")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 509.600000000)

        tdSql.checkData(0, 1, 5195.000000000)

        tdLog.info(f"=======================>td-1596")
        tdSql.execute(f"create table t2(ts timestamp, k int)")
        tdSql.execute(f"insert into t2 values('2020-1-2 1:1:1', 1);")
        tdSql.execute(f"insert into t2 values('2020-2-2 1:1:1', 1);")

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.execute(f"use db")
        tdSql.query(
            f"select _wstart, count(*), first(ts), last(ts) from t2 interval(1d);"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2020-01-02 00:00:00.000")

        tdSql.checkData(1, 0, "2020-02-02 00:00:00.000")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(0, 2, "2020-01-02 01:01:01.000")

        tdSql.checkData(1, 2, "2020-02-02 01:01:01.000")

        tdSql.checkData(0, 3, "2020-01-02 01:01:01.000")

        tdSql.checkData(1, 3, "2020-02-02 01:01:01.000")

        tdLog.info(f"===============================>td-3361")
        tdSql.execute(f"create table ttm1(ts timestamp, k int) tags(a nchar(12));")
        tdSql.execute(f"create table ttm1_t1 using ttm1 tags('abcdef')")
        tdSql.execute(f"insert into ttm1_t1 values(now, 1)")
        tdSql.query(f"select * from ttm1 where a=123456789012")
        tdSql.checkRows(0)

        tdLog.info(f"===============================>td-3621")
        tdSql.execute(f"create table ttm2(ts timestamp, k bool);")
        tdSql.execute(f"insert into ttm2 values('2021-1-1 1:1:1', true)")
        tdSql.execute(f"insert into ttm2 values('2021-1-1 1:1:2', NULL)")
        tdSql.execute(f"insert into ttm2 values('2021-1-1 1:1:3', false)")
        tdSql.query(f"select * from ttm2 where k is not null")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-01-01 01:01:01.000")

        tdSql.query(f"select * from ttm2 where k is null")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-01-01 01:01:02.000")

        tdSql.query(f"select * from ttm2 where k=true")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-01-01 01:01:01.000")

        tdSql.query(f"select * from ttm2 where k=false")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-01-01 01:01:03.000")

        tdSql.query(f"select * from ttm2 where k<>false")
        tdSql.checkRows(1)

        tdSql.query(f"select * from ttm2 where k=null")
        tdSql.query(f"select * from ttm2 where k<>null")
        tdSql.error(f"select * from ttm2 where k like null")
        tdSql.query(f"select * from ttm2 where k<null")
