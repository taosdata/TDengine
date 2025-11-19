from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestUnionAllAsTable:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_unionall_as_table(self):
        """Operator union all

        1. Union of projection queries
        2. Union of queries containing window and aggregate functions
        3. Union of system table queries
        4. Union of queries from databases with different precision levels


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/parser/union.sim
            - 2025-8-20 Simon Guan Migrated from tsim/parser/union_sysinfo.sim
            - 2025-8-20 Simon Guan Migrated from tsim/query/unionall_as_table.sim
            - 2025-8-20 Simon Guan Migrated from tsim/query/union_precision.sim

        """

        self.Union()
        tdStream.dropAllStreamsAndDbs()
        self.UnionSysinfo()
        tdStream.dropAllStreamsAndDbs()
        self.UnionAllAsSystable()
        tdStream.dropAllStreamsAndDbs()
        self.UnionPrecision()
        tdStream.dropAllStreamsAndDbs()

    def Union(self):
        dbPrefix = "union_db"
        tbPrefix = "union_tb"
        tbPrefix1 = "union_tb_"
        mtPrefix = "union_mt"
        tbNum = 10
        rowNum = 1000
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== union.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        j = 1

        mt1 = mtPrefix + str(j)

        tdSql.execute(f"create database if not exists {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int)"
        )

        i = 0
        t = 1578203484000

        half = tbNum / 2

        while i < half:
            tb = tbPrefix + str(i)

            nextSuffix = i + int(half)
            tb1 = tbPrefix + str(nextSuffix)

            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            tdSql.execute(f"create table {tb1} using {mt} tags( {nextSuffix} )")

            x = 0
            while x < rowNum:
                ms = x * 1000
                ms = ms * 60

                c = x % 100
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"

                t1 = t + ms
                tdSql.execute(
                    f"insert into {tb} values ({t1} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )  {tb1} values ({t1} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                x = x + 1

            i = i + 1

        tdSql.execute(
            f"create table {mt1} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int)"
        )

        j = 0
        t = 1578203484000
        rowNum = 100
        tbNum = 5
        i = 0

        while i < tbNum:
            tb1 = tbPrefix1 + str(j)
            tdSql.execute(f"create table {tb1} using {mt1} tags( {i} )")

            x = 0
            while x < rowNum:
                ms = x * 1000
                ms = ms * 60

                c = x % 100
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"

                t1 = t + ms
                tdSql.execute(
                    f"insert into {tb1} values ({t1} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                x = x + 1

            i = i + 1
            j = j + 1

        i = 1
        tb = tbPrefix + str(i)

        ## column type not identical
        tdSql.query(
            f"select count(*) as a from union_mt0 union all select avg(c1) as a from union_mt0"
        )
        tdSql.query(
            f"select count(*) as a from union_mt0 union all select spread(c1) as a from union_mt0;"
        )

        ## union not supported
        tdSql.query(
            "(select count(*) from union_mt0) union (select count(*) from union_mt0);"
        )

        ## column type not identical
        tdSql.error(
            f"select c1 from union_mt0 limit 10 union all select c2 from union_tb1 limit 20;"
        )

        ## union not support recursively union
        tdSql.error(
            f"select c1 from union_tb0 limit 2 union all (select c1 from union_tb1 limit 1 union all select c1 from union_tb3 limit 2);"
        )
        tdSql.error(
            f"(select c1 from union_tb0 limit 1 union all select c1 from union_tb1 limit 1) union all (select c1 from union_tb0 limit 10 union all select c1 from union_tb1 limit 10);"
        )

        # union as subclause
        tdSql.error(
            f"(select c1 from union_tb0 limit 1 union all select c1 from union_tb1 limit 1) limit 1"
        )

        #         tdSql.query("with parenthese
        tdSql.query("(select c1 from union_tb0)")
        tdSql.checkRows(1000)

        tdSql.checkData(0, 0, 0)

        tdSql.checkData(1, 0, 1)

        tdSql.query(
            "(select 'ab' as options from union_tb1 limit 1) union all (select 'dd' as options from union_tb0 limit 1) order by options;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "ab")

        tdSql.checkData(1, 0, "dd")

        tdSql.query(
            "(select 'ab12345' as options from union_tb1 limit 1) union all (select '1234567' as options from union_tb0 limit 1) order by options desc;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "ab12345")

        tdSql.checkData(1, 0, "1234567")

        # mixed order
        tdSql.query(
            "(select ts, c1 from union_tb1 order by ts asc limit 10) union all (select ts, c1 from union_tb0 order by ts desc limit 2) union all (select ts, c1 from union_tb2 order by ts asc limit 10) order by ts"
        )
        tdSql.checkRows(22)

        tdSql.checkData(0, 0, "2020-01-05 13:51:24.000")

        tdSql.checkData(0, 1, 0)

        tdSql.checkData(1, 0, "2020-01-05 13:51:24.000")

        tdSql.checkData(1, 1, 0)

        tdLog.info(f"{tdSql.getData(9,0)} {tdSql.getData(9,1)}")
        tdSql.checkData(9, 0, "2020-01-05 13:55:24.000")

        tdSql.checkData(9, 1, 4)

        # different sort order

        # super table & normal table mixed up
        tdSql.query(
            "(select c3 from union_tb0 limit 2) union all (select sum(c1) as c3 from union_mt0) order by c3;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 0)

        tdSql.checkData(1, 0, 1)

        tdSql.checkData(2, 0, 495000)

        # type compatible
        tdSql.query(
            "(select c3 from union_tb0 limit 2) union all (select sum(c1) as c3 from union_tb1) order by c3;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 0)

        tdSql.checkData(1, 0, 1)

        tdSql.checkData(2, 0, 49500)

        # two join subclause
        tdSql.query(
            "(select count(*) as c from union_tb0, union_tb1 where union_tb0.ts=union_tb1.ts) union all (select union_tb0.c3 as c from union_tb0, union_tb1 where union_tb0.ts=union_tb1.ts limit 10) order by c desc"
        )
        tdSql.checkRows(11)

        tdSql.checkData(0, 0, 1000)

        tdSql.checkData(1, 0, 9)

        tdSql.checkData(2, 0, 8)

        tdSql.checkData(9, 0, 1)

        tdLog.info(f"===========================================tags union")
        # two super table tag union, limit is not active during retrieve tags query
        tdSql.query("(select t1 from union_mt0) union all (select t1 from union_mt0)")
        tdSql.checkRows(20000)

        #        tdSql.query("select t1 from union_mt0 union all select t1 from union_mt0 limit 1
        # if $row != 11 then
        #  return -1
        # endi
        # ========================================== two super table join subclause
        tdLog.info(f"================two super table join subclause")
        tdSql.query(
            "(select _wstart as ts, avg(union_mt0.c1) as c from union_mt0 interval(1h) limit 10) union all (select union_mt1.ts, union_mt1.c1/1.0 as c from union_mt0, union_mt1 where union_mt1.ts=union_mt0.ts and union_mt1.t1=union_mt0.t1 limit 5);"
        )
        tdLog.info(f"the rows value is: {tdSql.getRows()})")
        tdSql.checkRows(15)

        # first subclause are empty
        tdSql.query(
            "(select count(*) as c from union_tb0 where ts > now + 3650d) union all (select sum(c1) as c from union_tb1);"
        )
        tdSql.checkRows(2)

        # if $tdSql.getData(0,0,49500 then
        #  return -1
        # endi

        # all subclause are empty
        tdSql.query(
            "(select c1 from union_tb0 limit 0) union all (select c1 from union_tb1 where ts>'2021-1-1 0:0:0')"
        )
        tdSql.checkRows(0)

        # middle subclause empty
        tdSql.query(
            "(select c1 from union_tb0 limit 1) union all (select c1 from union_tb1 where ts>'2030-1-1 0:0:0' union all select last(c1) as c1 from union_tb1) order by c1;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 0)

        tdSql.checkData(1, 0, 99)

        # multi-vnode projection query
        tdSql.query("(select c1 from union_mt0) union all select c1 from union_mt0;")
        tdSql.checkRows(20000)

        # multi-vnode projection query + limit
        tdSql.query(
            "(select ts, c1 from union_mt0 limit 1) union all (select ts, c1 from union_mt0 limit 1);"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2020-01-05 13:51:24.000")

        tdSql.checkData(0, 1, 0)

        tdSql.checkData(1, 0, "2020-01-05 13:51:24.000")

        tdSql.checkData(1, 1, 0)

        # two aggregated functions for super tables
        tdSql.query(
            "(select _wstart as ts, sum(c1) as a from union_mt0 interval(1s) limit 9) union all (select ts, max(c3) as a from union_mt0 limit 2) order by ts;"
        )
        tdSql.checkRows(10)

        tdSql.checkData(0, 0, "2020-01-05 13:51:24.000")

        tdSql.checkData(0, 1, 0)

        tdSql.checkData(1, 0, "2020-01-05 13:52:24.000")

        tdSql.checkData(1, 1, 10)

        tdSql.checkData(2, 0, "2020-01-05 13:53:24.000")

        tdSql.checkData(2, 1, 20)

        tdSql.checkData(9, 0, "2020-01-05 15:30:24.000")

        tdSql.checkData(9, 1, 99)

        # =================================================================================================
        # two aggregated functions for normal tables
        tdSql.query(
            "(select sum(c1) as a from union_tb0 limit 1) union all (select sum(c3) as a from union_tb1 limit 2);"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 49500)

        tdSql.checkData(1, 0, 49500)

        # two super table query + interval + limit
        tdSql.query(
            "(select ts, first(c3) as a from union_mt0 limit 1) union all (select _wstart as ts, sum(c3) as a from union_mt0 interval(1h) limit 1) order by ts desc;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2020-01-05 13:51:24.000")

        tdSql.checkData(0, 1, 0)

        tdSql.checkData(1, 0, "2020-01-05 13:00:00.000")

        tdSql.checkData(1, 1, 360)

        tdSql.query(
            "(select 'aaa' as option from union_tb1 where c1 < 0 limit 1) union all (select 'bbb' as option from union_tb0 limit 1)"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "bbb")

        tdSql.error(f"(show tables) union all (show tables)")
        tdSql.error(f"(show stables) union all (show stables)")
        tdSql.error(f"(show databases) union all (show databases)")

    def UnionSysinfo(self):
        tdSql.query("(select server_status()) union all (select server_status())")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)

        tdSql.query("(select client_version()) union all (select server_version())")
        tdSql.checkRows(2)

        tdSql.query("(select database()) union all (select database())")
        tdSql.checkRows(2)

    def UnionAllAsSystable(self):
        tdSql.execute(f"create database test;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"CREATE STABLE bw_yc_h_substation_mea (ts TIMESTAMP, create_date VARCHAR(50), create_time VARCHAR(30), load_time TIMESTAMP, sum_p_value FLOAT, sum_sz_value FLOAT, sum_gl_ys FLOAT, sum_g_value FLOAT) TAGS (id VARCHAR(50), name NCHAR(200), datasource VARCHAR(50), sys_flag VARCHAR(50));"
        )
        tdSql.execute(
            f"CREATE STABLE aw_yc_h_substation_mea (ts TIMESTAMP, create_date VARCHAR(50), create_time VARCHAR(30), load_time TIMESTAMP, sum_p_value FLOAT, sum_sz_value FLOAT, sum_gl_ys FLOAT, sum_g_value FLOAT) TAGS (id VARCHAR(50), name NCHAR(200), datasource VARCHAR(50), sys_flag VARCHAR(50));"
        )
        tdSql.execute(
            f"CREATE STABLE dw_yc_h_substation_mea (ts TIMESTAMP, create_date VARCHAR(50), create_time VARCHAR(30), load_time TIMESTAMP, sum_p_value FLOAT, sum_sz_value FLOAT, sum_gl_ys FLOAT, sum_g_value FLOAT) TAGS (id VARCHAR(50), name NCHAR(200), datasource VARCHAR(50), sys_flag VARCHAR(50));"
        )
        tdSql.execute(
            f"insert into t1 using dw_yc_h_substation_mea tags('1234567890','testa','0021001','abc01') values(now,'2023-03-27','00:01:00',now,2.3,3.3,4.4,5.5);"
        )
        tdSql.execute(
            f"insert into t2 using dw_yc_h_substation_mea tags('2234567890','testb','0022001','abc02') values(now,'2023-03-27','00:01:00',now,2.3,2.3,2.4,2.5);"
        )
        tdSql.execute(
            f"insert into t3 using aw_yc_h_substation_mea tags('2234567890','testc','0023001','abc03') values(now,'2023-03-27','00:15:00',now,2.3,2.3,2.4,2.5);"
        )
        tdSql.execute(
            f"insert into t4 using bw_yc_h_substation_mea tags('4234567890','testd','0021001','abc03') values(now,'2023-03-27','00:45:00',now,2.3,2.3,2.4,2.5);"
        )
        tdSql.execute(
            f"insert into t5 using bw_yc_h_substation_mea tags('5234567890','testd','0021001','abc03') values(now,'2023-03-27','00:00:00',now,2.3,2.3,2.4,2.5);"
        )
        tdSql.query(
            f"select t.ts,t.id,t.name,t.create_date,t.create_time,t.datasource,t.sum_p_value from (select ts,id,name,create_date,create_time,datasource,sum_p_value from bw_yc_h_substation_mea where create_date='2023-03-27' and substr(create_time,4,2) in ('00','15','30','45') union all select ts,id,name,create_date,create_time,datasource,sum_p_value from aw_yc_h_substation_mea where create_date='2023-03-27' and substr(create_time,4,2) in ('00','15','30','45') union all select ts,id,name,create_date,create_time,datasource,sum_p_value from dw_yc_h_substation_mea where create_date='2023-03-27' and substr(create_time,4,2) in ('00','15','30','45'))  t where t.datasource='0021001' and t.id='4234567890' order by t.create_time;"
        )

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, "4234567890")

        tdSql.checkData(0, 5, "0021001")

        tdSql.execute(f"create table st (ts timestamp, f int) tags (t int);")
        tdSql.execute(f"insert into ct1 using st tags(1) values(now, 1)(now+1s, 2)")
        tdSql.execute(f"insert into ct2 using st tags(2) values(now+2s, 3)(now+3s, 4)")
        tdSql.query(
            f"select count(*) from (select * from ct1 union all select * from ct2)"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 4)

        tdSql.query(f"select count(*) from (select * from ct1 union select * from ct2)")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 4)

        tdSql.execute(f"create table ctcount(ts timestamp, f int);")
        tdSql.execute(f"insert into ctcount(ts) values(now)(now+1s);")
        tdSql.query(f"select count(*) from (select f from ctcount);")
        tdLog.info(f"{tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select count(*) from (select f, f from ctcount)")
        tdLog.info(f"{tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select count(*) from (select last(ts), first(ts) from ctcount);")
        tdLog.info(f"{tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)

        tdSql.error(f"select f from (select f, f from ctcount);")

    def UnionPrecision(self):
        tdSql.execute(f"create  database tt precision 'us';")
        tdSql.execute(f"use tt ;")
        tdSql.execute(
            f"CREATE TABLE t_test_table (   ts TIMESTAMP,   a NCHAR(80),   b NCHAR(80),   c NCHAR(80) );"
        )
        tdSql.execute(
            f"insert into t_test_table values('2024-04-07 14:30:22.823','aa','aa', 'aa');"
        )
        tdSql.query(
            f"select * from t_test_table t  union all  select * from t_test_table t  ;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2024-04-07 14:30:22.823")
