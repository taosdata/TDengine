from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestTagBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tag_basic(self):
        """Tag query

        1. Projection queries combining data columns and the tbname pseudocolumn
        2. Projection queries with the DISTINCT keyword
        3. Queries combining tags and selection functions like LAST、FITST、MAX、MIN
        4. Grouping, sorting, and filtering by tags

        Catalog:
            - Query:Tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/parser/select_distinct_tag.sim
            - 2025-8-20 Simon Guan Migrated from tsim/parser/select_with_tags.sim
            - 2025-8-20 Simon Guan Migrated from tsim/query/tag_scan.sim

        """

        self.DistinctTag()
        tdStream.dropAllStreamsAndDbs()
        self.SelectTags()
        tdStream.dropAllStreamsAndDbs()
        self.TagScan()
        tdStream.dropAllStreamsAndDbs()
        
    def DistinctTag(self):   
        dbPrefix = "sav_db"
        tbPrefix = "sav_tb"
        stbPrefix = "sav_stb"
        tbNum = 20
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== alter.sim")
        i = 0
        db = dbPrefix
        stb = stbPrefix

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int, t2 int)"
        )

        i = 0
        ts = ts0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {stb} tags( {i} , 0 )")
            tdSql.execute(f"insert into {tb} (ts, c1) values (now, 1);")
            i = i + 1

        tdLog.info(f"====== table created")

        #### select distinct tag
        tdSql.query(f"select distinct t1 from {stb}")
        tdSql.checkRows(tbNum)

        #### select distinct tag
        tdSql.query(f"select distinct t2 from {stb}")
        tdSql.checkRows(1)

        #### unsupport sql
        tdSql.error(f"select distinct t1, t2 from &stb")

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def SelectTags(self):
        dbPrefix = "select_tags_db"
        tbPrefix = "select_tags_tb"
        mtPrefix = "select_tags_mt"

        tbNum = 16
        rowNum = 800
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== select_with_tags.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tstart = 100000

        tdSql.execute(f"create database if not exists {db} keep 36500")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12), t3 int)"
        )

        i = 0
        j = 1

        while i < tbNum:
            tb = tbPrefix + str(i)
            tg2 = "'abc" + str(i) + "'"
            tdSql.execute(f"create table {tb} using {mt} tags( {i} , {tg2} , 123 )")

            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                c = x % 100
                c1 = c + i

                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"

                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c1} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                tstart = tstart + 1
                x = x + 1

            i = i + 1
            j = j + 10000
            tstart = 100000 + j

        # ======================= only check first table tag, TD-4827
        tdSql.query(f"select count(*) from {mt} where t1 in (0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, rowNum)

        secTag = "'abc0'"
        tdSql.query(f"select count(*) from {mt} where t2 ={secTag}  and t1 in (0)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, rowNum)

        # ================================
        tdSql.query(f"select ts from select_tags_mt0")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(12800)

        tdSql.query(f"select first(ts), tbname, t1, t2 from select_tags_mt0;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")
        tdSql.checkData(0, 1, "select_tags_tb0")
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "abc0")

        tdSql.query(f"select last(ts), tbname, t1, t2 from select_tags_mt0;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "1970-01-01 08:04:10.800")
        tdSql.checkData(0, 1, "select_tags_tb15")
        tdSql.checkData(0, 2, 15)
        tdSql.checkData(0, 3, "abc15")

        tdSql.query(f"select min(c1), tbname, t1, t2 from select_tags_mt0;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, "select_tags_tb0")
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "abc0")

        tdSql.query(f"select max(c1), tbname, t1, t2 from select_tags_mt0;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 114)
        tdSql.checkData(0, 1, "select_tags_tb15")
        tdSql.checkData(0, 2, 15)
        tdSql.checkData(0, 3, "abc15")

        tdSql.query(f"select top(c6, 3) from select_tags_mt0 interval(10a)")
        tdSql.query(
            f"select top(c3,10) from select_tags_mt0 partition by tbname,t1,t2 interval(10a)"
        )
        tdSql.query(
            f"select top(c6, 3) from select_tags_mt0 partition by tbname interval(10a)"
        )

        tdSql.query(f"select top(c6, 10) from select_tags_mt0 interval(10a);")
        tdSql.checkRows(12800)

        tdSql.query(
            f"select ts, top(c1, 80), tbname, t1, t2 from select_tags_mt0 order by ts;"
        )
        tdSql.checkRows(80)
        tdSql.checkData(0, 0, "1970-01-01 08:03:40.100")
        tdSql.checkData(1, 0, "1970-01-01 08:03:40.200")
        tdSql.checkData(0, 1, 111)
        tdSql.checkData(0, 2, "select_tags_tb12")
        tdSql.checkData(0, 3, 12)
        tdSql.checkData(0, 4, "abc12")

        tdSql.query(
            f"select ts, top(c1, 80), tbname, t1, t2 from select_tags_mt0 order by ts;"
        )
        tdSql.checkRows(80)
        tdSql.checkData(0, 0, "1970-01-01 08:03:40.100")
        tdSql.checkData(1, 0, "1970-01-01 08:03:40.200")
        tdSql.checkData(0, 1, 111)
        tdSql.checkData(0, 2, "select_tags_tb12")
        tdSql.checkData(0, 3, 12)
        tdSql.checkData(0, 4, "abc12")

        tdSql.query(
            f"select ts, bottom(c1, 72), tbname, t1, t2 from select_tags_mt0 order by ts;"
        )
        tdSql.checkRows(72)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")
        tdSql.checkData(1, 0, "1970-01-01 08:01:40.001")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, "select_tags_tb0")
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, "abc0")

        tdSql.query(f"select last_row(c1, c2), tbname, t1, t2 from select_tags_mt0;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 114)

        tdLog.info(f"{tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, 99.00000)
        tdSql.checkData(0, 2, "select_tags_tb15")
        tdSql.checkData(0, 3, 15)

        tdLog.info(f"====== selectivity+tags+group by tags=======================")
        tdSql.query(
            f"select first(c1), tbname, t1, t2, tbname from select_tags_mt0 group by tbname order by t1;"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)

        tdLog.info(f"{tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, "select_tags_tb0")
        tdSql.checkData(1, 1, "select_tags_tb1")
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "abc0")
        tdSql.checkData(0, 4, "select_tags_tb0")

        tdSql.query(
            f"select last_row(ts,c1), tbname, t1, t2, tbname from select_tags_mt0 group by tbname order by t1;"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.799")
        tdSql.checkData(1, 0, "1970-01-01 08:01:50.800")

        tdLog.info(f"{tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, 99)
        tdSql.checkData(1, 1, 100)
        tdSql.checkData(0, 2, "select_tags_tb0")
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, "abc0")

        tdLog.info(f'"really this line"')
        tdSql.query(
            f"select distinct tbname,t1,t2 from select_tags_mt0 order by tbname;"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 0, "select_tags_tb0")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, "abc0")
        tdSql.checkData(1, 0, "select_tags_tb1")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, "abc1")

        tdSql.query(f"select tbname,ts from select_tags_mt0 order by ts;")
        tdSql.checkRows(12800)

        tdLog.info(f"{tdSql.getData(0,0)} , {tdSql.getData(0,1)}")
        tdSql.checkData(0, 0, "select_tags_tb0")
        tdSql.checkData(1, 0, "select_tags_tb0")
        tdSql.checkData(0, 1, "1970-01-01 08:01:40.000")
        tdSql.checkData(1, 1, "1970-01-01 08:01:40.001")

        tdSql.query(
            f"select ts, top(c1, 100), tbname, t1, t2 from select_tags_mt0 where tbname in ('select_tags_tb0', 'select_tags_tb1') group by tbname order by ts;"
        )
        tdSql.checkRows(200)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.087")
        tdSql.checkData(1, 0, "1970-01-01 08:01:40.088")
        tdSql.checkData(2, 0, "1970-01-01 08:01:40.089")
        tdSql.checkData(9, 0, "1970-01-01 08:01:40.096")
        tdSql.checkData(0, 1, 87)
        tdSql.checkData(0, 2, "select_tags_tb0")
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, "abc0")

        tdSql.query(
            f"select ts, top(c1, 100), tbname, t1, t2 from select_tags_mt0 where tbname in ('select_tags_tb0', 'select_tags_tb1') partition by tbname order by ts;"
        )
        tdSql.checkRows(200)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.087")
        tdSql.checkData(1, 0, "1970-01-01 08:01:40.088")
        tdSql.checkData(2, 0, "1970-01-01 08:01:40.089")
        tdSql.checkData(9, 0, "1970-01-01 08:01:40.096")
        tdSql.checkData(0, 1, 87)
        tdSql.checkData(0, 2, "select_tags_tb0")
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, "abc0")

        tdSql.query(
            f"select ts, top(c1, 2), t2, tbname, t2 from select_tags_mt0 where tbname in ('select_tags_tb0', 'select_tags_tb1') group by tbname,t2 order by ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.099")
        tdSql.checkData(0, 1, 99)
        tdSql.checkData(0, 2, "abc0")
        tdSql.checkData(0, 3, "select_tags_tb0")
        tdSql.checkData(0, 4, "abc0")
        tdSql.checkData(1, 0, "1970-01-01 08:01:40.199")
        tdSql.checkData(1, 1, 99)
        tdSql.checkData(1, 2, "abc0")
        tdSql.checkData(1, 3, "select_tags_tb0")
        tdSql.checkData(1, 4, "abc0")
        tdSql.checkData(2, 0, "1970-01-01 08:01:50.100")
        tdSql.checkData(2, 1, 100)
        tdSql.checkData(2, 2, "abc1")
        tdSql.checkData(2, 3, "select_tags_tb1")
        tdSql.checkData(2, 4, "abc1")
        tdSql.checkData(3, 0, "1970-01-01 08:01:50.200")
        tdSql.checkData(3, 1, 100)
        tdSql.checkData(3, 2, "abc1")
        tdSql.checkData(3, 3, "select_tags_tb1")
        tdSql.checkData(3, 4, "abc1")

        # slimit /limit
        tdSql.query(
            f"select ts, top(c1, 2), t2 from select_tags_mt0 where tbname in ('select_tags_tb0', 'select_tags_tb1') group by tbname,t2 limit 2 offset 1;"
        )
        tdSql.checkRows(2)

        tdLog.info(
            f"======= selectivity + tags + group by + tags + filter ==========================="
        )
        tdSql.query(
            f"select first(c1), t1, tbname from select_tags_mt0 where c1<=2 group by tbname order by t1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, "select_tags_tb0")
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, "select_tags_tb1")
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, "select_tags_tb2")

        tdSql.query(
            f"select _wstart, first(c1), tbname from select_tags_mt0 where c1<=2 interval(1s);"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, "select_tags_tb0")
        tdSql.checkData(1, 0, "1970-01-01 08:01:50.000")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, "select_tags_tb1")
        tdSql.checkData(2, 0, "1970-01-01 08:02:00.000")
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, "select_tags_tb2")

        tdSql.query(f"select first(ts),ts from select_tags_tb0 where c1<3")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tdSql.getData(0, 1))

        tdSql.query(f"select last(ts),ts from select_tags_tb0 where c1<3")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tdSql.getData(0, 1))

        tdSql.query(f"select first(ts), ts  from select_tags_tb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "1970-01-01 08:01:50.001")

        tdLog.info(
            f"======= selectivity + tags + group by + tags + filter + interval ================"
        )
        tdSql.query(
            f"select _wstart,first(c1), t2, t1, tbname, tbname from select_tags_mt0 where c1<=2 partition by tbname interval(1d) order by t1;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "1970-01-01 00:00:00.000")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, "abc0")
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, "select_tags_tb0")
        tdSql.checkData(0, 5, "select_tags_tb0")
        tdSql.checkData(1, 5, "select_tags_tb1")
        tdSql.checkData(2, 5, "select_tags_tb2")

        tdSql.query(
            f"select ts, top(c1, 5), t2, tbname from select_tags_mt0 where c1<=2 partition by tbname interval(1d) order by ts, t2;"
        )
        tdSql.checkRows(15)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.002")
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, "abc0")
        tdSql.checkData(0, 3, "select_tags_tb0")
        tdSql.checkData(9, 0, "1970-01-01 08:01:50.402")
        tdSql.checkData(9, 1, 2)
        tdSql.checkData(9, 2, "abc1")
        tdSql.checkData(9, 3, "select_tags_tb1")

        # if data
        tdSql.query(
            f"select ts, top(c1, 50), t2, t1, tbname, tbname from select_tags_mt0 where c1<=2  partition by tbname interval(1d) order by ts, t2;"
        )
        tdSql.checkRows(48)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, "abc0")
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, "select_tags_tb0")
        tdSql.checkData(0, 5, "select_tags_tb0")
        tdSql.checkData(1, 0, "1970-01-01 08:01:40.001")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, "abc0")
        tdSql.checkData(1, 3, 0)
        tdSql.checkData(1, 4, "select_tags_tb0")
        tdSql.checkData(1, 5, "select_tags_tb0")
        tdSql.checkData(9, 0, "1970-01-01 08:01:40.300")
        tdSql.checkData(9, 1, 0)
        tdSql.checkData(9, 2, "abc0")
        tdSql.checkData(9, 3, 0)
        tdSql.checkData(9, 4, "select_tags_tb0")
        tdSql.checkData(9, 5, "select_tags_tb0")

        tdSql.query(f"select last(ts),TBNAME from select_tags_mt0 interval(1y)")
        tdSql.checkRows(1)

        tdLog.info(
            f"TODO ======= selectivity + tags+ group by + tags + filter + interval + join==========="
        )

        tdLog.info(
            f"==========================mix tag columns and group by columns======================"
        )
        tdSql.query(
            f"select ts, top(c1, 100), tbname, t3 from select_tags_mt0 where tbname in ('select_tags_tb0', 'select_tags_tb1') group by t3 order by ts, tbname;"
        )
        tdSql.checkRows(100)
        tdSql.checkData(0, 0, "1970-01-01 08:01:40.094")
        tdSql.checkData(0, 1, 94)
        tdSql.checkData(0, 2, "select_tags_tb0")
        tdSql.checkData(0, 3, 123)
        tdSql.checkData(1, 0, "1970-01-01 08:01:40.095")
        tdSql.checkData(1, 1, 95)
        tdSql.checkData(1, 2, "select_tags_tb0")
        tdSql.checkData(1, 3, 123)

        tdLog.info(f"======error sql=============================================")
        tdSql.error(f"select first(*), tbname from select_tags_mt0;")
        tdSql.error(f"select first(ts), first(c1),tbname from select_tags_mt0;")
        tdSql.error(f"select first(ts), last(ts), tbname from select_tags_mt0;")
        tdSql.error(
            f"select last_row(*), first(ts), tbname, t1, t2 from select_tags_mt0;"
        )
        tdSql.error(f"select tbname, last_row(*), t1, first(ts) from select_tags_mt0;")
        tdSql.error(f"select count(*), tbname from select_tags_mt0;")
        tdSql.error(f"select sum(c2), tbname from select_tags_mt0;")
        tdSql.error(f"select avg(c3), tbname from select_tags_mt0;")
        tdSql.error(f"select percentile(c3, 50), tbname from select_tags_mt0;")
        tdSql.error(f"select apercentile(c4, 50), tbname from select_tags_mt0;")
        tdSql.error(f"select spread(c2), tbname, t1 from select_tags_mt0;")
        tdSql.error(f"select stddev(c2), tbname from select_tags_mt0;")
        tdSql.error(f"select twa(c2), tbname from select_tags_mt0;")
        tdSql.error(f"select interp(c2), tbname from select_tags_mt0 where ts=100001;")

        tdSql.query(f"select count(tbname) from select_tags_mt0 interval(1d);")
        tdSql.query(f"select count(tbname) from select_tags_mt0 group by t1;")
        tdSql.query(f"select count(tbname),SUM(T1) from select_tags_mt0 interval(1d);")
        tdSql.error(
            f"select first(c1), count(*), t2, t1, tbname from select_tags_mt0 where c1<=2 interval(1d) group by tbname;"
        )
        tdSql.error(f"select ts from select_tags_mt0 interval(1y);")
        tdSql.error(f"select count(*), tbname from select_tags_mt0 interval(1y);")
        tdSql.error(f"select tbname, t1 from select_tags_mt0 interval(1y);")

        # ===error sql + group by ===============================================
        # valid sql: select first(c1), last(c2), tbname from select_tags_mt0 group by tbname;
        # valid sql: select first(c1), last(c2), count(*), tbname from select_tags_mt0 group by tbname;
        # valid sql: select first(c1), last(c2), count(*) from select_tags_mt0 group by tbname, t1;
        # valid sql: select first(c1), tbname, t1 from select_tags_mt0 group by t2;

        tdLog.info(f"==================================>TD-4231")
        tdSql.query(f"select t1,tbname from select_tags_mt0 where c1<0")
        tdSql.query(
            f"select t1,tbname from select_tags_mt0 where c1<0 and tbname in ('select_tags_tb12')"
        )
        tdSql.query(
            f"select tbname from select_tags_mt0 where tbname in ('select_tags_tb12');"
        )

        tdSql.query(f"select first(ts), tbname from select_tags_mt0 group by tbname;")
        tdSql.query(
            f"select count(c1) from select_tags_mt0 where c1=99 group by tbname;"
        )
        tdSql.query(f"select count(*),tbname from select_tags_mt0 group by tbname")

        tdLog.info(f"==================================> tag supported in group")
        tdSql.query(f"select t1,t2,tbname from select_tags_mt0 group by tbname;")
        tdSql.query(
            f"select first(c1), last(c2), t1 from select_tags_mt0 group by tbname;"
        )
        tdSql.query(
            f"select first(c1), last(c2), tbname, t2 from select_tags_mt0 group by tbname;"
        )
        tdSql.query(
            f"select first(c1), count(*), t2, t1, tbname from select_tags_mt0 group by tbname;"
        )

    def TagScan(self):
        tdSql.connect("root")
        tdSql.execute(f"drop database if exists test")
        tdSql.execute(f"create database test;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table st(ts timestamp, f int) tags (t int);")
        tdSql.execute(f"insert into ct1 using st tags(1) values(now, 1);")
        tdSql.execute(f"insert into ct2 using st tags(2) values(now, 2);")
        tdSql.execute(f"insert into ct3 using st tags(3) values(now, 3);")
        tdSql.execute(f"insert into ct4 using st tags(4) values(now, 4);")

        tdSql.execute(f"create table st2(ts timestamp, f int) tags (t int);")
        tdSql.execute(f"insert into ct21 using st2 tags(1) values(now, 1);")
        tdSql.execute(f"insert into ct22 using st2 tags(2) values(now, 2);")
        tdSql.execute(f"insert into ct23 using st2 tags(3) values(now, 3);")
        tdSql.execute(f"insert into ct24 using st2 tags(4) values(now, 4);")

        tdSql.query(f"select tbname, 1 from st group by tbname order by tbname;")
        tdLog.info(
            f"{tdSql.getRows()})  {tdSql.getData(0,0)} {tdSql.getData(1,0)} {tdSql.getData(2,0)}"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "ct1")
        tdSql.checkData(1, 0, "ct2")

        tdSql.query(f"select tbname, 1 from st group by tbname slimit 0, 1;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select tbname, 1 from st group by tbname slimit 2, 2;")
        tdLog.info(f"{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(1,0)}")
        tdSql.checkRows(2)

        tdSql.query(
            f"select tbname, 1 from st group by tbname order by tbname slimit 0, 1;"
        )
        tdLog.info(
            f"{tdSql.getRows()})  {tdSql.getData(0,0)} {tdSql.getData(1,0)} {tdSql.getData(2,0)}"
        )
        tdSql.checkRows(4)

        tdSql.execute(
            f"create table stt1(ts timestamp, f int) tags (t int, b varchar(10));"
        )
        tdSql.execute(f"insert into ctt11 using stt1 tags(1, '1aa') values(now, 1);")
        tdSql.execute(f"insert into ctt12 using stt1 tags(2, '1bb') values(now, 2);")
        tdSql.execute(f"insert into ctt13 using stt1 tags(3, '1cc') values(now, 3);")
        tdSql.execute(f"insert into ctt14 using stt1 tags(4, '1dd') values(now, 4);")
        tdSql.execute(f"insert into ctt14 values(now, 5);")

        tdSql.execute(
            f"create table stt2(ts timestamp, f int) tags (t int, b varchar(10));"
        )
        tdSql.execute(f"insert into ctt21 using stt2 tags(1, '2aa') values(now, 1);")
        tdSql.execute(f"insert into ctt22 using stt2 tags(2, '2bb') values(now, 2);")
        tdSql.execute(f"insert into ctt23 using stt2 tags(3, '2cc') values(now, 3);")
        tdSql.execute(f"insert into ctt24 using stt2 tags(4, '2dd') values(now, 4);")

        tdSql.query(f"select tags t, b from stt1 order by t")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(3,0)} {tdSql.getData(3,1)}"
        )
        tdSql.checkRows(4)
        tdSql.checkData(3, 1, "1dd")

        tdSql.query(f"select tags t, b from stt2 order by t")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(3,0)} {tdSql.getData(3,1)}"
        )
        tdSql.checkRows(4)
        tdSql.checkData(3, 1, "2dd")

        tdSql.query(f"select tags t,b,f from stt1 order by t")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(5)
        tdSql.checkData(4, 2, 5)

        tdSql.query(f"select tags tbname,t,b from stt1 order by t")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(4)
        tdSql.checkData(3, 0, "ctt14")
        tdSql.checkData(3, 2, "1dd")

        tdSql.query(f"select tags t,b from stt1 where t=1")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, "1aa")

        tdSql.query(f"select tags t,b from stt1 where tbname='ctt11'")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, "1aa")

        tdSql.query(f"select tags t,b from ctt11")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, "1aa")

        tdSql.execute(f"create table stb34 (ts timestamp, f int) tags(t int);")
        tdSql.execute(
            f"insert into ctb34 using stb34 tags(1) values(now, 1)(now+1s, 2);"
        )
        tdSql.query(f"select 1 from (select tags t from stb34 order by t)")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select count(*) from (select tags t from stb34)")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select 1 from (select tags ts from stb34)")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(2)

        tdSql.query(f"select count(*) from (select tags ts from stb34)")
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select tags 2 from stb34")
        tdSql.checkRows(1)
