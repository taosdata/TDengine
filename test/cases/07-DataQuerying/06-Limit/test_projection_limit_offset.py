from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestProjectionLimitOffset:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_projection_limit_offset(self):
        """Projection limit offset

        1.

        Catalog:
            - Query:Limit

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/projection_limit_offset.sim

        """

        dbPrefix = "group_db"
        tbPrefix = "group_tb"
        mtPrefix = "group_mt"
        tbNum = 8
        rowNum = 1000
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== projection_limit_offset.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tstart = 100000

        tdSql.execute(f"create database if not exists {db} keep 36500")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12))"
        )

        i = 0
        half = tbNum / 2

        while i < half:
            tb = tbPrefix + str(i)
            tg2 = "'abc'"
            tbId = int(i + half)
            tb1 = tbPrefix + str(tbId)

            tdSql.execute(f"create table {tb} using {mt} tags( {i} , {tg2} )")
            tdSql.execute(f"create table {tb1} using {mt} tags( {i} , {tg2} )")

            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                c = x % 100

                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"

                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} ) {tb1} values ({tstart} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                tstart = tstart + 1
                x = x + 1

            i = i + 1
            tstart = 100000

        i1 = 1
        i2 = 0

        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        dbPrefix = "group_db"
        tbPrefix = "group_tb"
        mtPrefix = "group_mt"

        tb1 = tbPrefix + str(i1)
        tb2 = tbPrefix + str(i2)
        
        # ===============select * from super_table limit/offset[TBASE-691]=================================
        tdSql.query(f"select ts from group_mt0")
        tdLog.info(f"{tdSql.getRows()})")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' limit 8000 offset 0;"
        )
        tdSql.checkRows(4008)

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' limit 8000 offset 1;"
        )
        tdSql.checkRows(4007)

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' limit 8000 offset 101;"
        )
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(3907)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.101")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' limit 8000 offset 902;"
        )
        tdSql.checkRows(3106)

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' limit 8000 offset 400;"
        )
        tdSql.checkRows(3608)

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' limit 8000 offset 4007;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' limit 2000 offset 4008;"
        )
        tdSql.checkRows(0)

        # ==================================order by desc, multi vnode, limit/offset===================================
        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' order by ts desc limit 8000 offset 0;"
        )
        tdSql.checkRows(4008)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.500")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' order by ts desc limit 8000 offset 1;"
        )
        tdSql.checkRows(4007)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.500")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' order by ts desc limit 8000 offset 101;"
        )
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(3907)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.488")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' order by ts desc limit 8000 offset 902;"
        )
        tdSql.checkRows(3106)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.388")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' order by ts desc limit 8000 offset 400;"
        )
        tdSql.checkRows(3608)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.450")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' order by ts desc limit 8000 offset 4007;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")

        tdSql.query(
            f"select ts from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.500' order by ts desc limit 2000 offset 4008;"
        )
        tdSql.checkRows(0)

        # =================================single value filter======================================
        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts asc limit 10 offset 0;"
        )
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts asc limit 10 offset 1;"
        )
        tdSql.checkRows(7)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts asc limit 10 offset 2;"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts asc limit 10 offset 4;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts asc limit 10 offset 7;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts asc limit 10 offset 8;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts asc limit 10 offset 9;"
        )
        tdSql.checkRows(0)

        # ===============================single value filter, order by desc============================
        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts desc limit 10 offset 0;"
        )
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts desc limit 10 offset 1;"
        )
        tdSql.checkRows(7)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts desc limit 10 offset 2;"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts desc limit 10 offset 4;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts desc limit 10 offset 7;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.000")

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts desc limit 10 offset 8;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-1-1 8:1:40' and ts<='1970-1-1 8:1:40.00' order by ts desc limit 10 offset 9;"
        )
        tdSql.checkRows(0)

        # [tbase-695]
        tdSql.query(
            f"select ts,tbname from group_mt0 where ts>='1970-01-01 8:1:40' and ts<'1970-1-1 8:1:40.500' and c1<99999999 limit 10000 offset 500"
        )
        tdSql.checkRows(3500)

        # =================================parse error sql==========================================
        tdSql.error(
            f"select ts,tbname from group_mt0 order by ts desc limit 100 offset -1;"
        )
        tdSql.error(
            f"select ts,tbname from group_mt0 order by c1 asc limit 100 offset -1;"
        )
        tdSql.error(f"select ts,tbname from group_mt0 order by ts desc limit -1, 100;")
        tdSql.error(f"select ts,tbname from group_mt0 order by ts desc slimit -1, 100;")
        tdSql.error(
            f"select ts,tbname from group_mt0 order by ts desc slimit 1 soffset 1;"
        )

        # ================================functions applys to sql===================================
        tdSql.query(f"select first(t1) from group_mt0;")
        tdSql.query(f"select last(t1) from group_mt0;")
        tdSql.query(f"select min(t1) from group_mt0;")
        tdSql.query(f"select max(t1) from group_mt0;")
        tdSql.query(f"select top(t1, 20) from group_mt0;")
        tdSql.query(f"select bottom(t1, 20) from group_mt0;")
        tdSql.query(f"select avg(t1) from group_mt0;")
        tdSql.error(f"select percentile(t1, 50) from group_mt0;")
        tdSql.error(f"select percentile(t1, 50) from group_mt0;")
        tdSql.error(f"select percentile(t1, 50) from group_mt0;")

        # ====================================tbase-722==============================================
        tdLog.info(f"tbase-722")
        tdSql.query(f"select spread(ts) from group_tb0;")
        tdLog.info(f"{tdSql.getData(0,0)}")

        tdSql.checkData(0, 0, 999.000000000)

        # ====================================tbase-716==============================================
        tdLog.info(f"tbase-716")
        tdSql.query(
            f"select count(*) from group_tb0 where ts in ('2016-1-1 12:12:12');"
        )
        tdSql.error(f"select count(*) from group_tb0 where ts < '12:12:12';")

        # ===============================sql for twa==========================================
        tdSql.error(f"select twa(c1) from group_stb0;")
        tdSql.error(
            f"select twa(c2) from group_stb0 where ts<now and ts>now-1h group by t1;"
        )
        tdSql.error(
            f"select twa(c2) from group_stb0 where ts<now and ts>now-1h group by tbname,t1;"
        )
        tdSql.error(f"select twa(c2) from group_stb0 group by tbname,t1;")
        tdSql.error(f"select twa(c2) from group_stb0 group by tbname;")
        tdSql.error(f"select twa(c2) from group_stb0 group by t1;")
        tdSql.error(
            f"select twa(c2) from group_stb0 where ts<now and ts>now-1h group by t1,tbname;"
        )

        # ================================first/last error check================================
        tdSql.execute(f"create table m1 (ts timestamp, k int) tags(a int);")
        tdSql.execute(f"create table tm0 using m1 tags(1);")
        tdSql.execute(f"create table tm1 using m1 tags(2);")

        tdSql.execute(
            f"insert into tm0 values(10000, 1) (20000, 2)(30000, 3) (40000, NULL) (50000, 2) tm1 values(10001, 2)(20000,4)(90000,9);"
        )

        # =============================tbase-1205
        tdSql.query(
            f"select count(*) from tm1 where ts<now and ts>= now -1d interval(1h) fill(NULL);"
        )
        tdSql.checkRows(0)

        tdLog.info(f"===================>TD-1834")
        tdSql.query(f"select * from tm0 where ts>11000 and ts< 20000 order by ts asc")
        tdSql.checkRows(0)

        tdSql.query(f"select * from tm0 where ts>11000 and ts< 20000 order by ts desc")
        tdSql.checkRows(0)

        tdSql.query(
            f"select _wstart, count(*),first(k),last(k) from m1 where tbname in ('tm0') interval(1s) order by _wstart desc;"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "1970-01-01 08:00:50.000")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 2)

        tdSql.checkData(1, 1, 1)

        tdSql.checkData(1, 2, None)

        tdSql.checkData(1, 3, None)

        tdLog.info(f"=============tbase-1324")
        tdSql.query(f"select a, k-k from m1")
        tdSql.checkRows(8)

        tdSql.query(f"select diff(k) from tm0")
        tdSql.checkRows(4)

        tdSql.checkData(2, 0, None)

        # error sql
        tdSql.error(f"select * from 1;")
        # sql_error select 1;  // equals to select server_status();
        tdSql.error(f"select k+ str(k);")
        tdSql.error(f"select k+1;")
        tdSql.error(f"select abc();")
        tdSql.query(f"select 1 where 1=2;")
        tdSql.query(f"select 1 limit 1;")
        tdSql.query(f"select 1 slimit 1;")
        tdSql.query(f"select 1 interval(1h);")
        tdSql.error(f"select count(*);")
        tdSql.error(f"select sum(k);")
        tdSql.query(f"select 'abc';")
        tdSql.error(f"select k+1,sum(k) from tm0;")
        tdSql.error(f"select k, sum(k) from tm0;")
        tdSql.error(f"select k, sum(k)+1 from tm0;")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        # =============================tbase-1205
        tdSql.query(
            f"select count(*) from tm1 where ts<now and ts>= now -1d interval(1h) fill(NULL);"
        )

        tdLog.info(f"===================>TD-1834")
        tdSql.query(f"select * from tm0 where ts>11000 and ts< 20000 order by ts asc")
        tdSql.checkRows(0)

        tdSql.query(f"select * from tm0 where ts>11000 and ts< 20000 order by ts desc")
        tdSql.checkRows(0)
