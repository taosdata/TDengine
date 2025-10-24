import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestValgrindCheckError6:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_valgrind_check_error6(self):
        """valgrind check error 6

        1. -

        Catalog:
            - Others:Valgrind

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/valgrind/checkError6.sim

        """

        tdLog.info(
            f"=============== step1: create drop select * from information_schema.ins_dnodes"
        )
        clusterComCheck.checkDnodes(1)

        tbPrefix = "tb"
        tbNum = 5
        rowNum = 10

        tdLog.info(f"=============== step2: prepare data")
        tdSql.execute(f"create database db vgroups 2")
        tdSql.execute(f"use db")
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, tbcol int, tbcol2 float, tbcol3 double, tbcol4 binary(30), tbcol5 binary(30)) tags (tgcol int unsigned)"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using stb tags( {i} )")
            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                tdSql.execute(
                    f'insert into {tb} values ({ms} , {x} , {x} , {x} , "abcd1234=-+*" , "123456 0" )'
                )
                x = x + 1

            cc = x * 60000
            ms = 1601481600000 + cc
            tdSql.execute(
                f"insert into {tb} values ({ms} , NULL , NULL , NULL , NULL , NULL )"
            )
            i = i + 1

        tdLog.info(f"=============== step3: tb")
        tdSql.query(f"select avg(tbcol) from tb1")
        tdSql.query(f"select avg(tbcol) from tb1 where ts <= 1601481840000")
        tdSql.query(f"select avg(tbcol) as b from tb1")
        tdSql.query(f"select avg(tbcol) as b from tb1 interval(1d)")
        tdSql.query(
            f"select avg(tbcol) as b from tb1 where ts <= 1601481840000 interval(1m)"
        )
        tdSql.query(f"select bottom(tbcol, 2) from tb1 where ts <= 1601481840000")
        tdSql.query(f"select top(tbcol, 2) from tb1 where ts <= 1601481840000")
        tdSql.query(f"select percentile(tbcol, 2) from tb1 where ts <= 1601481840000")
        tdSql.query(
            f"select leastsquares(tbcol, 1, 1) as b from tb1 where ts <= 1601481840000"
        )
        tdSql.query(f"show table distributed tb1")
        tdSql.query(f"select count(1) from tb1")
        tdSql.query(
            f"select count(tbcol) as b from tb1 where ts <= 1601481840000 interval(1m)"
        )
        tdSql.query(f"select diff(tbcol) from tb1 where ts <= 1601481840000")
        tdSql.query(
            f"select diff(tbcol) from tb1 where tbcol > 5 and tbcol < 20 order by ts"
        )
        tdSql.query(
            f"select first(tbcol), last(tbcol) as b from tb1 where ts <= 1601481840000 interval(1m)"
        )
        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), sum(tbcol), stddev(tbcol) from tb1 where ts <= 1601481840000 partition by tgcol order by tgcol"
        )
        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), sum(tbcol), stddev(tbcol) from tb1 where ts <= 1601481840000 partition by tgcol interval(1m)"
        )
        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from tb1 where ts <= 1601481840000 and ts >= 1601481800000 partition by tgcol interval(1m) fill(value, 0,0,0,0,0)"
        )
        tdSql.query(f"select last_row(*) from tb1 where tbcol > 5 and tbcol < 20")
        tdSql.query(
            f"select _wstart, _wend, _wduration, _qstart, _qend,  count(*) from tb1 interval(10s, 2s) sliding(10s)"
        )
        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from tb1 where ts <= 1601481840000 and ts >= 1601481800000 partition by tgcol interval(1m) fill(value, 0,0,0,0,0)"
        )
        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from tb1 where ts <= 1601481840000 and ts >= 1601481800000 partition by tgcol interval(1m) fill(value, 0,0,0,0,0) order by tgcol desc"
        )
        tdSql.query(
            f"select log(tbcol), abs(tbcol), pow(tbcol, 2), sqrt(tbcol), sin(tbcol), cos(tbcol), tan(tbcol), asin(tbcol), acos(tbcol), atan(tbcol), ceil(tbcol), floor(tbcol), round(tbcol), atan(tbcol) from tb1"
        )
        tdSql.query(f'select length("abcd1234"), char_length("abcd1234=-+*") from tb1')
        tdSql.query(
            f"select tbcol4, length(tbcol4), lower(tbcol4), upper(tbcol4), ltrim(tbcol4), rtrim(tbcol4), concat(tbcol4, tbcol5), concat_ws('_', tbcol4, tbcol5), substr(tbcol4, 1, 4)  from tb1"
        )
        tdSql.query(f"select * from tb1 where tbcol not in (1,2,3,null);")
        tdSql.query(f"select * from tb1 where tbcol + 3 <> null;")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from tb1 where ts <= 1601481840000 and ts >= 1601481800000 partition by tgcol interval(1m) fill(value, 0,0,0,0,0)"
        )
        tdSql.query(f"select tbcol5 - tbcol3 from tb1")

        tdLog.info(f"=============== step4: stb")
        tdSql.query(f"select avg(tbcol) as c from stb")
        tdSql.query(f"select avg(tbcol) as c from stb where ts <= 1601481840000")
        tdSql.query(
            f"select avg(tbcol) as c from stb where tgcol < 5 and ts <= 1601481840000"
        )
        tdSql.query(f"select avg(tbcol) as c from stb interval(1m)")
        tdSql.query(f"select avg(tbcol) as c from stb interval(1d)")
        tdSql.query(
            f"select avg(tbcol) as b from stb where ts <= 1601481840000 interval(1m)"
        )
        tdSql.query(f"select avg(tbcol) as c from stb group by tgcol")
        tdSql.query(
            f"select avg(tbcol) as b from stb where ts <= 1601481840000 partition by tgcol interval(1m)"
        )
        tdSql.query(f"show table distributed stb")
        tdSql.query(f"select count(1) from stb")
        tdSql.query(
            f"select count(tbcol) as b from stb where ts <= 1601481840000 partition by tgcol interval(1m)"
        )
        # sql select diff(tbcol) from stb where ts <= 1601481840000
        tdSql.query(f"select first(tbcol), last(tbcol) as c from stb group by tgcol")
        tdSql.query(
            f"select first(tbcol), last(tbcol) as b from stb where ts <= 1601481840000 and tbcol2 is null partition by tgcol interval(1m)"
        )
        tdSql.query(
            f"select first(tbcol), last(tbcol) as b from stb where ts <= 1601481840000 partition by tgcol interval(1m)"
        )
        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), sum(tbcol), stddev(tbcol) from stb where ts <= 1601481840000 partition by tgcol interval(1m)"
        )
        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from stb where ts <= 1601481840000 and ts >= 1601481800000 partition by tgcol interval(1m) fill(value, 0,0,0,0,0)"
        )
        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from stb where ts <= 1601481840000 and ts >= 1601481800000 and tgcol = 1 partition by tgcol interval(1m) fill(value, 0,0,0,0,0) order by tgcol desc"
        )
        tdSql.query(
            f"select last_row(tbcol), stddev(tbcol) from stb where tbcol > 5 and tbcol < 20 group by tgcol"
        )
        tdSql.query(
            f"select _wstart, _wend, _wduration, _qstart, _qend,  count(*) from stb interval(10s, 2s) sliding(10s)"
        )
        tdSql.query(
            f"select log(tbcol), abs(tbcol), pow(tbcol, 2), sqrt(tbcol), sin(tbcol), cos(tbcol), tan(tbcol), asin(tbcol), acos(tbcol), atan(tbcol), ceil(tbcol), floor(tbcol), round(tbcol), atan(tbcol) from stb"
        )
        tdSql.query(f'select length("abcd1234"), char_length("abcd1234=-+*") from stb')
        tdSql.query(
            f"select tbcol4, length(tbcol4), lower(tbcol4), upper(tbcol4), ltrim(tbcol4), rtrim(tbcol4), concat(tbcol4, tbcol5), concat_ws('_', tbcol4, tbcol5), substr(tbcol4, 1, 4)  from stb"
        )
        tdSql.query(f"select * from stb where tbcol not in (1,2,3,null);")
        tdSql.query(f"select * from stb where tbcol + 3 <> null;")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from stb where tbcol = 1 and tbcol2 = 1 and tbcol3 = 1 partition by tgcol interval(1d)"
        )
        tdSql.query(f"select _wstart, count(*) from tb1 session(ts, 1m)")
        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from stb where ts <= 1601481840000 and ts >= 1601481800000 partition by tgcol interval(1m) fill(value, 0,0,0,0,0)"
        )
        tdSql.query(f"select tbcol5 - tbcol3 from stb")

        tdSql.query(
            f"select spread( tbcol2 )/44, spread(tbcol2), 0.204545455 * 44 from stb;"
        )
        tdSql.query(
            f"select min(tbcol) * max(tbcol) /4, sum(tbcol2) * apercentile(tbcol2, 20), apercentile(tbcol2, 33) + 52/9 from stb;"
        )
        tdSql.query(f"select distinct(tbname), tgcol from stb;")
        tdSql.query(
            f"select sum(tbcol) from stb partition by tbname interval(1s) slimit 1 soffset 1;"
        )
        tdSql.query(
            f"select sum(tbcol) from stb partition by tbname interval(1s) slimit 2 soffset 4 limit 10 offset 1;"
        )

        tdLog.info(f"=============== step5: explain")
        tdSql.query(f"explain analyze select ts from stb where -2;")
        tdSql.query(f"explain analyze select ts from tb1;")
        tdSql.query(f"explain analyze select ts from stb order by ts;")
        tdSql.query(f"explain analyze select * from information_schema.ins_stables;")
        tdSql.query(f"explain analyze select count(*),sum(tbcol) from tb1;")
        tdSql.query(f"explain analyze select count(*),sum(tbcol) from stb;")
        tdSql.query(
            f"explain analyze select count(*),sum(tbcol) from stb group by tbcol;"
        )
        tdSql.query(f"explain analyze select * from information_schema.ins_stables;")
        tdSql.query(
            f"explain analyze verbose true select * from information_schema.ins_stables where db_name='db2';"
        )
        tdSql.query(
            f"explain analyze verbose true select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from stb where ts <= 1601481840000 and ts >= 1601481800000 partition by tgcol interval(1m) fill(value, 0,0,0,0,0)"
        )
        tdSql.query(
            f"explain select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), count(tbcol) from stb where ts <= 1601481840000 and ts >= 1601481800000 partition by tgcol interval(1m) fill(value, 0,0,0,0,0)"
        )

        tdLog.info(f"=============== step6: in cast")
        tdSql.query(f"select 1+1n;")
        tdSql.query(f"select cast(1 as timestamp)+1n;")
        tdSql.query(f"select cast(1 as timestamp)+1y;")
        tdSql.query(
            f"select * from tb1 where ts in ('2018-07-10 16:31:01', '2022-07-10 16:31:03', 1657441865000);"
        )
        tdSql.query(f"select * from tb1 where tbcol2 in (257);")
        tdSql.query(f"select * from tb1 where tbcol3 in (2, 257);")
        tdSql.query(
            f"select * from stb where ts in ('2018-07-10 16:31:01', '2022-07-10 16:31:03', 1657441865000);"
        )
        tdSql.query(f"select * from stb where tbcol2 in (257);")
        tdSql.query(f"select * from stb where tbcol3 in (2, 257);")

        tdLog.info(f"=============== check")

        tdLog.info(f"=============== restart")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"select avg(tbcol) as c from stb")

        tdSql.query(f"select avg(tbcol) as c from stb where ts <= 1601481840000")
        tdSql.query(
            f"select avg(tbcol) as c from stb where tgcol < 5 and ts <= 1601481840000"
        )
        tdSql.query(f"select avg(tbcol) as c from stb interval(1m)")
        tdSql.query(f"select avg(tbcol) as c from stb interval(1d)")
        tdSql.query(
            f"select avg(tbcol) as b from stb where ts <= 1601481840000 interval(1m)"
        )
        tdSql.query(f"select avg(tbcol) as c from stb group by tgcol")
        tdSql.query(
            f"select avg(tbcol) as b from stb where ts <= 1601481840000 partition by tgcol interval(1m)"
        )
        tdSql.query(f"show table distributed stb")
        tdSql.query(
            f"select count(tbcol) as b from stb where ts <= 1601481840000 partition by tgcol interval(1m)"
        )
        # sql select diff(tbcol) from stb where ts <= 1601481840000
        tdSql.query(f"select first(tbcol), last(tbcol) as c from stb group by tgcol")
        tdSql.query(
            f"select first(tbcol), last(tbcol) as b from stb where ts <= 1601481840000 and tbcol2 is null partition by tgcol interval(1m)"
        )
        tdSql.query(
            f"select first(tbcol), last(tbcol) as b from stb where ts <= 1601481840000 partition by tgcol interval(1m)"
        )
        tdSql.query(
            f"select count(tbcol), avg(tbcol), max(tbcol), min(tbcol), sum(tbcol), stddev(tbcol) from stb where ts <= 1601481840000 partition by tgcol interval(1m)"
        )
        tdSql.query(
            f"select last_row(tbcol), stddev(tbcol) from stb where tbcol > 5 and tbcol < 20 group by tgcol"
        )
