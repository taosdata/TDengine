from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncLastLru:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_last_lru(self):
        """Last Lru（内存不足、多 Vgroup、复杂查询）

        1. 创建内存不足的数据库
        2. 创建超级表、子表、写入数据
        3. 进行 first/count/last/tbname/排序/interval/group by 等的混合查询
        4. 创建内存充足的数据库
        5. 重复以上查询
        6. 创建 vroups=4 的数据库
        7. 重复以上查询

        Catalog:
            - Function:Selection

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/query/cache_last.sim

        """

        tdSql.execute(
            f"create database if not exists db1 cachemodel 'both' cachesize 10;"
        )
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 double, f2 binary(200)) tags(t1 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1);")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:01', 1.0, \"a\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:02', 2.0, \"b\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:04', 4.0, \"b\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:05', 5.0, \"b\");")
        tdSql.execute(f"create table tba2 using sta tags(2);")
        tdSql.execute(f"insert into tba2 values ('2022-04-26 15:15:01', 1.2, \"a\");")
        tdSql.execute(f"insert into tba2 values ('2022-04-26 15:15:02', 2.2, \"b\");")
        tdSql.execute(f"create table tba3 using sta tags(3);")
        tdSql.execute(f"insert into tba3 values ('2022-04-26 15:15:10', 1.3, \"a\");")
        tdSql.execute(f"insert into tba3 values ('2022-04-26 15:15:11', 2.3, \"b\");")
        tdSql.query(f"select count(*), last(*) from sta;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 8)
        tdSql.checkData(0, 1, "2022-04-26 15:15:11")
        tdSql.checkData(0, 2, 2.300000000)
        tdSql.checkData(0, 3, "b")

        tdSql.query(f"explain select count(*), last(*) from sta;")
        tdSql.checkData(
            0,
            0,
            "-> Merge (columns=4 width=226 input_order=unknown output_order=unknown mode=column)",
        )

        tdSql.query(f"explain select first(f1), last(*) from sta;")
        tdSql.checkData(
            0,
            0,
            "-> Merge (columns=4 width=226 input_order=unknown output_order=unknown mode=column)",
        )

        tdSql.query(f"select first(f1), last(*) from sta;")
        tdSql.checkRows(1)

        tdSql.query(f"select last_row(f1), last(f1) from sta;")
        tdSql.checkRows(1)

        tdSql.query(f"select count(*), last_row(f1), last(f1) from sta;")
        tdSql.checkRows(1)

        tdSql.query(f"explain select count(*), last_row(f1), last(f1) from sta;")
        tdSql.checkData(
            0,
            0,
            "-> Merge (columns=3 width=24 input_order=unknown output_order=unknown mode=column)",
        )

        tdSql.error(f"select count(*), last_row(f1), min(f1), f1 from sta;")
        tdSql.query(
            f"select count(*), last_row(f1), min(f1),tbname from sta partition by tbname;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"explain select count(*), last_row(f1), min(f1),tbname from sta partition by tbname;"
        )
        tdSql.checkData(0, 0, "-> Data Exchange 2:1 (width=296)")

        tdSql.query(f"explain select count(*), last_row(f1), min(f1) from sta;")
        tdSql.checkData(
            0,
            0,
            "-> Merge (columns=3 width=24 input_order=unknown output_order=unknown mode=column)",
        )

        tdSql.query(
            f"explain select count(*), last_row(f1), min(f1),tbname from sta group by tbname;"
        )
        tdSql.checkData(0, 0, "-> Data Exchange 2:1 (width=296)")

        tdSql.query(
            f"explain select count(*), last_row(f1), min(f1),t1 from sta partition by t1;"
        )
        tdSql.checkData(0, 0, "-> Aggregate (functions=4 width=28 input_order=desc )")

        tdSql.query(
            f"explain select count(*), last_row(f1), min(f1),t1 from sta group by t1;"
        )
        tdSql.checkData(0, 0, "-> Aggregate (functions=4 width=28 input_order=desc )")

        tdSql.query(
            f"explain select distinct count(*), last_row(f1), min(f1) from sta;"
        )
        tdSql.checkData(
            1,
            0,
            "   -> Merge (columns=3 width=24 input_order=unknown output_order=unknown mode=column)",
        )

        tdSql.query(
            f"explain select count(*), last_row(f1), min(f1) from sta interval(1s);"
        )
        tdSql.checkData(
            1,
            0,
            "   -> Merge (columns=4 width=122 input_order=asc output_order=asc mode=sort)",
        )

        tdSql.query(
            f"explain select distinct count(*), last_row(f1), min(f1) from tba1;"
        )
        tdSql.checkData(
            1,
            0,
            "   -> Merge (columns=3 width=24 input_order=unknown output_order=unknown mode=column)",
        )

        tdSql.query(f"select distinct count(*), last_row(f1), min(f1) from tba1;")
        tdSql.checkRows(1)

        tdLog.info(f"step 2-------------------------------")

        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test  cachemodel 'both';")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create table stb (ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )

        tdSql.execute(f"create table t1 using stb tags(1,1,1);")
        tdSql.execute(f"create table t2 using stb tags(2,2,2);")
        tdSql.execute(f"insert into t1 values('2024-06-05 11:00:00',1,2,3);")
        tdSql.execute(f"insert into t1 values('2024-06-05 12:00:00',2,2,3);")
        tdSql.execute(f"insert into t2 values('2024-06-05 13:00:00',3,2,3);")
        tdSql.execute(f"insert into t2 values('2024-06-05 14:00:00',4,2,3);")

        tdSql.query(f"select last(ts) ts1,ts from stb;")
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 1))

        tdSql.query(f"select last(ts) ts1,ts from stb group by tbname;")
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 1))

        tdSql.query(f"select last(ts) ts1,tbname, ts from stb;")
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 2))
        tdSql.checkData(0, 1, "t2")

        tdSql.query(
            f"select last(ts) ts1,tbname, ts from stb group by tbname order by 1 desc;"
        )
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 2))
        tdSql.checkData(0, 1, "t2")

        tdLog.info(f"step 3-------------------------------")

        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test1  cachemodel 'both' vgroups 4;")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create table stb (ts timestamp,a int COMPOSITE key,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute('alter local \'showFullCreateTableColumn\' \'1\'')
        tdSql.query(f"show create table stb")
        tdSql.checkData(
            0,
            1,
            "CREATE STABLE `stb` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `a` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium' COMPOSITE KEY, `b` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `c` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`ta` INT, `tb` INT, `tc` INT)",
        )

        tdSql.query(f"desc stb")
        tdSql.checkData(1, 3, "COMPOSITE KEY")

        tdSql.execute(f"create table aaat1 using stb tags(1,1,1);")
        tdSql.execute(f"create table bbbt2 using stb tags(2,2,2);")
        tdSql.execute(f"insert into aaat1 values('2024-06-05 11:00:00',1,2,3);")
        tdSql.execute(f"insert into aaat1 values('2024-06-05 12:00:00',2,2,3);")
        tdSql.execute(f"insert into bbbt2 values('2024-06-05 13:00:00',3,2,3);")
        tdSql.execute(f"insert into bbbt2 values('2024-06-05 14:00:00',4,2,3);")

        tdSql.query(f"select last(ts) ts1,ts from stb;")
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 1))
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")

        tdSql.query(f"select last(ts) ts1,ts from stb group by tbname order by 1 desc;")
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 1))
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")

        tdSql.query(f"select last(ts) ts1,tbname, ts from stb;")
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 2))
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")
        tdSql.checkData(0, 1, "bbbt2")

        tdSql.query(
            f"select last(ts) ts1,tbname, ts from stb group by tbname order by 1 desc;"
        )
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 2))
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")

        tdSql.checkData(0, 1, "bbbt2")
        tdLog.info(f"{tdSql.getData(0,1)}")

        tdLog.info(f"step 4-------------------------------")

        tdSql.query(f"select last(a) a,ts from stb;")
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(0, 1, "2024-06-05 14:00:00")

        tdSql.query(f"select last(a) a,ts from stb group by tbname order by 1 desc;")
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(0, 1, "2024-06-05 14:00:00")

        tdSql.query(f"select last(a) a,tbname, ts from stb;")
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(0, 1, "bbbt2")
        tdSql.checkData(0, 2, "2024-06-05 14:00:00")

        tdSql.query(
            f"select last(a) a,tbname, ts from stb group by tbname order by 1 desc;"
        )
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(0, 1, "bbbt2")
        tdSql.checkData(0, 2, "2024-06-05 14:00:00")

        tdLog.info(f"step 5-------------------------------")

        tdSql.query(f"select last(ts) ts1,a from stb;")
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")
        tdSql.checkData(0, 1, 4)

        tdSql.query(f"select last(ts) ts1,a from stb group by tbname order by 1 desc;")
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")
        tdSql.checkData(0, 1, 4)

        tdSql.query(f"select last(ts) ts1,tbname, a from stb;")
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")
        tdSql.checkData(0, 1, "bbbt2")
        tdSql.checkData(0, 2, 4)

        tdSql.query(
            f"select last(ts) ts1,tbname, a from stb group by tbname order by 1 desc;"
        )
        tdSql.checkData(0, 0, "2024-06-05 14:00:00")
        tdSql.checkData(0, 1, "bbbt2")
        tdSql.checkData(0, 2, 4)

        tdSql.query(f"select last(ts), last_row(ts) from stb;")
        tdSql.checkEqual(tdSql.getData(0, 0), tdSql.getData(0, 1))
