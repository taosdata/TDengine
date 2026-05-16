from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck, tdDnodes
import datetime

PRIMARY_COL = "ts"

INT_COL     = "c1"
BINT_COL    = "c2"
SINT_COL    = "c3"
TINT_COL    = "c4"
FLOAT_COL   = "c5"
DOUBLE_COL  = "c6"
BOOL_COL    = "c7"

BINARY_COL  = "c8"
NCHAR_COL   = "c9"
TS_COL      = "c10"

NUM_COL     = [ INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, ]
CHAR_COL    = [ BINARY_COL, NCHAR_COL, ]
BOOLEAN_COL = [ BOOL_COL, ]
TS_TYPE_COL = [ TS_COL, ]

class TestUnionBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_union_basic(self):
        """Operator union basic

        1. Union of projection queries
        2. Union of queries containing window and aggregate functions
        3. Union of system table queries
        4. Union of queries from databases with different precision levels
        5. Union of same/diff limit
        6. Union of three select clause
        7. Union of order by

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/parser/union.sim
            - 2025-8-20 Simon Guan Migrated from tsim/parser/union_sysinfo.sim
            - 2025-8-20 Simon Guan Migrated from tsim/query/unionall_as_table.sim
            - 2025-8-20 Simon Guan Migrated from tsim/query/union_precision.sim
            - 2025-12-21 Alex Duan Migrated from uncatalog/system-test/2-query/test_union1.py

        """

        self.Union()
        tdStream.dropAllStreamsAndDbs()
        self.UnionSysinfo()
        tdStream.dropAllStreamsAndDbs()
        self.UnionAllAsSystable()
        tdStream.dropAllStreamsAndDbs()
        self.UnionPrecision()
        tdStream.dropAllStreamsAndDbs()
        self.do_union()

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

    #
    # ------------------- test_union1.py ----------------
    #
    def __query_condition(self,tbname):
        query_condition = []
        for char_col in CHAR_COL:
            query_condition.extend(
                (
                    f"count( {tbname}.{char_col} )",
                    f"cast( {tbname}.{char_col} as nchar(3) )",
                )
            )

        for num_col in NUM_COL:
            query_condition.extend(
                (
                    f"log( {tbname}.{num_col},  {tbname}.{num_col})",
                )
            )

        query_condition.extend(
            (
                ''' "test12" ''',
                # 1010,
            )
        )

        return query_condition

    def __join_condition(self, tb_list, filter=PRIMARY_COL, INNER=False):
        table_reference = tb_list[0]
        join_condition = table_reference
        join = "inner join" if INNER else "join"
        for i in range(len(tb_list[1:])):
            join_condition += f" {join} {tb_list[i+1]} on {table_reference}.{filter}={tb_list[i+1]}.{filter}"

        return join_condition

    def __where_condition(self, col=None, tbname=None, query_conditon=None):
        if query_conditon and isinstance(query_conditon, str):
            if query_conditon.startswith("count"):
                query_conditon = query_conditon[6:-1]
            elif query_conditon.startswith("max"):
                query_conditon = query_conditon[4:-1]
            elif query_conditon.startswith("sum"):
                query_conditon = query_conditon[4:-1]
            elif query_conditon.startswith("min"):
                query_conditon = query_conditon[4:-1]

        if query_conditon:
            return f" where {query_conditon} is not null"
        if col in NUM_COL:
            return f" where abs( {tbname}.{col} ) >= 0"
        if col in CHAR_COL:
            return f" where lower( {tbname}.{col} ) like 'bina%' or lower( {tbname}.{col} ) like '_cha%' "
        if col in BOOLEAN_COL:
            return f" where {tbname}.{col} in (false, true)  "
        if col in TS_TYPE_COL or col in PRIMARY_COL:
            return f" where cast( {tbname}.{col} as binary(16) ) is not null "

        return ""

    def __group_condition(self, col, having = None):
        if isinstance(col, str):
            if col.startswith("count"):
                col = col[6:-1]
            elif col.startswith("max"):
                col = col[4:-1]
            elif col.startswith("sum"):
                col = col[4:-1]
            elif col.startswith("min"):
                col = col[4:-1]
        return f" group by {col} having {having}" if having else f" group by {col} "

    def __single_sql(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0] != from_clause.split(".")[0]:
            return
        return f"select {select_clause} from {from_clause} {where_condition} {group_condition}"

    @property
    def __join_tblist(self):
        return [
            ["ct1", "ct2"],
            # ["ct1", "ct2", "ct4"],
            # ["ct1", "ct2", "t1"],
            # ["ct1", "ct4", "t1"],
            # ["ct2", "ct4", "t1"],
            # ["ct1", "ct2", "ct4", "t1"],
        ]

    @property
    def __tb_liast(self):
        return [
            "t1",
            "stb1",
        ]

    def sql_list(self):
        sqls = []
        __join_tblist = self.__join_tblist
        for join_tblist in __join_tblist:
            for join_tb in join_tblist:
                select_claus_list = self.__query_condition(join_tb)
                for select_claus in select_claus_list:
                    group_claus = self.__group_condition( col=select_claus)
                    where_claus = self.__where_condition(query_conditon=select_claus)
                    having_claus = self.__group_condition( col=select_claus, having=f"{select_claus} is not null")
                    sqls.extend(
                        (
                            self.__single_sql(select_claus, self.__join_condition(join_tblist, INNER=True), where_claus, having_claus),
                        )
                    )
        __no_join_tblist = self.__tb_liast
        for tb in __no_join_tblist:
                select_claus_list = self.__query_condition(tb)
                for select_claus in select_claus_list:
                    group_claus = self.__group_condition(col=select_claus)
                    where_claus = self.__where_condition(query_conditon=select_claus)
                    having_claus = self.__group_condition(col=select_claus, having=f"{select_claus} is not null")
                    sqls.extend(
                        (
                            self.__single_sql(select_claus, tb, where_claus, having_claus),
                        )
                    )

        # return filter(None, sqls)
        return list(filter(None, sqls))

    def __get_type(self, col):
        if tdSql.cursor.istype(col, "BOOL"):
            return "BOOL"
        if tdSql.cursor.istype(col, "INT"):
            return "INT"
        if tdSql.cursor.istype(col, "BIGINT"):
            return "BIGINT"
        if tdSql.cursor.istype(col, "TINYINT"):
            return "TINYINT"
        if tdSql.cursor.istype(col, "SMALLINT"):
            return "SMALLINT"
        if tdSql.cursor.istype(col, "FLOAT"):
            return "FLOAT"
        if tdSql.cursor.istype(col, "DOUBLE"):
            return "DOUBLE"
        if tdSql.cursor.istype(col, "BINARY"):
            return "BINARY"
        if tdSql.cursor.istype(col, "NCHAR"):
            return "NCHAR"
        if tdSql.cursor.istype(col, "TIMESTAMP"):
            return "TIMESTAMP"
        if tdSql.cursor.istype(col, "JSON"):
            return "JSON"
        if tdSql.cursor.istype(col, "TINYINT UNSIGNED"):
            return "TINYINT UNSIGNED"
        if tdSql.cursor.istype(col, "SMALLINT UNSIGNED"):
            return "SMALLINT UNSIGNED"
        if tdSql.cursor.istype(col, "INT UNSIGNED"):
            return "INT UNSIGNED"
        if tdSql.cursor.istype(col, "BIGINT UNSIGNED"):
            return "BIGINT UNSIGNED"

    def union_check(self):
        sqls = self.sql_list()
        for i in range(len(sqls)):
            tdSql.query(sqls[i])
            res1_type = self.__get_type(0)
            # if i % 5 == 0:
            #         tdLog.success(f"{i} : sql is already executing!")
            for j in range(len(sqls[i:])):
                tdSql.query(sqls[j+i])
                order_union_type = False
                rev_order_type = False
                all_union_type = False
                res2_type =  self.__get_type(0)

                if res2_type == res1_type:
                    all_union_type = True
                elif res1_type in ( "BIGINT" , "NCHAR" ) and res2_type in ("BIGINT" , "NCHAR"):
                    all_union_type = True
                elif res1_type in ("BIGINT", "NCHAR"):
                    order_union_type = True
                elif res2_type in ("BIGINT", "NCHAR"):
                    rev_order_type = True
                elif res1_type == "TIMESAMP" and res2_type not in ("BINARY", "NCHAR"):
                    order_union_type = True
                elif res2_type == "TIMESAMP" and res1_type not in ("BINARY", "NCHAR"):
                    rev_order_type = True
                elif res1_type == "BINARY" and res2_type != "NCHAR":
                    order_union_type = True
                elif res2_type == "BINARY" and res1_type != "NCHAR":
                    rev_order_type = True

                if all_union_type:
                    tdSql.execute(f"{sqls[i]} union {sqls[j+i]}")
                    tdSql.execute(f"{sqls[j+i]} union all {sqls[i]}")
                elif order_union_type:
                    tdSql.execute(f"{sqls[i]} union all {sqls[j+i]}")
                elif rev_order_type:
                    tdSql.execute(f"{sqls[j+i]} union {sqls[i]}")
                else:
                    tdSql.error(f"{sqls[i]} union {sqls[j+i]}")

    def __test_error(self):

        tdSql.error( "show tables union show tables" )
        tdSql.error( "create table errtb1 union all create table errtb2" )
        tdSql.error( "drop table ct1 union all drop table ct3" )
        tdSql.error( "select c1 from ct1 union all drop table ct3" )
        tdSql.error( "select c1 from ct1 union all '' " )
        tdSql.error( " '' union all select c1 from ct1 " )
        # tdSql.error( "select c1 from ct1 union select c1 from ct2 union select c1 from ct4 ")

    def check_select_from_union_all(self):
        tdSql.query('select c8, ts from ((select ts, c8,c1 from stb1 order by c1) union all select ts, c8, c1 from stb1 limit 15)')
        tdSql.checkRows(15)
        tdSql.query('select c8, ts from ((select ts, c8,c1 from stb1 order by c1) union all (select ts, c8, c1 from stb1 order by c8 limit 10) limit 15)')
        tdSql.checkRows(15)
        tdSql.query('select ts, c1 from ((select ts, c8,c1 from stb1 order by c1) union all (select ts, c8, c1 from stb1 order by c8 limit 10) limit 15)')
        tdSql.checkRows(15)
        tdSql.query('select ts, c1, c8 from ((select ts, c8,c1 from stb1 order by c1) union all (select ts, c8, c1 from stb1 order by c8 limit 10) limit 15)')
        tdSql.checkRows(15)
        tdSql.query('select ts, c8, c1, 123 from ((select ts, c8,c1 from stb1 order by c1) union all (select ts, c8, c1 from stb1 order by c8 limit 10) limit 15)')
        tdSql.checkRows(15)

    def all_test(self):
        self.__test_error()
        self.union_check()
        self.check_select_from_union_all()

    def __create_tb(self):

        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table stb1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
            ) tags (t1 int)
            '''
        create_ntb_sql = f'''create table t1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table ct{i+1} using stb1 tags ( {i+1} )')
            { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2}

    def __insert_data(self, rows):
        now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        for i in range(rows):
            tdSql.execute(
                f"insert into ct1 values ( { now_time - i * 1000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into ct4 values ( { now_time - i * 7776000000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into ct2 values ( { now_time - i * 7776000000 }, {-i},  {-11111 * i}, {-111 * i % 32767 }, {-11 * i % 127}, {-1.11*i}, {-1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
        tdSql.execute(
            f'''insert into ct1 values
            ( { now_time - rows * 5 }, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar_测试_0', { now_time + 8 } )
            ( { now_time + 10000 }, { rows }, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar_测试_9', { now_time + 9 } )
            '''
        )

        tdSql.execute(
            f'''insert into ct4 values
            ( { now_time - rows * 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3888000000 + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time +  7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                { now_time + 5184000000}, {pow(2,31)-pow(2,15)}, {pow(2,63)-pow(2,30)}, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000}
                )
            (
                { now_time + 2592000000 }, {pow(2,31)-pow(2,16)}, {pow(2,63)-pow(2,31)}, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000}
                )
            '''
        )

        tdSql.execute(
            f'''insert into ct2 values
            ( { now_time - rows * 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3888000000 + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time + 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                { now_time + 5184000000 }, { -1 * pow(2,31) + pow(2,15) }, { -1 * pow(2,63) + pow(2,30) }, -32766, -126,
                { -1 * 3.2 * pow(10,38) }, { -1.2 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000 }
                )
            (
                { now_time + 2592000000 }, { -1 * pow(2,31) + pow(2,16) }, { -1 * pow(2,63) + pow(2,31) }, -32767, -127,
                { - 3.3 * pow(10,38) }, { -1.3 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000 }
                )
            '''
        )

        for i in range(rows):
            insert_data = f'''insert into t1 values
                ( { now_time - i * 3600000 }, {i}, {i * 11111}, { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2},
                "binary_{i}", "nchar_测试_{i}", { now_time - 1000 * i } )
                '''
            tdSql.execute(insert_data)
        tdSql.execute(
            f'''insert into t1 values
            ( { now_time + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - (( rows // 2 ) * 60 + 30) * 60000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3600000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time + 7200000 }, { pow(2,31) - pow(2,15) }, { pow(2,63) - pow(2,30) }, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 },
                "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000 }
                )
            (
                { now_time + 3600000 } , { pow(2,31) - pow(2,16) }, { pow(2,63) - pow(2,31) }, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 },
                "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000 }
                )
            '''
        )

    def do_union(self):
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb()

        tdLog.printNoPrefix("==========step2:insert data")
        self.rows = 10
        self.__insert_data(self.rows)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test()

        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test()
