from new_test_framework.utils import tdLog, tdSql, tdStream, tdCom, tdDnodes, etool
import datetime
import random
import re

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

ALL_COL = [ INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, BOOL_COL, BINARY_COL, NCHAR_COL, TS_COL ]
DBNAME = "db"

class TestExplain:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
    #
    # ------------------- sim ----------------
    #
    def do_explain_basic(self):
        self.Explain()
        tdStream.dropAllStreamsAndDbs()
        self.ExplainTsOrder()
        tdStream.dropAllStreamsAndDbs()
        
    def Explain(self):
        tdLog.info(f'======== step1')
        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 3 cachesize 10 cachemodel 'both';")
        tdSql.execute(f"use db1;")
        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.execute(f"create stable st1 (ts timestamp, f1 int, f2 binary(200)) tags(t1 int);")
        tdSql.execute(f"create stable st2 (ts timestamp, f1 int, f2 binary(200)) tags(t1 int);")
        tdSql.execute(f"create table tb1 using st1 tags(1);")
        tdSql.execute(f'insert into tb1 values (now, 1, "Hash Join  (cost=230.47..713.98 rows=101 width=488) (actual time=0.711..7.427 rows=100 loops=1)");')

        tdSql.execute(f"create table tb2 using st1 tags(2);")
        tdSql.execute(f'insert into tb2 values (now, 2, "Seq Scan on tenk2 t2  (cost=0.00..445.00 rows=10000 width=244) (actual time=0.007..2.583 rows=10000 loops=1)");')
        tdSql.execute(f"create table tb3 using st1 tags(3);")
        tdSql.execute(f'insert into tb3 values (now, 3, "Hash  (cost=229.20..229.20 rows=101 width=244) (actual time=0.659..0.659 rows=100 loops=1)");')
        tdSql.execute(f"create table tb4 using st1 tags(4);")
        tdSql.execute(f'insert into tb4 values (now, 4, "Bitmap Heap Scan on tenk1 t1  (cost=5.07..229.20 rows=101 width=244) (actual time=0.080..0.526 rows=100 loops=1)");')

        #sql create table tb1 using st2 tags(1);
        #sql insert into tb1 values (now, 1, "Hash Join  (cost=230.47..713.98 rows=101 width=488) (actual time=0.711..7.427 rows=100 loops=1)");

        #sql create table tb2 using st2 tags(2);
        #sql insert into tb2 values (now, 2, "Seq Scan on tenk2 t2  (cost=0.00..445.00 rows=10000 width=244) (actual time=0.007..2.583 rows=10000 loops=1)");
        #sql create table tb3 using st2 tags(3);
        #sql insert into tb3 values (now, 3, "Hash  (cost=229.20..229.20 rows=101 width=244) (actual time=0.659..0.659 rows=100 loops=1)");
        #sql create table tb4 using st2 tags(4);
        #sql insert into tb4 values (now, 4, "Bitmap Heap Scan on tenk1 t1  (cost=5.07..229.20 rows=101 width=244) (actual time=0.080..0.526 rows=100 loops=1)");

        # for explain insert into select
        tdSql.execute(f"create table t1 (ts timestamp, f1 int, f2 binary(200), t1 int);")

        tdLog.info(f'======== step2')
        tdSql.query(f"explain select * from st1 where -2;")
        tdSql.query(f"explain insert into t1 select * from st1 where -2;")
        tdSql.query(f"explain select ts from tb1;")
        tdSql.query(f"explain insert into t1(ts) select ts from tb1;")
        tdSql.query(f"explain select * from st1;")
        tdSql.query(f"explain insert into t1 select * from st1;")
        tdSql.query(f"explain select * from st1 order by ts;")
        tdSql.query(f"explain insert into t1 select * from st1 order by ts;")
        tdSql.query(f"explain select * from information_schema.ins_stables;")
        tdSql.query(f"explain select count(*),sum(f1) from tb1;")
        tdSql.query(f"explain select count(*),sum(f1) from st1;")
        tdSql.query(f"explain select count(*),sum(f1) from st1 group by f1;")
        #sql explain select count(f1) from tb1 interval(10s, 2s) sliding(3s) fill(prev);
        tdSql.query(f"explain insert into t1(ts, t1) select _wstart, count(*) from st1 interval(10s);")
        tdSql.query(f"explain select TIMETRUNCATE('2026-01-05 15:30:18',1s) from tb1 SESSION(ts,18d);")
        tdSql.query(f"explain select TIMETRUNCATE('2026-01-05 15:30:18',1s) from tb1 interval(1d);")

        tdLog.info(f'======== step3')
        tdSql.query(f"explain verbose true select * from st1 where -2;")
        tdSql.query(f"explain verbose true insert into t1 select * from st1 where -2;")
        tdSql.query(f"explain verbose true select ts from tb1 where f1 > 0;")
        tdSql.query(f"explain verbose true insert into t1(ts) select ts from tb1 where f1 > 0;")
        tdSql.query(f"explain verbose true select * from st1 where f1 > 0 and ts > '2020-10-31 00:00:00' and ts < '2021-10-31 00:00:00';")
        tdSql.query(f"explain verbose true insert into t1 select * from st1 where f1 > 0 and ts > '2020-10-31 00:00:00' and ts < '2021-10-31 00:00:00';")
        tdSql.query(f"explain verbose true select count(*) from st1 partition by tbname slimit 1 soffset 2 limit 2 offset 1;")
        tdSql.query(f"explain verbose true select * from information_schema.ins_stables where db_name='db2';")
        tdSql.query(f"explain verbose true select st1.f1 from st1 join st2 on st1.ts=st2.ts and st1.f1 > 0;")
        tdSql.query(f"explain verbose true insert into t1(ts) select st1.f1 from st1 join st2 on st1.ts=st2.ts and st1.f1 > 0;")
        tdSql.query(f"explain verbose true insert into t1(ts, t1) select _wstart, count(*) from st1 interval(10s);")
        tdSql.query(f"explain verbose true select distinct tbname, table_name from information_schema.ins_tables;")
        tdSql.query(f"explain verbose true select diff(f1) as f11 from tb1 order by f11;")
        tdSql.query(f"explain verbose true select count(*) from st1 where ts > now - 3m and ts < now interval(10s) fill(linear);")
        tdSql.query(f"explain verbose true select count(*) from st1 partition by tbname;")
        tdSql.query(f"explain verbose true select count(*) from information_schema.ins_tables group by stable_name;")
        tdSql.query(f"explain verbose true select last(*) from st1;")
        tdSql.query(f"explain verbose true select last_row(*) from st1;")
        tdSql.query(f"explain verbose true select interp(f1) from tb1 where ts > now - 3m and ts < now range(now-3m,now) every(1m) fill(prev);")
        tdSql.query(f"explain verbose true select _wstart, _wend, count(*) from tb1 EVENT_WINDOW start with f1 > 0 end with f1 < 10;")
        tdSql.query(f"explain verbose true select TIMETRUNCATE('2026-01-05 15:30:18',1s) from tb1 SESSION(ts,18d);")
        tdSql.query(f"explain verbose true select TIMETRUNCATE('2026-01-05 15:30:18',1s) from tb1 interval(1d);")

        tdLog.info(f'======== step4')
        tdSql.query(f"explain analyze select ts from st1 where -2;")
        tdSql.query(f"explain analyze insert into t1(ts) select ts from st1 where -2;")
        tdSql.query(f"explain analyze select ts from tb1;")
        tdSql.query(f"explain analyze insert into t1(ts) select ts from tb1;")
        tdSql.query(f"explain analyze select ts from st1;")
        tdSql.query(f"explain analyze insert into t1(ts) select ts from st1;")
        tdSql.query(f"explain analyze select ts from st1 order by ts;")
        tdSql.query(f"explain analyze insert into t1(ts) select ts from st1 order by ts;")
        tdSql.query(f"explain analyze select * from information_schema.ins_stables;")
        tdSql.query(f"explain analyze select count(*),sum(f1) from tb1;")
        tdSql.query(f"explain analyze select count(*),sum(f1) from st1;")
        tdSql.query(f"explain analyze select count(*),sum(f1) from st1 group by f1;")
        tdSql.query(f"explain analyze insert into t1(ts, t1) select _wstart, count(*) from st1 interval(10s);")
        tdSql.query(f"explain analyze select TIMETRUNCATE('2026-01-05 15:30:18',1s) from tb1 SESSION(ts,18d);")
        tdSql.query(f"explain analyze select TIMETRUNCATE('2026-01-05 15:30:18',1s) from tb1 interval(1d);")

        tdLog.info(f'======== step5')
        tdSql.query(f"explain analyze verbose true select ts from st1 where -2;")
        tdSql.query(f"explain analyze verbose true insert into t1(ts) select ts from st1 where -2;")
        tdSql.query(f"explain analyze verbose true select ts from tb1;")
        tdSql.query(f"explain analyze verbose true insert into t1(ts) select ts from tb1;")
        tdSql.query(f"explain analyze verbose true select ts from st1;")
        tdSql.query(f"explain analyze verbose true insert into t1(ts) select ts from st1;")
        tdSql.query(f"explain analyze verbose true select ts from st1 order by ts;")
        tdSql.query(f"explain analyze verbose true insert into t1(ts) select ts from st1 order by ts;")
        tdSql.query(f"explain analyze verbose true select * from information_schema.ins_stables;")
        tdSql.query(f"explain analyze verbose true select count(*),sum(f1) from tb1;")
        tdSql.query(f"explain analyze verbose true select count(*),sum(f1) from st1;")
        tdSql.query(f"explain analyze verbose true select count(*),sum(f1) from st1 group by f1;")
        #sql explain analyze verbose true select count(f1) from tb1 interval(10s, 2s) sliding(3s) fill(prev);
        tdSql.query(f"explain analyze verbose true select ts from tb1 where f1 > 0;")
        tdSql.query(f"explain analyze verbose true select f1 from st1 where f1 > 0 and ts > '2020-10-31 00:00:00' and ts < '2021-10-31 00:00:00';")
        tdSql.query(f"explain analyze verbose true select * from information_schema.ins_stables where db_name='db2';")
        tdSql.query(f"explain analyze verbose true select * from (select min(f1),count(*) a from st1 where f1 > 0) where a < 0;")
        tdSql.query(f"explain analyze verbose true select count(f1) from st1 group by tbname;")
        tdSql.query(f"explain analyze verbose true select st1.f1 from st1 join st2 on st1.ts=st2.ts and st1.f1 > 0;")
        tdSql.query(f"explain analyze verbose true select diff(f1) as f11 from tb1 order by f11;")
        tdSql.query(f"explain analyze verbose true select count(*) from st1 where ts > now - 3m and ts < now interval(10s) fill(linear);")
        tdSql.query(f"explain analyze verbose true select count(*) from information_schema.ins_tables group by stable_name;")
        tdSql.query(f"explain analyze verbose true select last(*) from st1;")
        tdSql.query(f"explain analyze verbose true select last_row(*) from st1;")
        tdSql.query(f"explain analyze verbose true select interp(f1) from tb1 where ts > now - 3m and ts < now range(now-3m,now) every(1m) fill(prev);")
        tdSql.query(f"explain analyze verbose true select _wstart, _wend, count(*) from tb1 EVENT_WINDOW start with f1 > 0 end with f1 < 10;")
        tdSql.query(f"explain analyze verbose true select TIMETRUNCATE('2026-01-05 15:30:18',1s) from tb1 SESSION(ts,18d);")
        tdSql.query(f"explain analyze verbose true select TIMETRUNCATE('2026-01-05 15:30:18',1s) from tb1 interval(1d);")

        #not pass case
        #sql explain verbose true select count(*),sum(f1) as aa from tb1 where (f1 > 0 or f1 < -1) and ts > '2020-10-31 00:00:00' and ts < '2021-10-31 00:00:00' order by aa;
        #sql explain verbose true select * from st1 where (f1 > 0 or f1 < -1) and ts > '2020-10-31 00:00:00' and ts < '2021-10-31 00:00:00' order by ts;
        #sql explain verbose true select count(*),sum(f1) from st1 where (f1 > 0 or f1 < -1) and ts > '2020-10-31 00:00:00' and ts < '2021-10-31 00:00:00' order by ts;
        #sql explain verbose true select count(f1) from tb1 where (f1 > 0 or f1 < -1) and ts > '2020-10-31 00:00:00' and ts < '2021-10-31 00:00:00' interval(10s, 2s) sliding(3s) order by ts;
        #sql explain verbose true select min(f1) from st1 where (f1 > 0 or f1 < -1) and ts > '2020-10-31 00:00:00' and ts < '2021-10-31 00:00:00' interval(1m, 2a) sliding(30s)  fill(linear) order by ts;
        #sql explain select max(f1) from tb1 SESSION(ts, 1s);
        #sql explain select max(f1) from st1 SESSION(ts, 1s);
        #sql explain select * from tb1, tb2 where tb1.ts=tb2.ts;
        #sql explain select * from st1, st2 where tb1.ts=tb2.ts;
        #sql explain analyze verbose true select sum(a+ str(b)) from (select _rowts, min(f1) b,count(*) a from st1 where f1 > 0 interval(1a)) where a < 0 interval(1s);
        #sql explain select min(f1) from st1 interval(1m, 2a) sliding(30s);
        #sql explain verbose true select count(*),sum(f1) from st1 where f1 > 0 and ts > '2021-10-31 00:00:00' group by f1 having sum(f1) > 0;
        #sql explain analyze select min(f1) from st1 interval(3m, 2a) sliding(1m);
        #sql explain analyze select count(f1) from tb1 interval(10s, 2s) sliding(3s) fill(prev);
        #sql explain analyze verbose true select count(*),sum(f1) from st1 where f1 > 0 and ts > '2021-10-31 00:00:00' group by f1 having sum(f1) > 0;
        #sql explain analyze verbose true select min(f1) from st1 interval(3m, 2a) sliding(1m);


    def ExplainTsOrder(self):
        tdSql.execute(f"create database test")
        tdSql.execute(f"use test")
        tdSql.execute(
            f"CREATE STABLE `meters` (`ts` TIMESTAMP, `c2` INT) TAGS (`cc` VARCHAR(3), `cc2` VARCHAR(3))"
        )

        tdSql.execute(
            f'insert into d1 using meters (cc) tags("MY") values("2022-05-15 00:01:08.000 ",234)'
        )
        tdSql.execute(
            f'insert into d1 using meters (cc) tags("MY") values("2022-05-16 00:01:08.000 ",136)'
        )
        tdSql.execute(
            f'insert into d1 using meters (cc) tags("MY") values("2022-05-17 00:01:08.000 ", 59)'
        )
        tdSql.execute(
            f'insert into d1 using meters (cc) tags("MY") values("2022-05-18 00:01:08.000 ", 58)'
        )
        tdSql.execute(
            f'insert into d1 using meters (cc) tags("MY") values("2022-05-19 00:01:08.000 ",243)'
        )
        tdSql.execute(
            f'insert into d1 using meters (cc) tags("MY") values("2022-05-20 00:01:08.000 ",120)'
        )
        tdSql.execute(
            f'insert into d1 using meters (cc) tags("MY") values("2022-05-21 00:01:08.000 ", 11)'
        )
        tdSql.execute(
            f'insert into d1 using meters (cc) tags("MY") values("2022-05-22 00:01:08.000 ",196)'
        )
        tdSql.execute(
            f'insert into d1 using meters (cc) tags("MY") values("2022-05-23 00:01:08.000 ",116)'
        )
        tdSql.execute(
            f'insert into d1 using meters (cc) tags("MY") values("2022-05-24 00:01:08.000 ",210)'
        )

        tdSql.execute(
            f'insert into d2 using meters (cc) tags("HT") values("2022-05-15 00:01:08.000", 234)'
        )
        tdSql.execute(
            f'insert into d2 using meters (cc) tags("HT") values("2022-05-16 00:01:08.000", 136)'
        )
        tdSql.execute(
            f'insert into d2 using meters (cc) tags("HT") values("2022-05-17 00:01:08.000",  59)'
        )
        tdSql.execute(
            f'insert into d2 using meters (cc) tags("HT") values("2022-05-18 00:01:08.000",  58)'
        )
        tdSql.execute(
            f'insert into d2 using meters (cc) tags("HT") values("2022-05-19 00:01:08.000", 243)'
        )
        tdSql.execute(
            f'insert into d2 using meters (cc) tags("HT") values("2022-05-20 00:01:08.000", 120)'
        )
        tdSql.execute(
            f'insert into d2 using meters (cc) tags("HT") values("2022-05-21 00:01:08.000",  11)'
        )
        tdSql.execute(
            f'insert into d2 using meters (cc) tags("HT") values("2022-05-22 00:01:08.000", 196)'
        )
        tdSql.execute(
            f'insert into d2 using meters (cc) tags("HT") values("2022-05-23 00:01:08.000", 116)'
        )
        tdSql.execute(
            f'insert into d2 using meters (cc) tags("HT") values("2022-05-24 00:01:08.000", 210)'
        )

        testCase = "test_explain"
        tdLog.info(f"test case : {testCase}.")
        self.sqlFile = etool.curFile(__file__, f"t/test_explain.sql")
        self.ansFile = etool.curFile(__file__, f"r/test_explain.result")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

        #resultfile = tdCom.generate_query_result(
        #    "cases/09-DataQuerying/15-Explain/t/test_explain.sql", "test_explain"
        #)
        #tdLog.info(f"resultfile: {resultfile}")
        #tdCom.compare_result_files(
        #    resultfile, "cases/09-DataQuerying/15-Explain/r/test_explain.result"
        #)
        tdLog.printNoPrefix("do explain basic ...................... [passed]")
        
    #
    # ------------------- test_TD_20582.py ----------------
    #
    def prepare_datas(self, dbname="db"):

        tdSql.execute(
            f''' CREATE TABLE ac_stb (TS TIMESTAMP, C1 INT, C2 BIGINT, C3 FLOAT, C4 DOUBLE, C5 BINARY(10), C6 BOOL, 
            C7 SMALLINT, C8 TINYINT, C9 NCHAR(10)) TAGS (T1 INT);
            '''            
        )
        
        tdSql.execute(
            f''' insert into ctb0 using ac_stb tags (1) values ( 1537146001000 , 1,1,1,1,'bin',1,1,1,'________') 
            ( 1537146002000 , 2,2,2,2,'binar', 1,1,1,'nchar');
            '''       
        ) 

        tdSql.execute(
            f'''  insert into ntb0 using ac_stb tags (-1) values ( 1537146001000 , 1,1,1,1,'bin',1,1,1,'________') 
            ( 1537146002000 , 2,2,2,2,'binar', 1,1,1,'nchar');
            '''       
        ) 

        tdSql.execute(
            f'''  insert into ntb0 using ac_stb tags (2) values ( 1537146003000 , 1,1,1,1,'bin',1,1,1,'________') 
            ( 1537146004000 , 2,2,2,2,'binar', 1,1,1,'nchar');
            '''       
        ) 

        tdSql.execute(
            f''' insert into ctb6 using ac_stb tags(1) values ( 1537146000000 , 1, 1, 1, 1, 'bin1', 1, 1, 1, '________1') 
            ctb6 using ac_stb tags(2) values ( 1537146000000 , 2, 2, 2, 2, 'bin2', 2, 2, 2, '________2') 
            ctb6 using ac_stb tags(3) values ( 1537146000000 , 3, 3, 3, 3, 'bin3', 3, 3, 3, '________3')
            '''       
        )
    

    def check_result(self, dbname="db"):
        tdSql.query("select c1,c1,c2,c3,c4,c5,c7,c8,c9 from ac_stb")
        tdSql.checkRows(7)

        tdSql.query("select t1, count(*), first(c9) from ac_stb partition by t1 order by t1 asc slimit 3")
        tdSql.checkRows(2)

        # TD-20582
        tdSql.query("explain analyze verbose true select count(*) from ac_stb where T1=1")
        tdSql.checkRows(22)

        # TD-20581
        tdSql.execute("insert into ntb0 select * from ntb0")
        tdSql.query("select * from ntb0")
        tdSql.checkRows(4)

        return
        # basic query

    def do_td_20582(self):

        # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()

        tdLog.printNoPrefix("========== step1: create table ==============")

        self.prepare_datas()

        tdLog.printNoPrefix("========== step2: check results ==============")

        self.check_result()

        tdLog.printNoPrefix("do TD-20582 ........................... [passed]")

    
    #
    # ------------------- 3 ----------------
    #
    def __query_condition(self,tbname):
        query_condition = [f"{tbname}.{col}" for col in ALL_COL]
        for num_col in NUM_COL:
            query_condition.extend(
                (
                    f"abs( {tbname}.{num_col} )",
                    f"acos( {tbname}.{num_col} )",
                    f"asin( {tbname}.{num_col} )",
                    f"atan( {tbname}.{num_col} )",
                    f"avg( {tbname}.{num_col} )",
                    f"ceil( {tbname}.{num_col} )",
                    f"cos( {tbname}.{num_col} )",
                    f"count( {tbname}.{num_col} )",
                    f"floor( {tbname}.{num_col} )",
                    f"log( {tbname}.{num_col},  {tbname}.{num_col})",
                    f"max( {tbname}.{num_col} )",
                    f"min( {tbname}.{num_col} )",
                    f"pow( {tbname}.{num_col}, 2)",
                    f"round( {tbname}.{num_col} )",
                    f"sum( {tbname}.{num_col} )",
                    f"sin( {tbname}.{num_col} )",
                    f"sqrt( {tbname}.{num_col} )",
                    f"tan( {tbname}.{num_col} )",
                    f"cast( {tbname}.{num_col} as timestamp)",
                )
            )
            query_condition.extend((f"{num_col} + {any_col}" for any_col in ALL_COL))
        for char_col in CHAR_COL:
            query_condition.extend(
                (
                    f"sum(cast({tbname}.{char_col} as bigint ))",
                    f"max(cast({tbname}.{char_col} as bigint ))",
                    f"min(cast({tbname}.{char_col} as bigint ))",
                    f"avg(cast({tbname}.{char_col} as bigint ))",
                )
            )
        query_condition.extend(
            (
                1010.1,
                ''' "test1234!@#$%^&*():'><?/.,][}{" ''',
                "null"
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
            elif query_conditon.startswith("avg"):
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
            elif col.startswith("avg"):
                col = col[4:-1]
        return f" group by {col} having {having}" if having else f" group by {col} "

    def __single_sql(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_verbose_true(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain verbose true select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_verbose_false(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain verbose false select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_analyze(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain analyze select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_analyze_verbose_true(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain analyze verbose true select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_analyze_verbose_false(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain analyze verbose false select {select_clause} from {from_clause} {where_condition} {group_condition}"

    # reason for remove ratio: not implemented in the code
    # def __single_sql_analyze_ratio(self, select_clause, from_clause, where_condition="", group_condition=""):
    #     ratio = random.uniform(0.001,1)
    #     if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
    #         return
    #     return f"explain analyze ratio {ratio} select {select_clause} from {from_clause} {where_condition} {group_condition}"

    # def __single_sql_analyze_ratio_verbose_true(self, select_clause, from_clause, where_condition="", group_condition=""):
    #     ratio = random.uniform(0.001,1)
    #     if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
    #         return
    #     return f"explain analyze ratio {ratio} verbose true select {select_clause} from {from_clause} {where_condition} {group_condition}"

    # def __single_sql_analyze_ratio_verbose_false(self, select_clause, from_clause, where_condition="", group_condition=""):
    #     ratio = random.uniform(0.001,1)
    #     if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
    #         return
    #     return f"explain analyze ratio {ratio} verbose false select {select_clause} from {from_clause} {where_condition} {group_condition}"
                    
    @property
    def __tb_list(self, dbname=DBNAME):
        return [
            f"{dbname}.ct1",
            f"{dbname}.ct4",
            f"{dbname}.t1",
            f"{dbname}.ct2",
            f"{dbname}.stb1",
        ]

    def sql_list(self):
        sqls = []
        __no_join_tblist = self.__tb_list
        for tb in __no_join_tblist:
            tbname = tb.split(".")[-1]
            select_claus_list = self.__query_condition(tbname)
            for select_claus in select_claus_list:
                group_claus = self.__group_condition(col=select_claus)
                where_claus = self.__where_condition(query_conditon=select_claus)
                having_claus = self.__group_condition(col=select_claus, having=f"{select_claus} is not null")
                sqls.extend(
                    (
                        self.__single_sql(select_claus, tb, where_claus, having_claus),
                        self.__single_sql(select_claus, tb,),
                        self.__single_sql(select_claus, tb, where_condition=where_claus),
                        self.__single_sql(select_claus, tb, group_condition=group_claus),
                        self.__single_sql_verbose_true(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_verbose_true(select_claus, tb,),
                        self.__single_sql_verbose_true(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_verbose_true(select_claus, tb, group_condition=group_claus),
                        self.__single_sql_verbose_false(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_verbose_false(select_claus, tb,),
                        self.__single_sql_verbose_false(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_verbose_false(select_claus, tb, group_condition=group_claus),
                        
                        # reason for remove ratio: not implemented in the code
                        # self.__single_sql_ratio(select_claus, tb, where_claus, having_claus),
                        # self.__single_sql_ratio(select_claus, tb,),
                        # self.__single_sql_ratio(select_claus, tb, where_condition=where_claus),
                        # self.__single_sql_ratio(select_claus, tb, group_condition=group_claus),
                        # self.__single_sql_ratio_verbose_true(select_claus, tb, where_claus, having_claus),
                        # self.__single_sql_ratio_verbose_true(select_claus, tb,),
                        # self.__single_sql_ratio_verbose_true(select_claus, tb, where_condition=where_claus),
                        # self.__single_sql_ratio_verbose_true(select_claus, tb, group_condition=group_claus),
                        # self.__single_sql_ratio_verbose_false(select_claus, tb, where_claus, having_claus),
                        # self.__single_sql_ratio_verbose_false(select_claus, tb,),
                        # self.__single_sql_ratio_verbose_false(select_claus, tb, where_condition=where_claus),
                        # self.__single_sql_ratio_verbose_false(select_claus, tb, group_condition=group_claus),
                        
                        self.__single_sql_analyze(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_analyze(select_claus, tb,),
                        self.__single_sql_analyze(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_analyze(select_claus, tb, group_condition=group_claus),
                        self.__single_sql_analyze_verbose_true(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_analyze_verbose_true(select_claus, tb,),
                        self.__single_sql_analyze_verbose_true(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_analyze_verbose_true(select_claus, tb, group_condition=group_claus),
                        self.__single_sql_analyze_verbose_false(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_analyze_verbose_false(select_claus, tb,),
                        self.__single_sql_analyze_verbose_false(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_analyze_verbose_false(select_claus, tb, group_condition=group_claus),
                        
                        # reason for remove ratio: not implemented in the code
                        # self.__single_sql_analyze_ratio(select_claus, tb, where_claus, having_claus),
                        # self.__single_sql_analyze_ratio(select_claus, tb,),
                        # self.__single_sql_analyze_ratio(select_claus, tb, where_condition=where_claus),
                        # self.__single_sql_analyze_ratio(select_claus, tb, group_condition=group_claus),
                        # self.__single_sql_analyze_ratio_verbose_true(select_claus, tb, where_claus, having_claus),
                        # self.__single_sql_analyze_ratio_verbose_true(select_claus, tb,),
                        # self.__single_sql_analyze_ratio_verbose_true(select_claus, tb, where_condition=where_claus),
                        # self.__single_sql_analyze_ratio_verbose_true(select_claus, tb, group_condition=group_claus),
                        # self.__single_sql_analyze_ratio_verbose_false(select_claus, tb, where_claus, having_claus),
                        # self.__single_sql_analyze_ratio_verbose_false(select_claus, tb,),
                        # self.__single_sql_analyze_ratio_verbose_false(select_claus, tb, where_condition=where_claus),
                        # self.__single_sql_analyze_ratio_verbose_false(select_claus, tb, group_condition=group_claus),
                    )
                )

        # return filter(None, sqls)
        return list(filter(None, sqls))

    def explain_check(self):
        sqls = self.sql_list()
        tdLog.printNoPrefix("===step 1: curent case, must return query OK")
        for i in range(len(sqls)):
            tdLog.info(f"sql: {sqls[i]}")
            tdSql.query(sqls[i])

    def __test_current(self, dbname=DBNAME):
        
        ratio = random.uniform(0.001,1)
        
        tdSql.query(f"explain select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain select 1 from {dbname}.ct2")
        tdSql.query(f"explain select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain verbose true select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain verbose true select 1 from {dbname}.ct2")
        tdSql.query(f"explain verbose true select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain verbose true select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain verbose true select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain verbose true select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain verbose false select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain verbose false select 1 from {dbname}.ct2")
        tdSql.query(f"explain verbose false select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain verbose false select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain verbose false select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain verbose false select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        
        tdSql.query(f"explain ratio {ratio} select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain ratio {ratio} select 1 from {dbname}.ct2")
        tdSql.query(f"explain ratio {ratio} select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain ratio {ratio} select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain ratio {ratio} select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain ratio {ratio} select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain ratio {ratio} verbose true select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain ratio {ratio} verbose true select 1 from {dbname}.ct2")
        tdSql.query(f"explain ratio {ratio} verbose true select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain ratio {ratio} verbose true select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain ratio {ratio} verbose true select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain ratio {ratio} verbose true select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain ratio {ratio} verbose false select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain ratio {ratio} verbose false select 1 from {dbname}.ct2")
        tdSql.query(f"explain ratio {ratio} verbose false select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain ratio {ratio} verbose false select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain ratio {ratio} verbose false select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain ratio {ratio} verbose false select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain analyze select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain analyze select 1 from {dbname}.ct2")
        tdSql.query(f"explain analyze select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain analyze select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain analyze select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain analyze select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain analyze verbose true select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain analyze verbose true select 1 from {dbname}.ct2")
        tdSql.query(f"explain analyze verbose true select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain analyze verbose true select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain analyze verbose true select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain analyze verbose true select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain analyze verbose false select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain analyze verbose false select 1 from {dbname}.ct2")
        tdSql.query(f"explain analyze verbose false select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain analyze verbose false select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain analyze verbose false select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain analyze verbose false select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        
        tdSql.query(f"explain analyze ratio {ratio} select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain analyze ratio {ratio} select 1 from {dbname}.ct2")
        tdSql.query(f"explain analyze ratio {ratio} select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain analyze ratio {ratio} select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain analyze ratio {ratio} select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain analyze ratio {ratio} select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain analyze ratio {ratio} verbose true select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain analyze ratio {ratio} verbose true select 1 from {dbname}.ct2")
        tdSql.query(f"explain analyze ratio {ratio} verbose true select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain analyze ratio {ratio} verbose true select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain analyze ratio {ratio} verbose true select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain analyze ratio {ratio} verbose true select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain analyze ratio {ratio} verbose false select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain analyze ratio {ratio} verbose false select 1 from {dbname}.ct2")
        tdSql.query(f"explain analyze ratio {ratio} verbose false select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain analyze ratio {ratio} verbose false select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain analyze ratio {ratio} verbose false select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain analyze ratio {ratio} verbose false select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()

        # coverage explain.c add
        tdSql.query(f"explain verbose true select * from {dbname}.stb1 partition by c1 order by c2")

    def __test_error(self, dbname=DBNAME):
        
        ratio = random.uniform(0.001,1)

        tdLog.printNoPrefix("===step 0: err case, must return err")
        tdSql.error( f"explain select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain show databases " )
        tdSql.error( f"explain show {dbname}.stables " )
        tdSql.error( f"explain show {dbname}.tables " )
        tdSql.error( f"explain show {dbname}.vgroups " )
        tdSql.error( f"explain show dnodes " )
        tdSql.error( f'''explain select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain verbose true  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain verbose true  show databases " )
        tdSql.error( f"explain verbose true  show {dbname}.stables " )
        tdSql.error( f"explain verbose true  show {dbname}.tables " )
        tdSql.error( f"explain verbose true  show {dbname}.vgroups " )
        tdSql.error( f"explain verbose true  show dnodes " )
        tdSql.error( f'''explain verbose true  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain verbose false  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain verbose false  show databases " )
        tdSql.error( f"explain verbose false  show {dbname}.stables " )
        tdSql.error( f"explain verbose false  show {dbname}.tables " )
        tdSql.error( f"explain verbose false  show {dbname}.vgroups " )
        tdSql.error( f"explain verbose false  show dnodes " )
        tdSql.error( f'''explain verbose false  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
              

        tdLog.printNoPrefix("===step 0: err case, must return err")
        tdSql.error( f"explain ratio {ratio} select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain ratio {ratio} show databases " )
        tdSql.error( f"explain ratio {ratio} show {dbname}.stables " )
        tdSql.error( f"explain ratio {ratio} show {dbname}.tables " )
        tdSql.error( f"explain ratio {ratio} show {dbname}.vgroups " )
        tdSql.error( f"explain ratio {ratio} show dnodes " )
        tdSql.error( f'''explain ratio {ratio} select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain ratio {ratio} verbose true  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain ratio {ratio} verbose true  show databases " )
        tdSql.error( f"explain ratio {ratio} verbose true  show {dbname}.stables " )
        tdSql.error( f"explain ratio {ratio} verbose true  show {dbname}.tables " )
        tdSql.error( f"explain ratio {ratio} verbose true  show {dbname}.vgroups " )
        tdSql.error( f"explain ratio {ratio} verbose true  show dnodes " )
        tdSql.error( f'''explain ratio {ratio} verbose true  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain ratio {ratio} verbose false  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain ratio {ratio} verbose false  show databases " )
        tdSql.error( f"explain ratio {ratio} verbose false  show {dbname}.stables " )
        tdSql.error( f"explain ratio {ratio} verbose false  show {dbname}.tables " )
        tdSql.error( f"explain ratio {ratio} verbose false  show {dbname}.vgroups " )
        tdSql.error( f"explain ratio {ratio} verbose false  show dnodes " )
        tdSql.error( f'''explain ratio {ratio} verbose false  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )

        tdLog.printNoPrefix("===step 0: err case, must return err")
        tdSql.error( f"explain analyze select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain analyze show databases " )
        tdSql.error( f"explain analyze show {dbname}.stables " )
        tdSql.error( f"explain analyze show {dbname}.tables " )
        tdSql.error( f"explain analyze show {dbname}.vgroups " )
        tdSql.error( f"explain analyze show dnodes " )
        tdSql.error( f'''explain analyze select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain analyze verbose true  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain analyze verbose true  show databases " )
        tdSql.error( f"explain analyze verbose true  show {dbname}.stables " )
        tdSql.error( f"explain analyze verbose true  show {dbname}.tables " )
        tdSql.error( f"explain analyze verbose true  show {dbname}.vgroups " )
        tdSql.error( f"explain analyze verbose true  show dnodes " )
        tdSql.error( f'''explain analyze verbose true  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain analyze verbose false  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain analyze verbose false  show databases " )
        tdSql.error( f"explain analyze verbose false  show {dbname}.stables " )
        tdSql.error( f"explain analyze verbose false  show {dbname}.tables " )
        tdSql.error( f"explain analyze verbose false  show {dbname}.vgroups " )
        tdSql.error( f"explain analyze verbose false  show dnodes " )
        tdSql.error( f'''explain analyze verbose false  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )

        tdLog.printNoPrefix("===step 0: err case, must return err")
        tdSql.error( f"explain analyze ratio {ratio} select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain analyze ratio {ratio} show databases " )
        tdSql.error( f"explain analyze ratio {ratio} show {dbname}.stables " )
        tdSql.error( f"explain analyze ratio {ratio} show {dbname}.tables " )
        tdSql.error( f"explain analyze ratio {ratio} show {dbname}.vgroups " )
        tdSql.error( f"explain analyze ratio {ratio} show dnodes " )
        tdSql.error( f'''explain analyze ratio {ratio} select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain analyze ratio {ratio} verbose true  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain analyze ratio {ratio} verbose true  show databases " )
        tdSql.error( f"explain analyze ratio {ratio} verbose true  show {dbname}.stables " )
        tdSql.error( f"explain analyze ratio {ratio} verbose true  show {dbname}.tables " )
        tdSql.error( f"explain analyze ratio {ratio} verbose true  show {dbname}.vgroups " )
        tdSql.error( f"explain analyze ratio {ratio} verbose true  show dnodes " )
        tdSql.error( f'''explain analyze ratio {ratio} verbose true  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain analyze ratio {ratio} verbose false  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain analyze ratio {ratio} verbose false  show databases " )
        tdSql.error( f"explain analyze ratio {ratio} verbose false  show {dbname}.stables " )
        tdSql.error( f"explain analyze ratio {ratio} verbose false  show {dbname}.tables " )
        tdSql.error( f"explain analyze ratio {ratio} verbose false  show {dbname}.vgroups " )
        tdSql.error( f"explain analyze ratio {ratio} verbose false  show dnodes " )
        tdSql.error( f'''explain analyze ratio {ratio} verbose false  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
                     

    def all_test(self):
        self.__test_error()
        self.__test_current()

    def __create_tb(self, dbname=DBNAME):

        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
            ) tags (t1 int)
            '''
        create_ntb_sql = f'''create table {dbname}.t1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')
            { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2}

    def __insert_data(self, rows, dbname=DBNAME):
        now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        for i in range(rows):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( { now_time - i * 1000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( { now_time - i * 7776000000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct2 values ( { now_time - i * 7776000000 }, {-i},  {-11111 * i}, {-111 * i % 32767 }, {-11 * i % 127}, {-1.11*i}, {-1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
        tdSql.execute(
            f'''insert into {dbname}.ct1 values
            ( { now_time - rows * 5 }, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar_测试_0', { now_time + 8 } )
            ( { now_time + 10000 }, { rows }, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar_测试_9', { now_time + 9 } )
            '''
        )

        tdSql.execute(
            f'''insert into {dbname}.ct4 values
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
            f'''insert into {dbname}.ct2 values
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
            insert_data = f'''insert into {dbname}.t1 values
                ( { now_time - i * 3600000 }, {i}, {i * 11111}, { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2},
                "binary_{i}", "nchar_测试_{i}", { now_time - 1000 * i } )
                '''
            tdSql.execute(insert_data)
        tdSql.execute(
            f'''insert into {dbname}.t1 values
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

    def do_explain_complex(self):        
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
    
        tdLog.printNoPrefix("do explain complex .................... [passed]")

    def __extract_explain_plan_lines(self):
        lines = []
        for row in tdSql.queryResult:
            line = row[0] if isinstance(row, (list, tuple)) else row
            if line is not None:
                lines.append(str(line))
        return lines

    def __extract_exchange_indices(self, plan_lines):
        exchange_idx = [
            idx for idx, line in enumerate(plan_lines) if "Data Exchange" in line
        ]
        return exchange_idx

    def __check_filter_efficiency_percent(self, plan_lines):
        """
        Check filter efficiency percentage in explain plan lines.

        Format examples:
          - ...Filter: conditions=t1=1 efficiency=33.3%...
          - ...Tag Index Filter: conditions=t1=1...

        Validation rules:
          1. efficiency field must be present in Filter or Tag Index Filter line
          2. efficiency value must be a valid number
          3. efficiency value must be between 0 and 100
        """
        matched = []
        for line in plan_lines:
            if "Filter" not in line or "Tag Index" in line:
                continue
            m = re.search(r"efficiency=(\d+(?:\.\d+)?)%", line)
            if m is None:
                raise AssertionError(
                    "efficiency field not found in Filter line: {}".format(line)
                )
            try:
                matched.append(float(m.group(1)))
            except ValueError:
                raise AssertionError(
                    "efficiency value is not a valid number in line: {}".format(line)
                )

        for v in matched:
            assert 0.0 <= v <= 100.0, f"efficiency out of range: {v}"

    def __check_cost_and_rows_validity(self, plan_lines):
        """
        Check cost and rows values in explain plan lines.

        Format examples:
          - ...cost=0.316..0.316 rows=1...
          - ...cost=0.316(0.316)..0.316(0.316) rows=1...

        Validation rules:
          1. cost values must be non-negative
          2. cost values must not be millisecond timestamps (not near current time)
          3. for cost=A..C or cost=A(B)..C(D), C must >= A and D must >= B and B must >= A and D must >= C
          4. if all cost values are 0, then rows must also be 0
        """
        # Threshold for detecting timestamp values (1 day in ms)
        COST_MAX_REASONABLE_MS = 24 * 60 * 60 * 1000

        for line in plan_lines:
            if "cost=" not in line or " rows=" not in line:
                # note that the space before "rows=" is important
                continue

            # Extract cost values
            # Pattern 1: cost=X..Y (two values)
            # Pattern 2: cost=A(B)..C(D) (four values)
            cost_pattern = r"cost=([0-9.]+)(?:\(([0-9.]+)\))?\.\.([0-9.]+)(?:\(([0-9.]+)\))?"
            cost_match = re.search(cost_pattern, line)
            if cost_match is None:
                raise AssertionError(
                    "cost pattern not found in line: {}".format(line)
                )

            cost_values = []
            cost_values.append(float(cost_match.group(1)))
            cost_values.append(float(cost_match.group(3)))
            if cost_match.group(2) is not None:
                cost_values.append(float(cost_match.group(2)))
            if cost_match.group(4) is not None:
                cost_values.append(float(cost_match.group(4)))

            # Extract rows value
            rows_pattern = r"rows=([0-9.]+)"
            rows_match = re.search(rows_pattern, line)
            if rows_match is None:
                raise AssertionError(
                    "rows pattern not found in line: {}".format(line)
                )
            rows_value = float(rows_match.group(1))

            # Validation 1: cost values must be non-negative
            for cv in cost_values:
                if cv < 0:
                    raise AssertionError(
                        "cost value is negative: {} in line: {}".format(cv, line)
                    )

            # Validation 2: cost values must not be millisecond timestamps
            for cv in cost_values:
                if cv > COST_MAX_REASONABLE_MS:
                    raise AssertionError(
                        "cost value {} appears to be a timestamp (too large) in line: {}".format(cv, line)
                    )

            # Validation 3: for cost=A..C or cost=A(B)..C(D),
            # C must >= A and D must >= B and B must >= A and D must >= C
            cost_first = float(cost_match.group(1))
            cost_last = float(cost_match.group(3))
            if cost_last < cost_first:
                raise AssertionError(
                    "cost last row time {} < first row time {} in line: {}" \
                        .format(cost_last, cost_first, line)
                )
            if cost_match.group(2) is not None and cost_match.group(4) is not None:
                max_first = float(cost_match.group(2))
                max_last = float(cost_match.group(4))
                if max_last < max_first:
                    raise AssertionError(
                        "aggregated cost max last row time {} < max first row time {} in line: {}" \
                            .format(max_last, max_first, line)
                    )
                if max_first < cost_first or max_last < cost_last:
                    raise AssertionError(
                        "aggregated cost max first row time {} < avg first row time {} or "
                        "max last row time {} < avg last row time {} in line: {}" \
                            .format(max_first, cost_first, max_last, cost_last, line)
                    )

            # Validation 4: if all cost values are 0, rows must be 0
            if all(cv == 0 for cv in cost_values) and rows_value != 0:
                raise AssertionError(
                    "all cost values are 0 but rows={} in line: {}" \
                        .format(rows_value, line)
                )

    def __check_exec_cost_validity(self, plan_lines):
        """
        Check Exec cost line for compute, times, input_wait, output_wait fields.

        Format examples:
          - Exec cost: compute=0.316(0.316) times=1.0(1) input_wait=0.251(0.259) output_wait=0.5(1)

        Validation rules:
          1. values must be non-negative
          2. if aggregated, max value (in parentheses) >= avg value (before parentheses)
        """
        # Fields to check
        check_fields = ["compute", "times", "input_wait", "output_wait"]

        for line in plan_lines:
            if "Exec cost:" not in line:
                continue

            for field in check_fields:
                # Pattern: field=value or field=avg(max)
                pattern = r"{}=([0-9.]+)(?:\(([0-9.]+)\))?".format(field)
                match = re.search(pattern, line)
                if match is None:
                    raise AssertionError(
                        "{} field pattern not found in line: {}".format(field, line)
                    )

                avg_value = float(match.group(1))

                # Validation 1: value must be non-negative
                if avg_value < 0:
                    raise AssertionError(
                        "{} value {} is negative in line: {}"
                            .format(field, avg_value, line)
                    )

                # Validation 2: if aggregated, max >= avg
                if match.group(2) is not None:
                    max_value = float(match.group(2))
                    if max_value < 0:
                        raise AssertionError(
                            "{} max value {} is negative in line: {}"
                                .format(field, max_value, line)
                        )
                    if max_value < avg_value:
                        raise AssertionError(
                            "{} max value {} < avg value {} in line: {}" \
                                .format(field, max_value, avg_value, line)
                        )

    def __check_exchange_network_validity(self, plan_lines):
        """
        Check Network line under Data Exchange operator.

        Format examples:
          - Data Exchange 2:1 (cost=0.316..0.316 rows=1 width=16)
          - ...
          -    Network: mode=concurrent fetch_times=1.0(1) fetch_rows=0.5(1) fetch_cost=0.251(0.259)

        Validation rules:
          1. Each Data Exchange must have a corresponding Network line
          2. mode must be either 'concurrent' or 'sequence'
          3. fetch fields (fetch_times, fetch_rows, fetch_cost) must be non-negative
          4. if aggregated, max value >= avg value
          5. N:1 exchange (N>1) must have aggregated fetch values (avg(max) format)
             1:1 exchange must have single fetch values (no parentheses)
        """
        valid_modes = {"concurrent", "sequence"}
        fetch_fields = ["fetch_times", "fetch_rows", "fetch_cost"]

        # Find all Data Exchange lines and their Network lines
        exchange_pattern = r"Data Exchange\s+(\d+):(\d+)"
        network_pattern = r"Network:\s+mode=(\w+)"

        i = 0
        while i < len(plan_lines):
            line = plan_lines[i]
            exchange_match = re.search(exchange_pattern, line)

            if exchange_match is None:
                i += 1
                continue

            # Look for the corresponding Network line (should be within next few lines)
            network_line = None
            for j in range(i + 1, min(i + 10, len(plan_lines))):
                if "Network:" in plan_lines[j]:
                    network_line = plan_lines[j]
                    break
                # Stop if we hit another Data Exchange
                if "Data Exchange" in plan_lines[j]:
                    break

            if network_line is None:
                raise AssertionError(
                    "Network line not found for Data Exchange at line: {}".format(line)
                )

            # Found a Data Exchange line
            src_count = int(exchange_match.group(1))

            # Check mode
            mode_match = re.search(network_pattern, network_line)
            if mode_match is None:
                raise AssertionError(
                    "mode not found in Network line: {}".format(network_line)
                )
            mode = mode_match.group(1)
            if mode not in valid_modes:
                raise AssertionError(
                    "invalid mode '{}' in Network line: {}".format(mode, network_line)
                )

            # Check fetch fields
            for field in fetch_fields:
                # Pattern: field=value or field=avg(max)
                pattern = r"{}=([0-9.]+)(?:\(([0-9.]+)\))?".format(field)
                match = re.search(pattern, network_line)
                if match is None:
                    raise AssertionError(
                        "{} field not found in Network line: {}".format(field, network_line)
                    )

                avg_value = float(match.group(1))
                has_aggregation = match.group(2) is not None

                # Validation: value must be non-negative
                if avg_value < 0:
                    raise AssertionError(
                        "{} value {} is negative in Network line: {}"
                            .format(field, avg_value, network_line)
                    )

                # Validation: aggregation format must match exchange type
                if src_count > 1 and not has_aggregation:
                    raise AssertionError(
                        "N:1 exchange should have aggregated {} (avg(max) format) in Network line: {}"
                            .format(field, network_line)
                    )
                if src_count == 1 and has_aggregation:
                    raise AssertionError(
                        "1:1 exchange should have single {} value (no parentheses) in Network line: {}"
                            .format(field, network_line)
                    )

                # Validation: if aggregated, max >= avg
                if has_aggregation:
                    max_value = float(match.group(2))
                    if max_value < 0:
                        raise AssertionError(
                            "{} max value {} is negative in Network line: {}"
                                .format(field, max_value, network_line)
                        )
                    if max_value < avg_value:
                        raise AssertionError(
                            "{} max value {} < avg value {} in Network line: {}" \
                                .format(field, max_value, avg_value, network_line)
                        )

            i = j + 1 # skip to the next Data Exchange line

    def __check_io_cost_validity(self, plan_lines):
        """
        Check I/O cost lines (3 consecutive lines under Table Scan).

        Format examples:
        Line 1: total_blocks, file_load_blocks, stt_load_blocks, mem_load_blocks, sma_load_blocks, composed_blocks
        Line 2: file_load_elapsed, stt_load_elapsed, mem_load_elapsed, sma_load_elapsed, composed_elapsed
        Line 3: total_rows, check_rows [, slowest_vgroup_id, slow_deviation, cost_ratio, data_deviation]

        Validation rules:
          1. all numeric values must be non-negative (including percentages)
          2. check_rows >= total_blocks
          3. slowest_vgroup_id, slow_deviation, cost_ratio, data_deviation only appear when
             there are multiple vgroups
        """
        # Fields that should be non-negative (with optional aggregation)
        block_fields = ["total_blocks", "file_load_blocks", "stt_load_blocks",
                        "mem_load_blocks", "sma_load_blocks", "composed_blocks"]
        elapsed_fields = ["file_load_elapsed", "stt_load_elapsed", "mem_load_elapsed",
                          "sma_load_elapsed", "composed_elapsed"]
        row_fields = ["total_rows", "check_rows"]

        # Optional fields (only present when there are multiple vgroups)
        opt_single_fields = ["slowest_vgroup_id", "cost_ratio"]
        opt_percent_fields = ["slow_deviation", "data_deviation"]

        def extract_value(line, field, allow_aggregation=True):
            """Extract value from line, returns (avg_value, max_value or None)"""
            if allow_aggregation:
                pattern = r"{}=([0-9.]+)(?:\(([0-9.]+)\))?".format(field)
            else:
                pattern = r"{}=([0-9.]+)".format(field)
            match = re.search(pattern, line)
            if match is None:
                return None, None
            avg = float(match.group(1))
            max_val = float(match.group(2)) if match.lastindex and match.lastindex >= 2 else None
            return avg, max_val

        def extract_percent(line, field):
            """Extract percentage value from line (e.g., slow_deviation=3%)"""
            pattern = r"{}=([0-9.]+)%".format(field)
            match = re.search(pattern, line)
            if match is None:
                return None
            return float(match.group(1))

        i = 0
        while i < len(plan_lines):
            line = plan_lines[i]

            # Look for I/O cost header line
            if "I/O cost:" not in line:
                i += 1
                continue

            # Collect the 3 I/O cost lines
            io_lines = [line]
            for j in range(i + 1, min(i + 3, len(plan_lines))):
                if "I/O cost:" in plan_lines[j]:
                    break
                io_lines.append(plan_lines[j])

            if len(io_lines) < 3:
                raise AssertionError(
                    "I/O cost section has less than 3 lines starting at: {}".format(line)
                )

            line1, line2, line3 = io_lines[0], io_lines[1], io_lines[2]

            # Check if data is aggregated by checking total_blocks format
            _, total_blocks_max = extract_value(line1, "total_blocks")
            is_aggregated = total_blocks_max is not None

            # Validation 1: all values must be non-negative

            # Check block fields (line 1)
            for field in block_fields:
                avg, max_val = extract_value(line1, field)
                if avg is None:
                    raise AssertionError(
                        "{} not found in I/O cost line: {}".format(field, line1)
                    )
                if avg < 0:
                    raise AssertionError(
                        "{} value {} is negative in line: {}".format(field, avg, line1)
                    )
                if max_val is not None and max_val < 0:
                    raise AssertionError(
                        "{} max value {} is negative in line: {}".format(field, max_val, line1)
                    )
                if max_val is not None and max_val < avg:
                    raise AssertionError(
                        "{} max {} < avg {} in line: {}".format(field, max_val, avg, line1)
                    )

            # Check elapsed fields (line 2)
            for field in elapsed_fields:
                avg, max_val = extract_value(line2, field)
                if avg is None:
                    raise AssertionError(
                        "{} not found in I/O cost line: {}".format(field, line2)
                    )
                if avg < 0:
                    raise AssertionError(
                        "{} value {} is negative in line: {}".format(field, avg, line2)
                    )
                if max_val is not None and max_val < 0:
                    raise AssertionError(
                        "{} max value {} is negative in line: {}".format(field, max_val, line2)
                    )
                if max_val is not None and max_val < avg:
                    raise AssertionError(
                        "{} max {} < avg {} in line: {}".format(field, max_val, avg, line2)
                    )

            # Check row fields (line 3)
            for field in row_fields:
                avg, max_val = extract_value(line3, field)
                if avg is None:
                    raise AssertionError(
                        "{} not found in I/O cost line: {}".format(field, line3)
                    )
                if avg < 0:
                    raise AssertionError(
                        "{} value {} is negative in line: {}".format(field, avg, line3)
                    )
                if max_val is not None and max_val < 0:
                    raise AssertionError(
                        "{} max value {} is negative in line: {}".format(field, max_val, line3)
                    )
                if max_val is not None and max_val < avg:
                    raise AssertionError(
                        "{} max {} < avg {} in line: {}".format(field, max_val, avg, line3)
                    )

            # Check optional fields (line 3) - only when aggregated
            if is_aggregated:
                # Check optional single value fields
                for field in opt_single_fields:
                    avg, _ = extract_value(line3, field, allow_aggregation=False)
                    if avg is None:
                        raise AssertionError(
                            "{} not found in aggregated I/O cost line: {}".format(field, line3)
                        )
                    if avg < 0:
                        raise AssertionError(
                            "{} value {} is negative in line: {}".format(field, avg, line3)
                        )

                # Check optional percentage fields
                for field in opt_percent_fields:
                    val = extract_percent(line3, field)
                    if val is None:
                        raise AssertionError(
                            "{} not found in aggregated I/O cost line: {}".format(field, line3)
                        )
                    if val < 0:
                        raise AssertionError(
                            "{} value {}% is negative in line: {}".format(field, val, line3)
                        )
            else:
                # When not aggregated, optional fields should not be present
                for field in opt_single_fields:
                    avg, _ = extract_value(line3, field, allow_aggregation=False)
                    if avg is not None:
                        raise AssertionError(
                            "{} should not appear in non-aggregated I/O cost line: {}".format(field, line3)
                        )
                for field in opt_percent_fields:
                    val = extract_percent(line3, field)
                    if val is not None:
                        raise AssertionError(
                            "{} should not appear in non-aggregated I/O cost line: {}".format(field, line3)
                        )

            # Validation 2: check_rows >= total_blocks
            check_rows_avg, check_rows_max = extract_value(line3, "check_rows")
            total_blocks_avg, total_blocks_max = extract_value(line1, "total_blocks")

            if check_rows_avg < total_blocks_avg:
                raise AssertionError(
                    "check_rows {} < total_blocks {} in I/O cost lines: {} / {}"
                        .format(check_rows_avg, total_blocks_avg, line1, line3)
                )

            if check_rows_max is not None and total_blocks_max is not None:
                if check_rows_max < total_blocks_max:
                    raise AssertionError(
                        "check_rows max {} < total_blocks max {} in I/O cost lines: {} / {}"
                            .format(check_rows_max, total_blocks_max, line1, line3)
                    )

            i += len(io_lines)

    def do_explain_partition_by_tag_regression(self):
        tdSql.execute("drop database if exists db_explain_reg")
        tdSql.execute("create database db_explain_reg vgroups 2")
        tdSql.execute("use db_explain_reg")
        tdSql.execute("create stable stb (ts timestamp, v int) tags(gid int)")
        tdSql.execute("create table t1 using stb tags(1)")
        tdSql.execute("create table t2 using stb tags(2)")
        tdSql.execute("create table t3 using stb tags(3)")

        insert_rows = 4
        tdSql.execute("insert into t1 values(now, 1)")
        tdSql.execute("insert into t2 values(now - 1s, 2)(now - 2s, 3)(now - 3s, 4)")

        # check partition by gid
        tdSql.query("explain analyze verbose true select * from stb partition by gid")
        plan_lines = self.__extract_explain_plan_lines()
        self.__check_exchange_network_validity(plan_lines)
        self.__check_cost_and_rows_validity(plan_lines)
        self.__check_exec_cost_validity(plan_lines)
        self.__check_io_cost_validity(plan_lines)
        exchange_idx = self.__extract_exchange_indices(plan_lines)
        assert len(exchange_idx) == 2, (
            "expect two Data Exchange nodes for partition by gid"
        )

        # check union and filter
        tdSql.query("explain analyze verbose true select ts, v from stb where "
            "gid < 3 and v > 0 union select * from t3")
        plan_lines = self.__extract_explain_plan_lines()
        self.__check_filter_efficiency_percent(plan_lines)
        self.__check_exchange_network_validity(plan_lines)
        self.__check_cost_and_rows_validity(plan_lines)
        self.__check_exec_cost_validity(plan_lines)
        self.__check_io_cost_validity(plan_lines)
        exchange_idx = self.__extract_exchange_indices(plan_lines)
        assert len(exchange_idx) == 2, (
            "expect two Data Exchange nodes for union and filter"
        )

        tdLog.printNoPrefix("do explain partition by tag regression ... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_explain_basic(self):
        """Explain command basic

        1. Performing EXPLAIN on queries involving various functions, windows, subqueries, and sorting operations
        2. Verify bug TD-20582 (explain order by sql error)

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/query/explain.sim
            - 2025-8-20 Simon Guan Migrated from tsim/query/explain_tsorder.sim
            - 2025-10-31 Alex Duan Migrated from uncatalog/system-test/99-TDcase/test_TD_20582.py
            - 2025-12-19 Alex Duan Migrated from uncatalog/system-test/2-query/test_explain.py
            - 2026-3-9 Tony Zhang Create partition by tag regression test case

        """
        self.do_explain_basic()
        self.do_td_20582()
        self.do_explain_complex()
        self.do_explain_partition_by_tag_regression()
