from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck, tdCom


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
            f"CREATE STABLE `meters` (`ts` TIMESTAMP, `c2` INT) TAGS (`cc` VARCHAR(3))"
        )

        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-15 00:01:08.000 ",234)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-16 00:01:08.000 ",136)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-17 00:01:08.000 ", 59)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-18 00:01:08.000 ", 58)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-19 00:01:08.000 ",243)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-20 00:01:08.000 ",120)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-21 00:01:08.000 ", 11)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-22 00:01:08.000 ",196)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-23 00:01:08.000 ",116)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-24 00:01:08.000 ",210)'
        )

        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-15 00:01:08.000", 234)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-16 00:01:08.000", 136)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-17 00:01:08.000",  59)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-18 00:01:08.000",  58)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-19 00:01:08.000", 243)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-20 00:01:08.000", 120)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-21 00:01:08.000",  11)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-22 00:01:08.000", 196)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-23 00:01:08.000", 116)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-24 00:01:08.000", 210)'
        )

        resultfile = tdCom.generate_query_result(
            "cases/09-DataQuerying/15-Explain/t/test_explain.sql", "test_explain"
        )
        tdLog.info(f"resultfile: {resultfile}")
        tdCom.compare_result_files(
            resultfile, "cases/09-DataQuerying/15-Explain/r/test_explain.result"
        )
        print("do explain basic ...................... [passed]")
        
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
        tdSql.checkRows(16)

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

        print("do TD-20582 ........................... [passed]")

    
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

        """
        self.do_explain_basic()
        self.do_td_20582()
