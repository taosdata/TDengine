from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestAndOr:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
    #
    # ------------------- 1 ----------------
    #
    def do_sim_like(self):
        self.Like()
        tdStream.dropAllStreamsAndDbs()
        self.Tag()
        tdStream.dropAllStreamsAndDbs()
        print("do sim like ........................... [passed]")
        
    def Like(self):
        tdLog.info(f"======================== dnode1 start")

        db = "testdb"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} cachemodel 'last_value'")
        tdSql.execute(f"use {db}")

        table1 = "table_name"
        table2 = "tablexname"

        tdSql.execute(f"create table {table1} (ts timestamp, b binary(20))")
        tdSql.execute(f"create table {table2} (ts timestamp, b binary(20))")

        tdSql.execute(f'insert into {table1} values(now,    "table_name")')
        tdSql.execute(f'insert into {table1} values(now-3m, "tablexname")')
        tdSql.execute(f'insert into {table1} values(now-2m, "tablexxx")')
        tdSql.execute(f'insert into {table1} values(now-1m, "table")')

        tdSql.query(f"select b from {table1}")
        tdSql.checkRows(4)

        tdSql.query(f"select b from {table1} where b like 'table_name'")
        tdSql.checkRows(2)

        tdSql.query(f"select b from {table1} where b like 'table\_name'")
        tdSql.checkRows(1)

        tdSql.query(f"show tables;")
        tdSql.checkRows(2)

        tdSql.query(f"show tables like 'table_name'")
        tdSql.checkRows(2)

        tdSql.query(f"show tables like 'table\_name'")
        tdSql.checkRows(1)

        view1 = "view1_name"
        view2 = "view2_name"

        tdSql.execute(f"CREATE VIEW {view1} as select * from {table1}")
        tdSql.execute(f"CREATE VIEW {view2} AS select * from {table2}")

        tdSql.query(f"show views like 'view%'")
        tdSql.checkRows(2)

        tdSql.query(f"show views like 'view1%'")
        tdSql.checkRows(1)

    def Tag(self):
        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database if not exists db1 vgroups 10;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create stable sta (ts timestamp, f1 double, f2 binary(200)) tags(t1 binary(100));")
        tdSql.execute(f"create table tba1 using sta tags('ZQMPvstuzZVzCRjFTQawILuGSqZKSqlJwcBtZMxrAEqBbzChHWVDMiAZJwESzJAf');")
        tdSql.execute(f"create table tba2 using sta tags('ieofwehughkreghughuerugu34jf9340aieefjalie28ffj8fj8fafjaekdfjfii');")
        tdSql.execute(f"create table tba3 using sta tags('ZQMPvstuzZVzCRjFTQawILuGSqabSqlJwcBtZMxrAEqBbzChHWVDMiAZJwESzJAf');")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:01', 1.0, \"a\");")
        tdSql.execute(f"insert into tba2 values ('2022-04-26 15:15:01', 1.0, \"a\");")
        tdSql.execute(f"insert into tba2 values ('2022-04-26 15:15:02', 1.0, \"a\");")
        tdSql.execute(f"insert into tba3 values ('2022-04-26 15:15:01', 1.0, \"a\");")
        tdSql.execute(f"insert into tba3 values ('2022-04-26 15:15:02', 1.0, \"a\");")
        tdSql.execute(f"insert into tba3 values ('2022-04-26 15:15:03', 1.0, \"a\");")
        tdSql.query(f"select t1 from sta where t1 like '%ab%';")
        tdSql.checkRows(3)

        tdSql.query(f"select t1 from sta where t1 like '%ax%';")
        tdSql.checkRows(0)

        tdSql.query(f"select t1 from sta where t1 like '%cd%';")
        tdSql.checkRows(0)

        tdSql.query(f"select t1 from sta where t1 like '%ii';")
        tdSql.checkRows(2)

    #
    # ------------------- 1 ----------------
    #
    def initDB(self):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db")
        
    def stopTest(self):
        tdSql.execute("drop database if exists db")

    def like_wildcard_test(self):
        tdSql.execute("create table db.t1x (ts timestamp, c1 varchar(100))")
        tdSql.execute("create table db.t_1x (ts timestamp, c1 varchar(100))")

        tdSql.query("select * from information_schema.ins_columns where table_name like '%1x'")
        tdSql.checkRows(4)
        
        tdSql.query("select * from information_schema.ins_columns where table_name like '%\_1x'")
        tdSql.checkRows(2)

        tdSql.query("insert into db.t1x values(now, 'abc'), (now+1s, 'a%c'),(now+2s, 'a_c'),(now+3s, '_c'),(now+4s, '%c')")
        
        tdSql.query("select * from db.t1x")
        tdSql.checkRows(5)
        
        tdSql.query("select * from db.t1x where c1 like '%_c'")
        tdSql.checkRows(5)
        
        tdSql.query("select * from db.t1x where c1 like '%__c'")
        tdSql.checkRows(3)
        
        tdSql.query("select * from db.t1x where c1 like '%\_c'")
        tdSql.checkRows(2)
        
        tdSql.query("select * from db.t1x where c1 like '%\%c'")
        tdSql.checkRows(2)
        
        tdSql.query("select * from db.t1x where c1 like '_\%c'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "a%c")
        
        tdSql.query("select * from db.t1x where c1 like '_\_c'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "a_c")
        
        tdSql.query("select * from db.t1x where c1 like '%%_c'")
        tdSql.checkRows(5)
        
        tdSql.query("select * from db.t1x where c1 like '%_%c'")
        tdSql.checkRows(5)
        
        tdSql.query("select * from db.t1x where c1 like '__%c'")
        tdSql.checkRows(3)
        
        tdSql.query("select * from db.t1x where c1 not like '__%c'")
        tdSql.checkRows(2)
       
    def like_cnc_wildcard_test(self): 
        tdSql.execute("create table db.t3x (ts timestamp, c1 varchar(100))")
        
        tdSql.execute("insert into db.t3x values(now, '我是中文'), (now+1s, '我是_中文'), (now+2s, '我是%中文'), (now+3s, '%中文'),(now+4s, '_中文')")
        tdSql.query("select * from db.t3x")
        tdSql.checkRows(5)
          
        tdSql.query("select * from db.t3x where c1 like '%中文'")
        tdSql.checkRows(5)
        
        tdSql.query("select * from db.t3x where c1 like '%中_文'")
        tdSql.checkRows(0)
        
        tdSql.query("select * from db.t3x where c1 like '%\%中文'")
        tdSql.checkRows(2)
        
        tdSql.query("select * from db.t3x where c1 like '%\_中文'")
        tdSql.checkRows(2)
        
        tdSql.query("select * from db.t3x where c1 like '_中文'")
        tdSql.checkRows(2)
        
        tdSql.query("select * from db.t3x where c1 like '\_中文'")
        tdSql.checkRows(1)
        
    def like_multi_wildcard_test(self): 
        tdSql.execute("create table db.t4x (ts timestamp, c1 varchar(100))")

        # 插入测试数据
        tdSql.execute("insert into db.t4x values(now, 'abc'), (now+1s, 'a%c'),(now+2s, 'a_c'),(now+3s, '_c'),(now+4s, '%c')")
        tdSql.execute("insert into db.t4x values(now+5s, '%%%c'),(now+6s, '___c'),(now+7s, '%_%c'),(now+8s, '%\\c')")

        tdSql.query("select * from db.t4x where c1 like '%%%_'")
        tdSql.checkRows(9)

        tdSql.query("select * from db.t4x where c1 like '\%\%\%_'")
        tdSql.checkRows(1)
        
        tdSql.query("select * from db.t4x where c1 like '%\_%%'")
        tdSql.checkRows(4)

        tdSql.query("select * from db.t4x where c1 like '_\%\%'")
        tdSql.checkRows(0)
        
        tdSql.query("select * from db.t4x where c1 like '%abc%'")
        tdSql.checkRows(1)
        
        tdSql.query("select * from db.t4x where c1 like '_%abc%'")
        tdSql.checkRows(0)
        
        tdSql.query("select * from db.t4x where c1 like '\%%\%%'")
        tdSql.checkRows(2)
        
        tdSql.query("select * from db.t4x where c1 like '\%\_%\%%'")
        tdSql.checkRows(1)
        
    def like_wildcard_test2(self):
        tdSql.execute("create table db.t5x (ts timestamp, c1 varchar(100))")

        tdSql.execute("insert into db.t5x values(now(), 'a\%c')")
        tdSql.execute("insert into db.t5x values(now+1s, 'a\%bbbc')")
        tdSql.execute("insert into db.t5x values(now()+2s, 'a%c')")

        tdSql.query("select * from db.t5x where c1 like 'a\%c'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "a%c")

        tdSql.execute("create table db.t6x (ts timestamp, c1 varchar(100))")

        tdSql.execute("insert into db.t6x values(now(), '\%c')")
        tdSql.execute("insert into db.t6x values(now+1s, '\%bbbc')")
        tdSql.execute("insert into db.t6x values(now()+2s, '%c')")

        tdSql.query("select * from db.t6x where c1 like '\%c'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "%c")

        tdSql.execute("create table db.t7x (ts timestamp, c1 varchar(100))")

        tdSql.execute("insert into db.t7x values(now(), 'a\_c')")
        tdSql.execute("insert into db.t7x values(now+1s, 'a\_bbbc')")
        tdSql.execute("insert into db.t7x values(now()+2s, 'a_c')")

        tdSql.query("select * from db.t7x where c1 like 'a\_c'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "a_c")

        tdSql.execute("create table db.t8x (ts timestamp, c1 varchar(100))")

        tdSql.execute("insert into db.t8x values(now(), '\_c')")
        tdSql.execute("insert into db.t8x values(now+1s, '\_bbbc')")
        tdSql.execute("insert into db.t8x values(now()+2s, '_c')")

        tdSql.query("select * from db.t8x where c1 like '\_c'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "_c")

    def like_wildcard_test3(self):
        tdSql.execute("create table db.t9x (ts timestamp, c1 varchar(100))")
        
        # insert test data
        tdSql.execute("insert into db.t9x values (now(), '\\\\')")      # \
        tdSql.execute("insert into db.t9x values (now+1s, '%')")        # %
        tdSql.execute("insert into db.t9x values (now+2s, '\\\\\\\\')") # \\
        tdSql.execute("insert into db.t9x values (now+3s, '\\%')")      # \%
        tdSql.execute("insert into db.t9x values (now+4s, '%\\\\')")    # %\
        tdSql.execute("insert into db.t9x values (now+5s, '\\%\\%')")   # \%\%

        # query with like
        tdSql.query("select * from db.t9x where c1 like '%\\%'", show=True)             # pattern: '%\%' => ends with %
        tdSql.checkRows(3)

        tdSql.query("select * from db.t9x where c1 like '%\\\\%'", show=True)           # pattern: '%\\%' => ends with %
        tdSql.checkRows(3)

        tdSql.query("select * from db.t9x where c1 like '%\\\\\\%'", show=True)         # pattern: '%\\\%' => contains \
        tdSql.checkRows(5)

        tdSql.query("select * from db.t9x where c1 like '%\\\\\\\\%'", show=True)       # pattern: '%\\\\%' => contains \
        tdSql.checkRows(5)

        tdSql.query("select * from db.t9x where c1 like '%\\\\\\\\\\%'", show=True)     # pattern: '%\\\\\%' => contains \ and ends with %
        tdSql.checkRows(2)

    def do_query_like(self):
        tdLog.printNoPrefix("==========start like_wildcard_test run ...............")
        tdSql.prepare(replica = self.replicaVar)

        self.initDB()
        self.like_wildcard_test()
        self.like_cnc_wildcard_test()
        self.like_multi_wildcard_test()
        self.like_wildcard_test2()
        self.like_wildcard_test3()
        tdLog.printNoPrefix("==========end like_wildcard_test run ...............")
        self.stopTest()
        
        print("do query like ......................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_like(self):
        """Operator like

        1. Like in SELECT statements
        2. Like in SHOW statements
        3. Like in tag queries
        1. Like wildcard
        2. Like cnc wildcard
        3. Like multi wildcard
        4. Like escape character
        5. Like unicode
        6. Like special character
        7. Like backslash
        8. Like backslash and wildcard 
        9. Like backslash and escape character
        10. Like backslash, wildcard and escape character
        11. Like backslash, wildcard and cnc character
        12. Like on super/child table 
        

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:5
            - 2025-5-10 Simon Guan Migrated from tsim/parser/like.sim
            - 2025-5-10 Simon Guan Migrated from tsim/query/tagLikeFilter.sim
            - 2025-12-19 Alex Duan Migrated from uncatalog/system-test/2-query/test_like.py

        """
        self.do_sim_like()
        self.do_query_like()