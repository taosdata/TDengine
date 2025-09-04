from wsgiref.headers import tspecials
from new_test_framework.utils import tdLog, tdSql

class TestOperator:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.dbname = "db"
        cls.rowNum = 10
        cls.ts = 1537146000000

    def ts5757(self):
        
        tdSql.execute(f"create database if not exists {self.dbname}")
        
        tdSql.execute(f"DROP STABLE IF EXISTS {self.dbname}.super_t1;")
        tdSql.execute(f"DROP TABLE IF EXISTS {self.dbname}.t1;")
        tdSql.execute(f"CREATE STABLE IF NOT EXISTS {self.dbname}.super_t1(time TIMESTAMP, c0 BIGINT UNSIGNED) TAGS (location BINARY(64))")
        tdSql.execute(f"CREATE TABLE {self.dbname}.t1 USING {self.dbname}.super_t1 TAGS ('ek')")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c0) VALUES (1641024000000, 1);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c0) VALUES (1641024005000, 2);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c0) VALUES (1641024010000, NULL);")

        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL AND c0 IN (-1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL AND c0 IN (-1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL AND c0 IN (-1, 1);")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL AND c0 IN (2, -1, 1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL AND c0 NOT IN (-1);")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL AND c0 NOT IN (-1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL AND c0 NOT IN (3);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL AND c0 NOT IN (-1, 1);")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL AND c0 NOT IN (2, -1, 1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE (c0 IS NULL AND c0 IN (-1)) or c0 in(1)")
        tdSql.checkRows(1)
        
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL OR c0 IN (-1);")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL OR c0 IN (-1);")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL OR c0 IN (-1, 1);")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL OR c0 IN (2, -1, 1);")
        tdSql.checkRows(3)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL OR c0 NOT IN (-1);")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL OR c0 NOT IN (-1);")
        tdSql.checkRows(3)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL OR c0 NOT IN (3);")
        tdSql.checkRows(3)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL OR c0 NOT IN (-1, 1);")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL OR c0 NOT IN (-1);")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NULL OR c0 NOT IN (2, -1, 1);")
        tdSql.checkRows(1)
        
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE ((c0 is NULL) AND  (c0 in (-1)) )")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE ((c0 in (-1)) AND (c0 is NULL) )")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE ((c0 in (-1)) AND (c0 is NULL) ) OR c0 in(1)")
        tdSql.checkRows(1)
        
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IN (-1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IS NOT NULL;")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IN (-1) or c0 in(1);")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IN (1) or c0 in(-1);")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IN (-1) or c0 in(-1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IN (-1) and c0 in(1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IN (1) and c0 in(-1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM {self.dbname}.t1 WHERE c0 IN (-1) and c0 in(-1);")
        tdSql.checkRows(0)

    def ts5760(self):
        tdSql.execute(f"create database if not exists {self.dbname}")
        
        tdSql.execute(f"DROP TABLE IF EXISTS {self.dbname}.t1;")
        tdSql.execute(f"CREATE TABLE {self.dbname}.t1( time TIMESTAMP, c0 INT);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c0) VALUES (1641024000000, 1);")
        
        tdSql.query(f"SELECT time, c0 FROM {self.dbname}.t1 WHERE (time - c0) > 0;")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT time, c0 FROM {self.dbname}.t1 WHERE (time + c0) > 0;")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT time, c0 FROM {self.dbname}.t1 WHERE (-(- c0)) > 0;")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT time, c0 FROM {self.dbname}.t1 WHERE -(- c0) > 0;")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT time, c0 FROM {self.dbname}.t1 WHERE -(- c0) < 0;")
        tdSql.checkRows(0)
        
        tdSql.query(f"SELECT time, c0 FROM {self.dbname}.t1 WHERE -(- c0) = 0;")
        tdSql.checkRows(0)
        
        tdSql.query(f"SELECT time, c0 FROM {self.dbname}.t1 WHERE (- c0) > 0;")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT time, c0 FROM {self.dbname}.t1 WHERE (- c0) < 0;")
        tdSql.checkRows(1)
        
        tdSql.query(f"SELECT time, c0 FROM {self.dbname}.t1 WHERE (time + (- c0)) > 0;")
        tdSql.checkRows(1)
        
        tdSql.query(f"SELECT time, c0 FROM {self.dbname}.t1 WHERE (time + (- c0)) > 0;")
        tdSql.checkRows(1)
        
        tdSql.query(f"SELECT time, c0 FROM {self.dbname}.t1 WHERE (time - (- (- c0)) ) > 0;")
        tdSql.checkRows(1)
        
        tdSql.query(f"SELECT time, c0 FROM {self.dbname}.t1 WHERE (time + (-(- c0))) > 0;")
        tdSql.checkRows(1)

    def ts5758(self):
        tdSql.execute(f"create database if not exists {self.dbname}")
        
        tdSql.execute(f"DROP TABLE IF EXISTS {self.dbname}.t1;")
        tdSql.execute(f"CREATE TABLE {self.dbname}.t1( time TIMESTAMP, c1 BIGINT);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c1) VALUES (1641024000000, 0);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c1) VALUES (1641024000001, 1);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c1) VALUES (1641024000002, 2);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c1) VALUES (1641024000003, 3);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c1) VALUES (1641024000004, 4);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c1) VALUES (1641024000005, 5);")
        
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time IN (1) AND time BETWEEN (1741024000000) AND (1741024000000);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time BETWEEN (1741024000000) AND (1741024000000) AND time IN (1);")
        tdSql.checkRows(0)    
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time IN (1) and time BETWEEN (1741024000000) AND (1741024000000) AND time IN (1);")
        tdSql.checkRows(0)           
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time IN (1641024000000) and time BETWEEN  (1741024000000) AND (1741024000000) AND time IN (1);")
        tdSql.checkRows(0)     
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time IN (1) AND time BETWEEN (1641024000000) AND (1741024000000);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time IN (1641024000001) AND time BETWEEN (1641024000000) AND (1741024000000);")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time IN (1641024000001, 1641024000002, 1641024000003) AND time BETWEEN (1641024000000) AND (1741024000000);")
        tdSql.checkRows(3)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time IN (1641024000001, 1641024000002, 1641024000005) AND time BETWEEN (1641024000000) AND (1641024000004);")
        tdSql.checkRows(2)
        
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time IN (1) OR time = 1741024000000;")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time = 1741024000000 OR time IN (1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time IN (1, 2, 3) OR time BETWEEN (1641024000000) and (1741024000000);")
        tdSql.checkRows(6)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time IN (1, 2, 3) OR time = 1641024000000;")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time = 1641024000001 OR time BETWEEN (1641024000000) and (1641024000002);")
        tdSql.checkRows(3)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time = 1641024000004 OR time BETWEEN (1641024000000) and (1641024000002);")
        tdSql.checkRows(4)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time = 1641024000001 OR time = 1741024000000;")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time IN (1641024000001, 1641024000002) OR time = 1741024000000;")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE time IN (1641024000001, 1641024000002) OR time BETWEEN (1641024000000) and (1741024000000);")
        tdSql.checkRows(6)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE (time = 1641024000004 OR time BETWEEN (1641024000000) and (1641024000002)) and time in(1);")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE (time = 1641024000004 OR time BETWEEN (1641024000000) and (1641024000002)) and time in(1641024000004, 1641024000002);")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE (time = 1641024000004 OR time BETWEEN (1641024000000) and (1641024000002)) or time in(1);")
        tdSql.checkRows(4)

    def ts5759(self):
        tdSql.execute(f"create database if not exists {self.dbname}")
        
        tdSql.execute(f"DROP TABLE IF EXISTS {self.dbname}.t1;")
        tdSql.execute(f"CREATE TABLE {self.dbname}.t1( time TIMESTAMP, c1 BIGINT);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c1) VALUES (1641024000000, 0);")
        
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE (time BETWEEN 1641024000000 AND 1641024000001)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE (time BETWEEN 1641024000000 AND 1641024000001) OR (1 < 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE (time BETWEEN 1641024000000 AND 1641024000001) OR (3 < 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE (time BETWEEN 1641024000000 AND 1641024000001) and (1 < 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE (time BETWEEN 1641024000000 AND 1641024000001) OR (1 > 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE (time BETWEEN 1641024000000 AND 1641024000001) and (1 > 2)")
        tdSql.checkRows(0)
        
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c1) VALUES (1641024000001, 1);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1(time, c1) VALUES (1641024000002, 2);")
        
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE (time BETWEEN 1641024000000 AND 1641024000001) OR (1 < 2)")
        tdSql.checkRows(3)
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE (time BETWEEN 1641024000000 AND 1641024000001) OR (1 > 2)")
        tdSql.checkRows(2)      
        tdSql.query(f"SELECT c1 FROM {self.dbname}.t1 WHERE (time BETWEEN 1641024000000 AND 1641024000001) and (1 < 2)")
        tdSql.checkRows(2)

    def operOnTime(self):
        tdSql.execute(f"create database if not exists {self.dbname}")
        
        tdSql.execute(f"DROP TABLE IF EXISTS {self.dbname}.t1;")
        tdSql.execute(f"CREATE TABLE {self.dbname}.t1( ts TIMESTAMP, c0 INT, c1 INT UNSIGNED, \
            c2 BIGINT, c3 BIGINT UNSIGNED, c4 SMALLINT, c5 SMALLINT UNSIGNED, c6 TINYINT, c7 TINYINT UNSIGNED);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t1 VALUES (1641024000001, 1, 1, 1, 1, 1, 1, 1, 1);")

        columns = ["c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"]
        for col in columns:
            tdLog.debug(f"oper on time test, {col} start ...")
            tdSql.query(f"SELECT ts, ts+1, ts+{col}, ts+(-{col}) FROM {self.dbname}.t1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1641024000001)
            tdSql.checkData(0, 1, 1641024000002)
            tdSql.checkData(0, 2, 1641024000002)
            tdSql.checkData(0, 3, 1641024000000)

            tdSql.query(f"SELECT ts, ts+1, ts+{col}, ts+(-{col}) FROM {self.dbname}.t1 where (ts-(-{col})) > 0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1641024000001)
            tdSql.checkData(0, 1, 1641024000002)
            tdSql.checkData(0, 2, 1641024000002)
            tdSql.checkData(0, 3, 1641024000000)

            tdSql.query(f"SELECT ts, ts-1, ts-{col}, ts-(-{col}) FROM {self.dbname}.t1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1641024000001)
            tdSql.checkData(0, 1, 1641024000000)
            tdSql.checkData(0, 2, 1641024000000)
            tdSql.checkData(0, 3, 1641024000002)

        tdSql.query(f"SELECT ts, ts+true, ts-true, ts-false, ts+false FROM {self.dbname}.t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1641024000001)
        tdSql.checkData(0, 1, 1641024000002)
        tdSql.checkData(0, 2, 1641024000000)
        tdSql.checkData(0, 3, 1641024000001)
        tdSql.checkData(0, 4, 1641024000001)
        
        tdSql.execute(f"DROP TABLE IF EXISTS {self.dbname}.t2;")
        tdSql.execute(f"CREATE TABLE {self.dbname}.t2( ts TIMESTAMP, c1 float, c2 double);")
        tdSql.execute(f"INSERT INTO {self.dbname}.t2(ts, c1, c2) VALUES (1641024000001, 1.0, 1.0);")
        
        columns = ["c1", "c2"]
        for col in columns:
            tdSql.query(f"SELECT ts, ts+{col}, ts+(-{col}), ts-{col}, ts-(-{col}) FROM {self.dbname}.t2")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1641024000001)
            tdSql.checkData(0, 1, 1641024000002)
            tdSql.checkData(0, 2, 1641024000000)
            tdSql.checkData(0, 3, 1641024000000)
            tdSql.checkData(0, 4, 1641024000002)

            tdSql.query(f"SELECT ts, ts+{col}, ts+(-{col}), ts-{col}, ts-(-{col}) FROM {self.dbname}.t2 where (ts-(-{col})) > 0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1641024000001)
            tdSql.checkData(0, 1, 1641024000002)
            tdSql.checkData(0, 2, 1641024000000)
            tdSql.checkData(0, 3, 1641024000000)
            tdSql.checkData(0, 4, 1641024000002)
            
            tdSql.query(f"SELECT ts, cast(ts+{col} as bigint), cast(ts+(-{col}) as bigint), cast(ts-{col} as bigint),\
                cast(ts-(-{col}) as bigint) FROM {self.dbname}.t2")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1641024000001)
            tdSql.checkData(0, 1, 1641024000002)
            tdSql.checkData(0, 2, 1641024000000)
            tdSql.checkData(0, 3, 1641024000000)
            tdSql.checkData(0, 4, 1641024000002)
 
        tdSql.query(f"SELECT sum(ts + c1), sum(ts+c2) from {self.dbname}.t2")
        tdSql.checkData(0, 0, 1641024000002)
        tdSql.checkData(0, 1, 1641024000002)
        tdSql.query(f"SELECT sum(ts * c1), sum(ts*c2) from {self.dbname}.t2")
        tdSql.checkData(0, 0, 1641024000001)
        tdSql.checkData(0, 1, 1641024000001)
        tdSql.query(f"SELECT sum(ts / c1), sum(ts/c2) from {self.dbname}.t2")
        tdSql.checkData(0, 0, 1641024000001)
        tdSql.checkData(0, 1, 1641024000001)
        tdSql.execute(f"INSERT INTO {self.dbname}.t2(ts, c1, c2) VALUES (1641024000002, 2.0, 2.0);")  
        tdSql.query(f"SELECT sum(ts + c1), sum(ts+c2) from {self.dbname}.t2")
        tdSql.checkData(0, 0, 3282048000006)
        tdSql.checkData(0, 1, 3282048000006)
        tdSql.query(f"SELECT sum(ts - c1), sum(ts-c2) from {self.dbname}.t2")
        tdSql.checkData(0, 0, 3282048000000)
        tdSql.checkData(0, 1, 3282048000000)
        tdSql.query(f"SELECT sum(ts * c1), sum(ts*c2) from {self.dbname}.t2")
        tdSql.checkData(0, 0, 4923072000005)
        tdSql.checkData(0, 1, 4923072000005)
        tdSql.query(f"SELECT ts / c1, ts/c2 from {self.dbname}.t2 order by ts")
        tdSql.checkData(0, 0, 1641024000001)
        tdSql.checkData(0, 1, 1641024000001)
        tdSql.checkData(1, 0,  820512000001)
        tdSql.checkData(1, 1,  820512000001)
        tdSql.query(f"SELECT sum(ts / c1), sum(ts/c2) from {self.dbname}.t2")
        tdSql.checkData(0, 0, 2461536000002)
        tdSql.checkData(0, 1, 2461536000002)
        
        # data overflow
        tdSql.query(f"SELECT ts + 9223372036854775807 from {self.dbname}.t2 order by ts")
        tdSql.query(f"SELECT ts - 9223372036854775808 from {self.dbname}.t2 order by ts")
        
        tdSql.query(f"SELECT ts + 8223372036854775807 from {self.dbname}.t2 order by ts")
        tdSql.query(f"SELECT ts - 8223372036854775808 from {self.dbname}.t2 order by ts")
               
    def test_operator(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """

        dbname = "db"
        tdSql.prepare()
        tdSql.execute(f"create database if not exists {self.dbname}")
        
        self.ts5757()
        self.ts5760()
        self.ts5758()
        self.ts5759()
        self.operOnTime()

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
