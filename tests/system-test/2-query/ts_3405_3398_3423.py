from util.log import *
from util.sql import *
from util.cases import *
from util.sqlset import *
import datetime
import random

class TDTestCase:
    """The test cases are for TS_3398, TS_3405, TS_3423
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)

    def run(self):
        """This test case is used to verify the query performance for the merge scans process of
        multiple tables join
        """
        tdLog.info("Start the test case for ts_3405")
        # test case for https://jira.taosdata.com:18080/browse/TS-3405:
        # create db
        tdSql.execute("CREATE DATABASE IF NOT EXISTS statistics2 REPLICA {} DURATION 14400m KEEP 5256000m,5256000m,5256000m PRECISION 'ms' MINROWS 100 MAXROWS 4096 COMP 2;".format(self.replicaVar))
        tdSql.execute("use statistics2;")

        # create stable
        tdSql.execute("CREATE STABLE IF NOT EXISTS statistics2.`pg`(`day` timestamp,`lt_3` int,`c3_3` int,`c6_3` int,`c9_3` int,`c12_3` int,`c15_3` int,`c18_3` int,`c21_3` int,`c24_3` int,`c27_3` int,`ge_3` int) TAGS(`vin` binary(32));")
        tdSql.execute("CREATE STABLE IF NOT EXISTS statistics2.`b`(`day` timestamp, `month` int) TAGS(`group_path` binary(32),`vin` binary(32));")
        tdSql.execute("CREATE STABLE IF NOT EXISTS statistics2.`g`(`day` timestamp,`run_state` tinyint) TAGS(`vin` binary(32));")

        # insert the data to table
        times = 10
        insertRows = 3000
        pg_sql = "insert into d1001 using statistics2.`pg` tags('test') values"
        b_sql  = "insert into d2001 using statistics2.`b` tags('1#%', 'test') values"
        g_sql  = "insert into d3001 using statistics2.`g` tags('test') values"
        for t in range(times):
            for i in range(t * insertRows, t * insertRows + insertRows):
                ts = datetime.datetime.strptime('2023-05-01 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f') + datetime.timedelta(seconds=i)
                pg_sql += " ('{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})".format(ts, i, i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9)
                b_sql += " ('{}', {})".format(ts, 5)
                g_sql += " ('{}', {})".format(ts, 1)

            tdSql.execute(pg_sql)
            tdSql.execute(b_sql)
            tdSql.execute(g_sql)
            # reset the sql statements
            pg_sql = "insert into d1001 using statistics2.`pg` tags('test') values"
            b_sql  = "insert into d2001 using statistics2.`b` tags('1#%', 'test') values"
            g_sql  = "insert into d3001 using statistics2.`g` tags('test') values"
        tdLog.info("insert %d rows" % (insertRows * times))

        # execute the sql statements
        ret = tdSql.query("SELECT sum(pg.lt_3) es1,sum(pg.c3_3) es2,sum(pg.c6_3) es3,sum(pg.c9_3) es4,sum(pg.c12_3) es5,sum(pg.c15_3) es6,sum(pg.c18_3) es7,sum(pg.c21_3) es8,sum(pg.c24_3) es9,sum(pg.c27_3) es10,sum(pg.ge_3) es11 FROM statistics2.b b,statistics2.pg pg,statistics2.g g WHERE b.`day` = pg.`day` AND b.`day` = g.`day` AND b.vin = pg.vin AND b.vin = g.vin AND b.vin IS NOT NULL AND b.`group_path` LIKE '1#%';")
        # check the first query result
        if (449985000, 449985000, 450015000, 450045000, 450075000, 450105000, 450135000, 450165000, 450195000, 450225000, 450255000) in tdSql.queryResult:
            tdLog.info("first query result is correct")
        else:
            tdLog.info("first query result is wrong with res: {}".format(str(tdSql.queryResult)))

        ret = tdSql.query("SELECT sum(pg.lt_3) es1, sum(pg.c3_3) es2, sum(pg.c6_3) es3, sum(pg.c9_3) es4, sum(pg.c12_3) es5, sum(pg.c15_3) es6, sum(pg.c18_3) es7, sum(pg.c21_3) es8, sum(pg.c24_3) es9, sum(pg.c27_3) es10, sum(pg.ge_3) es11 FROM (select * from statistics2.b order by day,month) b, (select * from statistics2.pg order by day,lt_3 ) pg, (select * from statistics2.g order by day,run_state) g WHERE b.`day` = pg.`day` AND b.`day` = g.`day` AND b.vin = pg.vin AND b.vin = g.vin AND b.vin IS NOT NULL;")
        # check the second query result
        if (449985000, 449985000, 450015000, 450045000, 450075000, 450105000, 450135000, 450165000, 450195000, 450225000, 450255000) in tdSql.queryResult:
            tdLog.info("second query result is correct")
        else:
            tdLog.info("second query result is wrong with res: {}".format(str(tdSql.queryResult)))
        tdLog.info("Finish the test case for ts_3405 successfully")

        """This test case is used to verify the aliasName of Node structure is not truncated
        when sum clause is more than 65 bits.
        """
        # test case for https://jira.taosdata.com:18080/browse/TS-3398:
        # create db
        tdLog.info("Start the test case for ts_3398")
        tdSql.execute("CREATE DATABASE IF NOT EXISTS statistics1 REPLICA {} DURATION 14400m KEEP 5256000m,5256000m,5256000m PRECISION 'ms' MINROWS 100 MAXROWS 4096 COMP 2;".format(self.replicaVar))
        tdSql.execute("use statistics1;")

        # create stable
        tdSql.execute("CREATE STABLE IF NOT EXISTS statistics1.`g`(`day` timestamp,`run_state` tinyint) TAGS(`vin` binary(32));")
        tdSql.execute("CREATE STABLE IF NOT EXISTS statistics1.`b`(`day` timestamp, `total_heart` int) TAGS(`vin` binary(32));")
        tdSql.execute("CREATE STABLE IF NOT EXISTS statistics1.`tg`(`day` timestamp,`lt_4177` int,`f30_4177` int, `f35_4177` int) TAGS(`vin` binary(32));")

        # insert the data to table
        tdSql.execute("insert into d1001 using statistics1.`g` tags('NJHYNBSAS0000061') values (%s, %d)" % ("'2023-05-01'", 99))
        tdSql.execute("insert into d2001 using statistics1.`b` tags('NJHYNBSAS0000061') values (%s, %d)" % ("'2023-05-01'", 99))
        tdSql.execute("insert into d3001 using statistics1.`tg` tags('NJHYNBSAS0000061') values (%s, %d, %d, %d)" % ("'2023-05-01'", 99, 99, 99))

        # execute the sql statements
        tdSql.query("SELECT b.`day` `day`,sum(CASE WHEN tg.lt_4177 IS NULL THEN 0 ELSE tg.lt_4177 END \
            + CASE WHEN tg.f35_4177 IS NULL THEN 0 ELSE tg.f35_4177 END) / 3600 es0,sum(CASE WHEN tg.lt_4177 \
                IS NULL THEN 0 ELSE tg.lt_4177 END + CASE WHEN tg.f35_4177 IS NULL THEN 0 ELSE tg.f35_4177 \
                    END + CASE WHEN tg.f30_4177 IS NULL THEN 0 ELSE tg.f30_4177 END) / 3600 es1 FROM \
                        statistics1.b b,statistics1.tg tg,statistics1.g g WHERE b.`day` = tg.`day` AND g.`day` = b.`day` \
                            AND b.vin = tg.vin AND b.vin = g.vin AND b.`day` BETWEEN '2023-05-01' AND '2023-05-05' \
                                AND b.vin = 'NJHYNBSAS0000061' AND g.vin IS NOT NULL AND b.vin IS NOT NULL AND tg.vin IS NOT NULL \
                                    GROUP BY b.`day`;")
        # check the result
        if 0.055 in tdSql.queryResult[0] and 0.0825 in tdSql.queryResult[0]:
            tdLog.info("query result is correct")
        else:
            tdLog.info("query result is wrong")
        tdLog.info("Finish the test case for ts_3398 successfully")

        """This test case is used to verify last(*) query result is correct when the data
        is group by tag for stable
        """
        # test case for https://jira.taosdata.com:18080/browse/TS-3423:
        # create db
        tdLog.info("Start the test case for ts_3423")
        tdSql.execute("CREATE DATABASE IF NOT EXISTS ts_3423 REPLICA {} DURATION 14400m KEEP 5256000m,5256000m,5256000m PRECISION 'ms' MINROWS 100 MAXROWS 4096 COMP 2;".format(self.replicaVar))
        tdSql.execute("use ts_3423;")

        # create stable
        tdSql.execute("CREATE STABLE IF NOT EXISTS ts_3423.`st_last`(`ts` timestamp,`n1` int,`n2` float) TAGS(`groupname` binary(32));")

        # insert the data to table
        insertRows = 10
        child_table_num = 10
        for i in range(insertRows):
            ts = datetime.datetime.strptime('2023-05-01 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f') + datetime.timedelta(seconds=i)
            for j in range(child_table_num):
                tdSql.execute("insert into {} using ts_3423.`st_last` tags('{}') values ('{}', {}, {})".format("d" + str(j), "group" + str(j), str(ts), str(i+1), random.random()))
        tdLog.info("insert %d rows for every child table" % (insertRows))

        # cache model list
        cache_model = ["none", "last_row", "last_value", "both"]
        query_res = []
        
        # execute the sql statements first
        tdSql.query("select `cachemodel` from information_schema.ins_databases where name='ts_3423'")
        current_cache_model = tdSql.queryResult[0][0]
        tdLog.info("query on cache model {}".format(current_cache_model))
        tdSql.query("select last(*) from st_last group by groupname;")
        # save the results
        query_res.append(len(tdSql.queryResult))
        # remove the current cache model
        cache_model.remove(current_cache_model)
        
        for item in cache_model:
            tdSql.execute("alter database ts_3423 cachemodel '{}';".format(item))
            # execute the sql statements
            tdSql.query("select last(*) from st_last group by groupname;")
            tdLog.info("query on cache model {}".format(item))
            query_res.append(len(tdSql.queryResult))
        # check the result
        res = True if query_res.count(child_table_num) == 4 else False
        if res:
            tdLog.info("query result is correct and same among different cache model")
        else:
            tdLog.info("query result is wrong")
        tdLog.info("Finish the test case for ts_3423 successfully")

    def stop(self):
        # clear the db
        tdSql.execute("drop database if exists statistics1;")
        tdSql.execute("drop database if exists statistics2;")
        tdSql.execute("drop database if exists ts_3423;")
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
