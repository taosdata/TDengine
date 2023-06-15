from util.log import *
from util.sql import *
from util.cases import *
from util.sqlset import *
import datetime

class TDTestCase:
    """This test case is used to verify the query performance for the merge scans process of
    multiple tables join
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), False)

    def run(self):
        # test case for https://jira.taosdata.com:18080/browse/TS-3405:
        # create db
        ret = tdSql.execute("CREATE DATABASE IF NOT EXISTS statistics2 REPLICA {} DURATION 14400m KEEP 5256000m,5256000m,5256000m PRECISION 'ms' MINROWS 100 MAXROWS 4096 COMP 2;".format(self.replicaVar))
        tdSql.execute("use statistics2;")

        # create stable
        ret = tdSql.execute("CREATE STABLE IF NOT EXISTS statistics2.`pg`(`day` timestamp,`lt_3` int,`c3_3` int,`c6_3` int,`c9_3` int,`c12_3` int,`c15_3` int,`c18_3` int,`c21_3` int,`c24_3` int,`c27_3` int,`ge_3` int) TAGS(`vin` binary(32));")
        ret = tdSql.execute("CREATE STABLE IF NOT EXISTS statistics2.`b`(`day` timestamp, `month` int) TAGS(`group_path` binary(32),`vin` binary(32));")
        ret = tdSql.execute("CREATE STABLE IF NOT EXISTS statistics2.`g`(`day` timestamp,`run_state` tinyint) TAGS(`vin` binary(32));")

        # insert the data to table
        insertRows = 30000
        for i in range(insertRows):
            ts = datetime.datetime.strptime('2023-05-01 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f') + datetime.timedelta(seconds=i)
            tdSql.execute("insert into d1001 using statistics2.`pg` tags('test') values ('{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}) \
                d2001 using statistics2.`b` tags('1#%', 'test') values ('{}', {}) \
                d3001 using statistics2.`g` tags('test') values ('{}', {});".format(ts, i, i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9, ts, 5, ts, 1))
        tdLog.info("insert %d rows" % (insertRows))

        # execute the sql statements
        ret = tdSql.query("SELECT sum(pg.lt_3) es1,sum(pg.c3_3) es2,sum(pg.c6_3) es3,sum(pg.c9_3) es4,sum(pg.c12_3) es5,sum(pg.c15_3) es6,sum(pg.c18_3) es7,sum(pg.c21_3) es8,sum(pg.c24_3) es9,sum(pg.c27_3) es10,sum(pg.ge_3) es11 FROM statistics2.b b,statistics2.pg pg,statistics2.g g WHERE b.`day` = pg.`day` AND b.`day` = g.`day` AND b.vin = pg.vin AND b.vin = g.vin AND b.vin IS NOT NULL AND b.`group_path` LIKE '1#%';")
        # check the first query result
        if (449985000, 449985000, 450015000, 450045000, 450075000, 450105000, 450135000, 450165000, 450195000, 450225000, 450255000) in tdSql.queryResult:
            tdLog.info("first query result is correct")
        else:
            tdLog.info("first query result is wrong")

        ret = tdSql.query("SELECT sum(pg.lt_3) es1, sum(pg.c3_3) es2, sum(pg.c6_3) es3, sum(pg.c9_3) es4, sum(pg.c12_3) es5, sum(pg.c15_3) es6, sum(pg.c18_3) es7, sum(pg.c21_3) es8, sum(pg.c24_3) es9, sum(pg.c27_3) es10, sum(pg.ge_3) es11 FROM (select * from statistics2.b order by day,month) b, (select * from statistics2.pg order by day,lt_3 ) pg, (select * from statistics2.g order by day,run_state) g WHERE b.`day` = pg.`day` AND b.`day` = g.`day` AND b.vin = pg.vin AND b.vin = g.vin AND b.vin IS NOT NULL;")
        # check the second query result
        if (449985000, 449985000, 450015000, 450045000, 450075000, 450105000, 450135000, 450165000, 450195000, 450225000, 450255000) in tdSql.queryResult:
            tdLog.info("second query result is correct")
        else:
            tdLog.info("second query result is wrong")


    def stop(self):
        # clear the db
        tdSql.execute("drop database if exists statistics2;")
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
