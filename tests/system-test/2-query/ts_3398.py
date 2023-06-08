from util.log import *
from util.sql import *
from util.cases import *
from util.sqlset import *
import datetime


class TDTestCase:
    """This test case is used to verify the aliasName of Node structure is not truncated
    when sum clause is more than 65 bits.
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), False)

    def run(self):
        # test case for https://jira.taosdata.com:18080/browse/TS-3405:
        # create db
        ret = tdSql.execute("CREATE DATABASE IF NOT EXISTS statistics1 REPLICA {} DURATION 14400m KEEP 5256000m,5256000m,5256000m PRECISION 'ms' MINROWS 100 MAXROWS 4096 COMP 2;".format(self.replicaVar))
        tdSql.execute("use statistics1;")

        # create stable
        ret = tdSql.execute("CREATE STABLE IF NOT EXISTS statistics1.`g`(`day` timestamp,`run_state` tinyint) TAGS(`vin` binary(32));")
        ret = tdSql.execute("CREATE STABLE IF NOT EXISTS statistics1.`b`(`day` timestamp, `total_heart` int) TAGS(`vin` binary(32));")
        ret = tdSql.execute("CREATE STABLE IF NOT EXISTS statistics1.`tg`(`day` timestamp,`lt_4177` int,`f30_4177` int, `f35_4177` int) TAGS(`vin` binary(32));")

        # insert the data to table
        ret = tdSql.execute("insert into d1001 using statistics1.`g` tags('NJHYNBSAS0000061') values (%s, %d)" % ("'2023-05-01'", 99))
        ret = tdSql.execute("insert into d2001 using statistics1.`b` tags('NJHYNBSAS0000061') values (%s, %d)" % ("'2023-05-01'", 99))
        ret = tdSql.execute("insert into d3001 using statistics1.`tg` tags('NJHYNBSAS0000061') values (%s, %d, %d, %d)" % ("'2023-05-01'", 99, 99, 99))

        # execute the sql statements
        ret = tdSql.query("SELECT b.`day` `day`,sum(CASE WHEN tg.lt_4177 IS NULL THEN 0 ELSE tg.lt_4177 END \
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

    def stop(self):
        # clear the db
        tdSql.execute("drop database if exists statistics1;")
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
