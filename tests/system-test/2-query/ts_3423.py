from util.log import *
from util.sql import *
from util.cases import *
from util.sqlset import *
import datetime
import random

class TDTestCase:
    """This test case is used to verify last(*) query result is correct when the data
    is group by tag for stable
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), False)

    def run(self):
        # test case for https://jira.taosdata.com:18080/browse/TS-3423:
        # create db
        ret = tdSql.execute("CREATE DATABASE IF NOT EXISTS ts_3423 REPLICA {} DURATION 14400m KEEP 5256000m,5256000m,5256000m PRECISION 'ms' MINROWS 100 MAXROWS 4096 COMP 2;".format(self.replicaVar))
        tdSql.execute("use ts_3423;")

        # create stable
        ret = tdSql.execute("CREATE STABLE IF NOT EXISTS ts_3423.`st_last`(`ts` timestamp,`n1` int,`n2` float) TAGS(`groupname` binary(32));")

        # insert the data to table
        insertRows = 10
        child_table_num = 10
        for i in range(insertRows):
            ts = datetime.datetime.strptime('2023-05-01 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f') + datetime.timedelta(seconds=i)
            for j in range(child_table_num):
                ret = tdSql.execute("insert into {} using ts_3423.`st_last` tags('{}') values ('{}', {}, {})".format("d" + str(j), "group" + str(j), str(ts), str(i+1), random.random()))
        tdLog.info("insert %d rows for every child table" % (insertRows))

        # cache model list
        cache_model = ["none", "last_row", "last_value", "both"]
        query_res = []
        
        # execute the sql statements first
        ret = tdSql.query("select `cachemodel` from information_schema.ins_databases where name='ts_3423'")
        current_cache_model = tdSql.queryResult[0][0]
        tdLog.info("query on cache model {}".format(current_cache_model))
        ret = tdSql.query("select last(*) from st_last group by groupname;")
        # save the results
        query_res.append(len(tdSql.queryResult))
        # remove the current cache model
        cache_model.remove(current_cache_model)
        
        for item in cache_model:
            tdSql.execute("alter database ts_3423 cachemodel '{}';".format(item))
            # execute the sql statements
            ret = tdSql.query("select last(*) from st_last group by groupname;")
            tdLog.info("query on cache model {}".format(item))
            query_res.append(len(tdSql.queryResult))
        # check the result
        res = True if query_res.count(child_table_num) == 4 else False
        if res:
            tdLog.info("query result is correct and same among different cache model")
        else:
            tdLog.info("query result is wrong")

    def stop(self):
        # clear the db
        tdSql.execute("drop database if exists ts_3423;")
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
