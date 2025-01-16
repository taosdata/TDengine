
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
from util.dnodes import *
from util.common import *


class TDTestCase:

    """
    Here is the class description for the whole file cases
    """

    # add the configuration of the client and server here
    clientCfgDict = {'debugFlag': 131}
    updatecfgDict = {
        "debugFlag"        : "131",
        "queryBufferSize"  : 10240,
        'clientCfg'        : clientCfgDict
    }

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.replicaVar = int(replicaVar)


    def test_function(self):   # case function should be named start with test_
        """
        Here is the function description for single test:
        Test case for custom function
        """
        tdLog.info(f"Test case test custom function")
        # excute the sql 
        tdSql.execute(f"create database db_test_function")
        tdSql.execute(f"create table db_test_function.stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned);")
        # qury the result and return the result
        tdSql.query(f"show databases")
        # print result and check the result
        database_info = tdLog.info(f"{tdSql.queryResult}")
        tdSql.checkRows(3)
        tdSql.checkData(2,0,"db_test_function")


    def run(self):
        self.test_function()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
