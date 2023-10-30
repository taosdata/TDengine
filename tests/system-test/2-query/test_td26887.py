from util.log import *
from util.sql import *
from util.cases import *
from util.gettime import *


class TDTestCase:
    """This test case is used to verify the same column name of aggregate function in the same query when 
    the keepColumnName is set to 1.
    """
    updatecfgDict = {'keepColumnName': 1}
    
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        
        self.db_name = "db"
        self.st_name = "st"
        self.ct_name = "ct"
        
        # create database
        tdSql.execute(f"create database if not exists {self.db_name};")
        tdSql.execute(f"use {self.db_name};")
        
        # create table
        tdSql.execute(f"create table st(ts timestamp, c1 int) tags(groupid int);")
        
        # insert data into child table
        tdSql.execute(f"insert into {self.ct_name} using st tags(1) values(now, 1)(now+1s, 2)(now+2s, 3);")

    def run(self):
        try:
            # query and check data
            tdSql.query(f"select first(ts) from {self.st_name};")
            res = tdSql.queryResult
            first_ts = str(res[0][0])
            
            tdSql.query(f"select last(ts) from {self.st_name};")
            res = tdSql.queryResult
            last_ts = str(res[0][0])

            tdSql.query(f"select first(ts), last(ts) from {self.st_name};")
            res = tdSql.queryResult
            res_value = [str(item) for item in res[0]]
            assert(len(res[0]) == 2 and first_ts in res_value and last_ts in res_value)

            tdSql.query(f"select count(*) from (select first(ts), last(ts) from {self.st_name});")
            res = tdSql.queryResult
            assert(len(res[0]) == 1)
        except Exception as ex:
            tdLog.exit("Failed to execute the query with error: {}".format(str(ex)))
    
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
