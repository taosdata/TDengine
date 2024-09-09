# from asyncio.windows_events import NULL
from util.log import *
from util.sql import *
from util.cases import *

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")



    def run(self):
        tdSql.query("select count(*) from performance_schema.perf_queries limit 1;")
        tdSql.checkData(0, 0, "None")
        tdSql.info("-----111-----performance_schema.perf_queries ---- success")


        tdSql.query("select count(*) from  information_schema.perf_queries limit 1;")
        tdSql.checkData(0, 0, "None")
        tdSql.info("-----222-----information_schema.perf_queries ---- success")


    def stop(self):
        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")