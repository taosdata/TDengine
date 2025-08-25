from new_test_framework.utils import tdLog, tdSql

import socket
import taos
import time

class TestCompact:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
    
    def test_compact(self):
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

        tdSql.query("CREATE DATABASE power KEEP 365 DURATION 10 BUFFER 16 WAL_LEVEL 1 vgroups 1 replica 1;")

        tdSql.query("CREATE DATABASE power1 KEEP 365 DURATION 10 BUFFER 16 WAL_LEVEL 1 vgroups 1 replica 1;")

        #first
        tdSql.query("compact database power;")

        tdLog.info("compact id:%d"%tdSql.queryResult[0][1])

        tdSql.query("show compact %d;"%tdSql.queryResult[0][1])

        tdLog.info("detail:%d"%tdSql.queryRows)

        #second
        tdSql.query("compact database power1;")

        tdLog.info("compact id:%d"%tdSql.queryResult[0][1])

        tdSql.query("show compact %d;"%tdSql.queryResult[0][1])

        tdLog.info("detail:%d"%tdSql.queryRows)


        #kill
        tdSql.query("show compacts;")
        number1 = tdSql.queryResult[0][0]
        number2 = tdSql.queryResult[1][0]

        #first 
        tdLog.info("kill compact %d;"%number1)  
        tdSql.query("kill compact %d;"%number1)

        #second
        tdLog.info("kill compact %d;"%number2) 
        tdSql.query("kill compact %d;"%number2)


        #show
        count = 0
        tdLog.info("query progress")
        while count < 50:
            tdSql.query("show compact %d;"%number1)

            row1 = tdSql.queryRows

            tdSql.query("show compact %d;"%number2)

            row2 = tdSql.queryRows

            tdLog.info("compact%d:detail count:%d"%(number1, row1))
            tdLog.info("compact%d:detail count:%d"%(number2, row2))

            if row1 == 0 and row2 == 0 :
                break

            time.sleep(1)

            count +=1
            #tdLog.info("loop%d"%count)

        if row1 != 0 or row2 != 0:
            tdLog.exit("compact failed")
        
        tdLog.success(f"{__file__} successfully executed")

