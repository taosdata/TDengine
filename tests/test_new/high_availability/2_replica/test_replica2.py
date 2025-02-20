###################################################################
 #		   Copyright (c) 2016 by TAOS Technologies, Inc.
 #				     All rights reserved.
 #
 #  This file is proprietary and confidential to TAOS Technologies.
 #  No part of this file may be reproduced, stored, transmitted, 
 #  disclosed or used in any form or by any means other than as 
 #  expressly provided by the written permission from Jianhui Tao
 #
###################################################################
from util.cases import *
from util.sql import *
from util.dnodes import *
from util.log import *

class TestReplica2:
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to init {__file__}")
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), logSql)

    def test_replica2(self):
        """测试双副本

        创建双副本db，查看vgroups，停止dnode2和3，校验assigned

        Since: v3.3.0.0

        Labels: 3nodes, replica

        Jira: TD-12345,TS-1234

        History:
            - 2024-2-6 Feng Chao Created

        """
        self.run()
    def run(self):
        tdSql.execute('CREATE DATABASE db vgroups 1 replica 2;')

        time.sleep(1)

        tdSql.query("show db.vgroups;")

        if(tdSql.queryResult[0][4] == "follower") and (tdSql.queryResult[0][6] == "leader"):
            tdLog.info("stop dnode2")
            sc.dnodeStop(2)

        if(tdSql.queryResult[0][6] == "follower") and (tdSql.queryResult[0][4] == "leader"):
            tdLog.info("stop dnode 3")
            sc.dnodeStop(3)

        tdLog.info("wait 10 seconds")
        time.sleep(10)

        tdSql.query("show db.vgroups;")

        if(tdSql.queryResult[0][4] != "assigned") and (tdSql.queryResult[0][6] != "assigned"):
            tdLog.exit("failed to set aasigned")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
	
tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())