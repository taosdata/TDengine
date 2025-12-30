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
from new_test_framework.utils import tdLog, tdSql

class TestReplica2:
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to init {__file__}")
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), logSql)

    def test_replica2(self):
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

        tdLog.success(f"{__file__} successfully executed")
	
