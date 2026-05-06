###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import time
from clusterSetup import *
from util.sql import tdSql
from util.log import tdLog
import random

class ClusterTestcase:

    def _has_three_nodes(self, nodes):
        return len(nodes.tdnodes) >= 3 and nodes.node1 is not None and nodes.node2 is not None and nodes.node3 is not None

    def _run_lagging_node_regression(self, nodes, db_name):
        tdLog.info("syncingTest lagging-node regression start")

        nodes.stopOneNode(2)
        try:
            for i in range(50):
                tdSql.execute("create table if not exists lagging_%d using meters tags(1)" % i)
                tdSql.execute("insert into lagging_%d values(now, %d)" % (i, i))
        finally:
            nodes.startOneNode(2)

        deadline = time.time() + 30
        replica_ready = False
        while time.time() < deadline:
            tdSql.query("show %s.vgroups" % db_name)
            if tdSql.queryRows > 0:
                replica_ready = True
                break
            time.sleep(1)

        tdLog.info("syncingTest lagging-node regression recovered=%s" % replica_ready)
        tdSql.query("select count(*) from meters")
    
    ## test case 24, 25, 26, 27 ##
    def run(self):
        started_at = time.time()
        
        nodes = Nodes()
        if nodes.node1 is None:
            tdLog.exit("syncingTest requires at least one configured cluster node")

        ctest = ClusterTest(nodes.node1.hostName)
        ctest.connectDB()        
        ctest.createSTable(1)
        ctest.run()
        tdSql.init(ctest.conn.cursor(), False)
        
        
        tdSql.execute("use %s" % ctest.dbName)        
        tdSql.execute("alter database %s replica 3" % ctest.dbName)

        tdLog.info("syncingTest baseline timing start=%s" % started_at)
        
        for i in range(100):
            tdSql.execute("drop table t%d" % i)
        
        for i in range(100):
            tdSql.execute("create table a%d using meters tags(1)" % i)
        
        tdSql.execute("alter table meters add col col5 int")
        tdSql.execute("alter table meters drop col col5 int")

        if self._has_three_nodes(nodes):
            self._run_lagging_node_regression(nodes, ctest.dbName)
        else:
            tdLog.info("syncingTest lagging-node regression skipped: fewer than 3 configured nodes")

        tdSql.execute("drop database %s" % ctest.dbName)
        elapsed = time.time() - started_at
        tdLog.info("syncingTest elapsed_seconds=%.3f" % elapsed)
        
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

ct = ClusterTestcase()
ct.run()
