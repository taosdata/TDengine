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

from new_test_framework.utils import tdLog, tdSql, epath, sc




class TestClusterBasic:
    def init(self, conn, logSql, replicaVar=3):
        super(TDTestCase, self).init(conn, logSql, replicaVar=3, db="db")
        self.vgroupNum = 3
        tdSql.init(conn.cursor(), logSql)

    def checkClusterEmptyDB(self):
        while 1:
            if clusterComCheck.checkDnodes(5): break
        tdSql.query("show cluster alive;")
        tdSql.checkData(0, 0, 1)

        sc.dnodeStop(3)
        while 1:
            if clusterComCheck.checkDnodes(4): break
        tdSql.query("show cluster alive;")
        tdSql.checkData(0, 0, 1)

        sc.dnodeStop(2)
        while 1:
            if clusterComCheck.checkDnodes(3): break
        tdSql.query("show cluster alive;")
        tdSql.checkData(0, 0, 1)

    def checkClusterWithDB(self):
        sc.dnodeStart(3)
        sc.dnodeStart(2)
        while 1:
            if clusterComCheck.checkDnodes(5): break
        
        tdSql.execute(f'drop database if exists {self.db}')
        tdSql.execute(f'create database {self.db} replica {self.replicaVar} vgroups {self.vgroupNum}')
        while 1:
            leader_status = clusterComCheck.check_vgroups_status_with_offline(vgroup_numbers=self.vgroupNum, db_replica=self.replicaVar)
            if leader_status >= 0: break
        leader_status = clusterComCheck.check_vgroups_status_with_offline(vgroup_numbers=self.vgroupNum, db_replica=self.replicaVar)
        tdSql.query("show cluster alive;")
        tdSql.checkData(0, 0, leader_status)

        sc.dnodeStop(3)
        while 1:
            if clusterComCheck.checkDnodes(4): break
        while 1:
            leader_status = clusterComCheck.check_vgroups_status_with_offline(vgroup_numbers=self.vgroupNum, db_replica=self.replicaVar)
            if leader_status >= 0: break
        leader_status = clusterComCheck.check_vgroups_status_with_offline(vgroup_numbers=self.vgroupNum, db_replica=self.replicaVar)
        tdSql.query("show cluster alive;")
        tdSql.checkData(0, 0, leader_status)
        
        sc.dnodeStop(2)
        while 1:
            if clusterComCheck.checkDnodes(3): break
        while 1:
            leader_status = clusterComCheck.check_vgroups_status_with_offline(vgroup_numbers=self.vgroupNum, db_replica=self.replicaVar)
            if leader_status >= 0: break
        leader_status = clusterComCheck.check_vgroups_status_with_offline(vgroup_numbers=self.vgroupNum, db_replica=self.replicaVar)
        tdSql.query("show cluster alive;")
        tdSql.checkData(0, 0, leader_status)

    # run
    def test_cluster_basic(self):
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
        tdLog.debug(f"start to excute {__file__}")
        self.checkClusterEmptyDB()
        self.checkClusterWithDB()

        sc.dnodeStop(1)
        sc.dnodeStop(4)
        sc.dnodeStop(5)
        tdLog.success(f"{__file__} successfully executed")

