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
import random

import taos
import frame
import frame.etool


from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.srvCtl import *
from frame.clusterCommonCheck import clusterComCheck

class TDTestCase(TBase):
    def init(self, conn, logSql, replicaVar=3):
        super(TDTestCase, self).init(conn, logSql, replicaVar=3, db="db")
        self.vgroupNum = 3
        tdSql.init(conn.cursor(), logSql)

    def checkClusterEmptyDB(self):
        while 1:
            if clusterComCheck.checkDnodes(3): break
        tdSql.query("show cluster alive;")
        tdSql.checkData(0, 0, 1)

        sc.dnodeStop(3)
        while 1:
            if clusterComCheck.checkDnodes(2): break
        tdSql.query("show cluster alive;")
        tdSql.checkData(0, 0, 1)

        sc.dnodeStop(2)
        while 1:
            if clusterComCheck.checkDnodes(1): break
        tdSql.query("show cluster alive;")
        tdSql.checkData(0, 0, 1)

    def checkClusterWithDB(self):
        sc.dnodeStart(3)
        sc.dnodeStart(2)
        while 1:
            if clusterComCheck.checkDnodes(3): break
            
        tdSql.execute(f'drop database if exists {self.db}')
        tdSql.execute(f'create database {self.db} replica {self.replicaVar} vgroups {self.vgroupNum}')
        while 1:
            leader_status = clusterComCheck.check_vgroups_status_with_offline(vgroup_numbers=self.vgroupNum, db_replica=self.replicaVar)
            if leader_status >= 0: break

        # leader_status = clusterComCheck.check_vgroups_status_with_offline(vgroup_numbers=self.vgroupNum, db_replica=self.replicaVar)
        tdSql.query("show cluster alive;")
        tdSql.checkData(0, 0, leader_status)

        sc.dnodeStop(3)
        while 1:
            if clusterComCheck.checkDnodes(2): break
            
        while 1:
            leader_status = clusterComCheck.check_vgroups_status_with_offline(vgroup_numbers=self.vgroupNum, db_replica=self.replicaVar)
            if leader_status >= 0: break
        # leader_status = clusterComCheck.check_vgroups_status_with_offline(vgroup_numbers=self.vgroupNum, db_replica=self.replicaVar)
        tdSql.query("show cluster alive;")
        tdSql.checkData(0, 0, leader_status)
        
        sc.dnodeStop(2)
        while 1:
            if clusterComCheck.checkDnodes(1): break
        while 1:
            leader_status = clusterComCheck.check_vgroups_status_with_offline(vgroup_numbers=self.vgroupNum, db_replica=self.replicaVar)
            if leader_status >= 0: break
        # leader_status = clusterComCheck.check_vgroups_status_with_offline(vgroup_numbers=self.vgroupNum, db_replica=self.replicaVar)
        tdSql.query("show cluster alive;")
        tdSql.checkData(0, 0, leader_status)

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")
        self.checkClusterEmptyDB()
        self.checkClusterWithDB()

    def stop(self):
        sc.dnodeStop(1)
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
