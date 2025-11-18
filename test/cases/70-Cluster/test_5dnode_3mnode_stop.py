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
import pytest
import taos
import sys
import time
import os
from new_test_framework.utils import tdSql, tdLog, cluster
from clusterCommonCheck import *


class Test5dnode3mnodeStop:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")


    @pytest.mark.cluster
    def test_five_dnode_three_mnode(self):
        """Cluster database with replica 3

        1. Create 5 dnodes 3 mnodes cluster
        2. Create database with replica 3 and vgroups 4
        3. Stop dnode one by one and check mnode status
        4. Restart dnode one by one and check mnode status
        5. Check vgroups status and replica status


        Since: v3.3.0.0

        Labels: cluster

        History:
            - 2022-6-11 tomchon Created
            - 2025-5-13 Huo Hong Migrated to new test framework

        """

        dnodenumbers = self.dnode_nums
        mnodeNums = self.mnode_nums
        restartNumber = 1
        clusterComCheck.init(self.conn)
        tdLog.info("======== test case 1: ")
        paraDict = {'dbName':     'db',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'replica':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbNum':     1,
                    'rowsPerTbl': 10000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  10,
                    'showMsg':    1,
                    'showRow':    1}
        dnodenumbers=int(dnodenumbers)
        mnodeNums=int(mnodeNums)
        dbNumbers = int(dnodenumbers * restartNumber)

        tdLog.info("first check dnode and mnode")
        tdSql.query("select * from information_schema.ins_dnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(4,1,'%s:6430'%self.host)
        clusterComCheck.checkDnodes(dnodenumbers)
        
        #check mnode status
        tdLog.info("check mnode status")
        clusterComCheck.checkMnodeStatus(mnodeNums)

        # # fisr add three mnodes;
        # tdLog.info("check mnode status")
        # # tdSql.execute("create mnode on dnode 2")
        # clusterComCheck.checkMnodeStatus(2)
        # # tdSql.execute("create mnode on dnode 3")
        # clusterComCheck.checkMnodeStatus(3)

        # add some error operations and
        tdLog.info("Confirm the status of the dnode again")
        tdSql.error("create mnode on dnode 2")
        tdSql.query("select * from information_schema.ins_dnodes;")
        # print(tdSql.queryResult)
        clusterComCheck.checkDnodes(dnodenumbers)
        # restart all taosd
        tdDnodes=cluster.dnodes
        tdLog.info(f"tdDnodes: {tdDnodes}")
        tdLog.info(f"tdDnodes[1].running: {tdDnodes[1].running}")
        tdLog.info(f"tdDnodes[1].index: {tdDnodes[1].index}")
        tdLog.info(f"tdDnodes[1].cfgDict: {tdDnodes[1].cfgDict}")
        tdDnodes[1].stoptaosd()
        clusterComCheck.check3mnodeoff(2,3)
        tdDnodes[1].starttaosd()
        clusterComCheck.checkMnodeStatus(3)

        tdDnodes[2].stoptaosd()
        clusterComCheck.check3mnodeoff(3,3)
        tdDnodes[2].starttaosd()
        clusterComCheck.checkMnodeStatus(3)

        tdDnodes[0].stoptaosd()
        clusterComCheck.check3mnodeoff(1,3)
        tdDnodes[0].starttaosd()
        clusterComCheck.checkMnodeStatus(3)

        tdLog.info(f"{__file__} successfully executed")

