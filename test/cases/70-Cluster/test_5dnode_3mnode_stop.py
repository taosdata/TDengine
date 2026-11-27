# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
import pytest
import taos
import sys
import time
import os
from new_test_framework.utils import tdSql, tdLog, cluster, clusterComCheck


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

        Labels: cluster,integration,functional
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



