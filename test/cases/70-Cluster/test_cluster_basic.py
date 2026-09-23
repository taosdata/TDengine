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
import sys
import time
import random
import taos
from new_test_framework.utils import tdLog, tdSql, cluster, sc, clusterComCheck


class TestClusterBasic:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.replicaVar = 3
        cls.vgroupNum = 3

    @pytest.mark.cluster
    def test_check_cluster_empty_db(self):
        """Cluster check empty db

        1. Create 5 dnodes cluster
        2. Create empty database
        3. Start and stop dnode 2
        4. Check cluster status

        Since: v3.3.0.0

        Labels: cluster,integration,functional
        Jira: None

        History:
            - 2024-2-6 Feng Chao Created
            - 2025-3-10 Huo Hong Migrated to new test framework

        """
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

    @pytest.mark.cluster
    def test_check_cluster_with_db(self):
        """Cluster restart dnode one by one

        1. Create 5 dnodes cluster
        2. Create database with replica 3 and vgroups 3
        3. Start and stop dnode 2 and dnode 3 one by one
        4. Check cluster status

        Since: v3.3.0.0

        Labels: cluster,integration,functional
        Jira: None 

        History:
            - 2024-2-6 Feng Chao Created
            - 2025-3-10 Huo Hong Migrated to new test framework

        """
        
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

