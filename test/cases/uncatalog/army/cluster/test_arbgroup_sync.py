# Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import tdLog, tdSql, epath, sc

# from frame.server.dnodes import *
# from frame.server.cluster import *


class TestArbgroupSync:
    updatecfgDict = {'dDebugFlag':131}
    
    def setup_class(cls):
        cls.init(db="test", stb="meters", checkColName="c1")
        cls.valgrind = 0
        cls.childtable_count = 10

    def test_arbgroup_sync(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: common,ci,integration,functional
        Jira: xxx
        
        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        tdSql.execute('CREATE DATABASE db vgroups 1 replica 2;')

        time.sleep(1)

        count = 0

        while count < 100:        
            tdSql.query("show arbgroups;")

            if tdSql.getData(0, 4) == True:
                break

            tdLog.info("wait %d seconds for is sync"%count)
            time.sleep(1)

            count += 1

        if count == 100:
            tdLog.exit("arbgroup sync failed")
            return    

        tdSql.query("show db.vgroups;")

        if(tdSql.getData(0, 4) == "follower") and (tdSql.getData(0, 6) == "leader"):
            tdLog.info("stop dnode2")
            sc.dnodeStop(2)

        if(tdSql.getData(0, 6) == "follower") and (tdSql.getData(0, 4) == "leader"):
            tdLog.info("stop dnode 3")
            sc.dnodeStop(3)

        
        count = 0
        while count < 100:
            tdSql.query("show db.vgroups;")

            if(tdSql.getData(0, 4) == "assigned ") or (tdSql.getData(0, 6) == "assigned "):
                break
            
            tdLog.info("wait %d seconds for set assigned"%count)
            time.sleep(1)

            count += 1
        
        if count == 100:
            tdLog.exit("check assigned failed")
            return




