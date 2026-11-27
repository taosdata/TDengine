# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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
import time


class TestClusterArbitrator:
    
    def setup_class(cls):
        cls.db="test"
        cls.stb="meters"
        cls.checkColName="c1"
        cls.valgrind = 0
        cls.childtable_count = 10

    #
    # ------------------- test_arbitrator.py ----------------
    #
    def do_arbitrator(self):
        tdLog.info("create database")
        tdSql.execute('CREATE DATABASE db vgroups 1 replica 2;')

        if self.waitTransactionZero() is False:
            tdLog.exit(f"create db transaction not finished")
            return False

        time.sleep(1)

        tdSql.execute("use db;")

        tdLog.info("create stable")
        tdSql.execute("CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);")

        if self.waitTransactionZero() is False:
            tdLog.exit(f"create stable transaction not finished")
            return False

        tdLog.info("create table")
        tdSql.execute("CREATE TABLE d0 USING meters TAGS (\"California.SanFrancisco\", 2);");

        count = 0

        tdLog.info("waiting vgroup is sync")
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
            
        count = 0
        while count < 100:
            tdSql.query("show db.vgroups;")

            if(tdSql.getData(0, 4) == "follower") and (tdSql.getData(0, 7) == "leader"):
                tdLog.info("stop dnode2")
                sc.dnodeStop(2)
                break

            if(tdSql.getData(0, 7) == "follower") and (tdSql.getData(0, 4) == "leader"):
                tdLog.info("stop dnode 3")
                sc.dnodeStop(3)
                break
            
            time.sleep(1)
            count += 1

        if count == 100:
            tdLog.exit("check leader and stop node failed")
            return    
        
        count = 0
        while count < 100:
            tdSql.query("show db.vgroups;")

            if(tdSql.getData(0, 4) == "assigned ") or (tdSql.getData(0, 7) == "assigned "):
                break
            
            tdLog.info("wait %d seconds for set assigned"%count)
            time.sleep(1)

            count += 1
        
        if count == 100:
            tdLog.exit("check assigned failed")
            return

        tdSql.execute("INSERT INTO d0 VALUES (NOW, 10.3, 219, 0.31);")
        tdSql.query("SELECT * FROM d0;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10.3)
        tdSql.checkData(0, 2, 219)
        tdSql.checkData(0, 3, 0.31)
        
        sc.dnodeStart(2)
        sc.dnodeStart(3)

        print("do arbitrator ......................... [passed]")

    #
    # ------------------- test_arbitrator_restart.py ----------------
    #
    def do_arbitrator_restart(self):
        tdLog.info("create database")
        tdSql.execute("drop database if exists db;")
        tdSql.execute('CREATE DATABASE db vgroups 1 replica 2;')

        if self.waitTransactionZero() is False:
            tdLog.exit(f"create db transaction not finished")
            return False

        time.sleep(1)

        tdSql.execute("use db;")

        tdLog.info("create stable")
        tdSql.execute("CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);")

        if self.waitTransactionZero() is False:
            tdLog.exit(f"create stable transaction not finished")
            return False

        tdLog.info("create table")
        tdSql.execute("CREATE TABLE d0 USING meters TAGS (\"California.SanFrancisco\", 2);");

        tdLog.info("waiting vgroup is sync")
        count = 0
        while count < 100:        
            tdSql.query("show arbgroups;")

            if tdSql.getData(0, 4) == True:
                break

            tdLog.info("wait 1 seconds for is sync")
            time.sleep(1)

            count += 1
        if count == 100:
            tdLog.exit("arbgroup sync failed")
            return 

        tdLog.info("stop dnode2 and dnode3")
        sc.dnodeStop(2) 
        sc.dnodeStop(3)

        tdLog.info("start dnode2")
        sc.dnodeStart(2)

        tdLog.info("waiting candidate")
        count = 0
        while count < 100:
            tdSql.query("show db.vgroups;")

            if(tdSql.getData(0, 4) == "candidate") or (tdSql.getData(0, 6) == "candidate"):
                break
            
            tdLog.info("wait 1 seconds for candidate")
            time.sleep(1)

            count += 1
        if count == 100:
            tdLog.exit("wait candidate failed")
            return
        
        tdLog.info("force assign")
        tdSql.execute("ASSIGN LEADER FORCE;")

        tdLog.info("waiting assigned")
        count = 0
        while count < 100:
            tdSql.query("show db.vgroups;")

            if(tdSql.getData(0, 4) == "assigned ") or (tdSql.getData(0, 6) == "assigned "):
                break
            
            tdLog.info("wait 1 seconds for set assigned")
            time.sleep(1)

            count += 1
        if count == 100:
            tdLog.exit("assign leader failed")
            return

        tdSql.execute("INSERT INTO d0 VALUES (NOW, 10.3, 219, 0.31);")

        tdLog.info("start dnode3")
        sc.dnodeStart(3)

        tdLog.info("waiting vgroup is sync")
        count = 0
        while count < 100:        
            tdSql.query("show arbgroups;")

            if tdSql.getData(0, 4) == 1:
                break

            tdLog.info("wait 1 seconds for is sync")
            time.sleep(1)

            count += 1
        if count == 100:
            tdLog.exit("arbgroup sync failed")
            return

        print("do arbitrator restart ................. [passed]")  

    #
    # ------------------- test_arbitrator_split.py ----------------
    # Regression test: splitting a double-replica (withArbitrator) vgroup used to
    # leave the corresponding arbgroup stale (the source arbgroup was never
    # dropped) and the two resulting replica=2 vgroups ended up with no arbgroup.
    # After the fix, every replica=2 vgroup owns exactly one arbgroup, so the
    # number of arbgroups must equal the number of replica=2 vgroups at all times.
    #
    def do_arbitrator_split(self):
        tdLog.info("create database")
        tdSql.execute("drop database if exists db;")
        tdSql.execute('CREATE DATABASE db vgroups 2 replica 2;')

        if self.waitTransactionZero() is False:
            tdLog.exit(f"create db transaction not finished")
            return False

        time.sleep(1)

        tdLog.info("wait for the initial arbgroups to sync")
        count = 0
        while count < 100:
            tdSql.query("show arbgroups;")
            if tdSql.getData(0, 4) == True:
                break
            tdLog.info("wait %d seconds for arbgroups sync" % count)
            time.sleep(1)
            count += 1
        if count == 100:
            tdLog.exit("arbgroup sync failed")
            return

        # before split: 2 vgroups (replica 2) -> 2 arbgroups
        tdSql.query("show arbgroups;")
        beforeArbCount = tdSql.getRows()
        tdLog.info("before split, arbgroup count = %d" % beforeArbCount)
        if beforeArbCount != 2:
            tdLog.exit("unexpected arbgroup count before split: %d, expected 2" % beforeArbCount)
            return

        # pick an existing replica=2 vgroup to split. Do NOT assume vgId=1: the global
        # vgroup id allocator may start from a higher number (e.g. 2/3), so vgId=1 can
        # be absent. Any of the current arbgroups' vgIds is a valid split target.
        splitVgId = tdSql.getData(0, 1)
        tdLog.info("split vgroup %d" % splitVgId)
        tdSql.execute("split vgroup %d;" % splitVgId)

        if self.waitTransactionZero() is False:
            tdLog.exit(f"split vgroup transaction not finished")
            return False

        time.sleep(1)

        # after split: the split source vgroup (splitVgId) is dropped and replaced by
        # 2 new vgroups, so the cluster now has 3 replica=2 vgroups -> 3 arbgroups.
        # The stale arbgroup for the split source must no longer exist (checked below).
        tdSql.query("show arbgroups;")
        afterArbCount = tdSql.getRows()
        tdLog.info("after split, arbgroup count = %d" % afterArbCount)
        if afterArbCount != 3:
            tdLog.exit("unexpected arbgroup count after split: %d, expected 3" % afterArbCount)
            return

        for i in range(afterArbCount):
            vgId = tdSql.getData(i, 1)
            dbName = tdSql.getData(i, 0)
            if dbName != "db":
                continue
            if vgId == splitVgId:
                tdLog.exit("stale arbgroup for split source vgId=%d still exists" % splitVgId)
                return

        tdLog.info("arbgroup consistency verified after split (no stale arbgroup, 3 arbgroups for 3 vgroups)")
        print("do arbitrator split .................. [passed]")


    #
    # ------------------- main ----------------
    #
    def test_cluster_arbitrator(self):
        """Cluster arbitrator basic

        1. Create cluster with 3 dnodes
        2. Create a database and a stable with 2 replicas
        3. Create a child table
        4. Stop one dnode which is follower
        5. Check the vgroup status to be assigned
        6. Restart the dnodes
        7. Insert data into the child table
        8. Check inserted data is correct
        9. Check the vgroup status to be candidate
        10. Check "show arbgroups" result
        

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        
        Jira: None

        History:
            - 2025-10-23 Alex Duan Migrated from uncatalog/army/cluster/test_arbitrator.py
            - 2025-10-23 Alex Duan Migrated from uncatalog/army/cluster/test_arbitrator_restart.py

        """
        self.do_arbitrator()
        self.do_arbitrator_restart()
        self.do_arbitrator_split()