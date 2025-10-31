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
    # ------------------- main ----------------
    #
    def test_cluster_arbitrator(self):
        """Cluster arbitrator basic

        1. Create cluster with 3 dnodes
        2. Create a database and a stable with 2 replicas
        3. Create a child table
        4. Stop one dnode which is follower
        5. Check the vgroup status to be assigned
        6. Insert data into the child table
        7. Check inserted data is correct
        8. Restart the dnodes
        9. Check the vgroup status to be candidate
        10. Check "show arbgroups" result
        

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-23 Alex Duan Migrated from uncatalog/army/cluster/test_arbitrator.py
            - 2025-10-23 Alex Duan Migrated from uncatalog/army/cluster/test_arbitrator_restart.py

        """
        self.do_arbitrator()
        self.do_arbitrator_restart()