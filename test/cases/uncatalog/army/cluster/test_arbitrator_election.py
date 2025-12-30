from new_test_framework.utils import tdLog, tdSql, epath, sc
import time
# from frame.server.dnodes import *
# from frame.server.cluster import *


class TestArbitratorElection:
    
    def init(self, conn, logSql, replicaVar=1):
        updatecfgDict = {'dDebugFlag':131}
        super(TDTestCase, self).init(conn, logSql, replicaVar=1, checkColName="c1")
        
        self.valgrind = 0
        self.db = "test"
        self.stb = "meters"
        self.childtable_count = 10
        tdSql.init(conn.cursor(), logSql)  

    def test_arbitrator_election(self):
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

        tdSql.execute("use db;")

        tdSql.execute("CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);")

        tdSql.execute("CREATE TABLE d0 USING meters TAGS (\"California.SanFrancisco\", 2);");

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

        if(tdSql.getData(0, 4) == "follower") and (tdSql.getData(0, 7) == "leader"):
            tdLog.info("stop dnode2")
            sc.dnodeStop(2)

        if(tdSql.getData(0, 7) == "follower") and (tdSql.getData(0, 4) == "leader"):
            tdLog.info("stop dnode 3")
            sc.dnodeStop(3)

        
        count = 0
        while count < 11:
            tdSql.query("show db.vgroups;")

            if(tdSql.getData(0, 4) == "assigned ") or (tdSql.getData(0, 7) == "assigned "):
                break
            
            tdLog.info("wait %d seconds for set assigned"%count)
            time.sleep(1)

            count += 1
        
        if count == 11:
            tdLog.exit("check assigned failed")
            return

        

        tdLog.success(f"{__file__} successfully executed")


