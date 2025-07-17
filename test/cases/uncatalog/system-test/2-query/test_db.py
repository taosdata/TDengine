import threading

from new_test_framework.utils import tdLog, tdSql, common as tdCom

class TestDb:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        pass

    def case1(self):
        tdSql.execute("create database if not exists dbms precision 'ms'")
        tdSql.execute("create database if not exists dbus precision 'us'")
        tdSql.execute("create database if not exists dbns precision 'ns'")

        tdSql.execute("create table dbms.ntb (ts timestamp, c1 int, c2 bigint)")
        tdSql.execute("create table dbus.ntb (ts timestamp, c1 int, c2 bigint)")
        tdSql.execute("create table dbns.ntb (ts timestamp, c1 int, c2 bigint)")

        tdSql.execute("insert into dbms.ntb values ('2022-01-01 08:00:00.001', 1, 2)")
        tdSql.execute("insert into dbms.ntb values ('2022-01-01 08:00:00.002', 3, 4)")

        tdSql.execute("insert into dbus.ntb values ('2022-01-01 08:00:00.000001', 1, 2)")
        tdSql.execute("insert into dbus.ntb values ('2022-01-01 08:00:00.000002', 3, 4)")

        tdSql.execute("insert into dbns.ntb values ('2022-01-01 08:00:00.000000001', 1, 2)")
        tdSql.execute("insert into dbns.ntb values ('2022-01-01 08:00:00.000000002', 3, 4)")

        tdSql.query("select count(c1) from dbms.ntb interval(1a)")
        tdSql.checkRows(2)

        tdSql.query("select count(c1) from dbus.ntb interval(1u)")
        tdSql.checkRows(2)

        tdSql.query("select count(c1) from dbns.ntb interval(1b)")
        tdSql.checkRows(2)
    
    def case2(self):

        tdSql.query("show variables") 
        tdSql.checkGreater(tdSql.getRows(), 80)       

        for i in range(self.replicaVar):
            tdSql.query("show dnode %d variables like 'debugFlag'" % (i + 1))
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, i + 1)
            tdSql.checkData(0, 1, 'debugFlag')
            tdSql.checkData(0, 2, 0)

        tdSql.query("show dnode 1 variables like '%debugFlag'")
        tdSql.checkRows(27)

        tdSql.query("show dnode 1 variables like '____debugFlag'")
        tdSql.checkRows(2)

        tdSql.query("show dnode 1 variables like 'ssAutoMigrateIntervalSec%'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'ssAutoMigrateIntervalSec')
        tdSql.checkData(0, 2, 3600)

        tdSql.query("show dnode 1 variables like 'ssPageCacheSize%'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'ssPageCacheSize')
        tdSql.checkData(0, 2, 4096)

        tdSql.query("show dnode 1 variables like 'ssUploadDelaySec%'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'ssUploadDelaySec')
        tdSql.checkData(0, 2, 60)
        
    def show_local_variables_like(self):
        tdSql.query("show local variables")        
        tdSql.checkGreater(tdSql.getRows(), 80)

        tdSql.query("show local variables like 'debugFlag'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'debugFlag')
        # tdSql.checkData(0, 1, 0)

        tdSql.query("show local variables like '%debugFlag'")
        tdSql.checkRows(9)

        tdSql.query("show local variables like '____debugFlag'")
        tdSql.checkRows(0)

        tdSql.query("show local variables like 'ssEnab%'")
        tdSql.checkRows(0)

        tdSql.query("show local variables like 'mini%'")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 'minimalTmpDirGB')

        tdSql.query("show local variables like '%info'")
        tdSql.checkRows(2)

    def show_cluster_variables_like(self):
        zones = ["", "cluster"]
        for zone in zones:
            tdLog.info(f"show {zone} variables")
            tdSql.query(f"show {zone} variables")        
            tdSql.checkGreater(tdSql.getRows(), 80)

            tdLog.info(f"show {zone} variables like 'debugFlag'")
            #tdSql.query(f"show {zone} variables like 'debugFlag'")
            #tdSql.checkRows(0)

            tdSql.query(f"show {zone} variables like 'ss%'")
            tdSql.checkRows(5)

            tdSql.query(f"show {zone} variables like 'Max%'")
            tdSql.checkRows(3)

            tdSql.query(f"show {zone} variables like 'ttl%'")
            tdSql.checkRows(5)

            tdSql.query(f"show {zone} variables like 'ttl34343434%'")
            tdSql.checkRows(0)

            tdSql.query(f"show {zone} variables like 'jdlkfdjdfkdfnldlfdnfkdkfdmfdlfmnnnnnjkjk'")
            tdSql.checkRows(0)
            
        
    def threadTest(self, threadID):
        print(f"Thread {threadID} starting...")
        tdsqln = tdCom.newTdSql()
        for i in range(100):
            tdsqln.query(f"desc db1.stb_1")
            tdsqln.checkRows(3)
        
        print(f"Thread {threadID} finished.")

    def case3(self):
        tdSql.execute("create database db1")
        tdSql.execute("create table db1.stb (ts timestamp, c1 varchar(100)) tags(t1 int)")
        tdSql.execute("create table db1.stb_1 using db1.stb tags(1)")

        threads = []
        for i in range(10):
            t = threading.Thread(target=self.threadTest, args=(i,))
            threads.append(t)
            t.start()
            
        for thread in threads:
            print(f"Thread waitting for finish...")
            thread.join()
        
        print(f"Mutithread test finished.")

    def test_db(self):
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
  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare(replica = self.replicaVar)

        tdLog.printNoPrefix("==========start case1 run ...............")
        self.case1()
        tdLog.printNoPrefix("==========end case1 run ...............")

        tdLog.printNoPrefix("==========start case2 run ...............")
        self.case2()
        tdLog.printNoPrefix("==========end case2 run ...............")
        
        tdLog.printNoPrefix("==========start case3 run ...............")
        self.case3()
        tdLog.printNoPrefix("==========end case3 run ...............")
        
        tdLog.printNoPrefix("==========start show_local_variables_like run ...............")
        self.show_local_variables_like()
        tdLog.printNoPrefix("==========end show_local_variables_like run ...............")
                
        tdLog.printNoPrefix("==========start show_cluster_variables_like run ...............")
        self.show_cluster_variables_like()
        tdLog.printNoPrefix("==========end show_cluster_variables_like run ...............")

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
