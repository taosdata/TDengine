from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
import os
import time
import datetime

class TestAlterReplica:

    def setup_class(cls):
        cls.replicaVar = 1
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
      
        
    def checkVgroups(self, dbName, vgNum):
        sleepNum = vgNum * 60
        flag = 0
        while (sleepNum > 0):
            sql = f'show {dbName}.vgroups'
            tdSql.query(sql)
            flag = 0
            for vgid in range (vgNum) :
                v1_status = tdSql.queryResult[vgid][4]
                v2_status = tdSql.queryResult[vgid][7]
                v3_status = tdSql.queryResult[vgid][10]
                if ((v1_status == 'leader') and (v2_status == 'follower') and (v3_status == 'follower')) \
                or ((v2_status == 'leader') and (v1_status == 'follower') and (v3_status == 'follower')) \
                or ((v3_status == 'leader') and (v2_status == 'follower') and (v1_status == 'follower')):
                    continue
                else: 
                    sleepNum = sleepNum - 1
                    time.sleep(1)
                    flag = 1
                    break
            if (0 == flag):
                return 0
        tdLog.debug("vgroup[%d] status: %s, %s, %s" %(vgid,v1_status,v2_status,v3_status))
        return -1
        
    #
    # alter database replica basic
    #
    def alter_replica_basic(self):        
        # create db and alter replica
        tdLog.debug("====alter db repica 1====")
        vgNum = 3
        dbName = 'db1'
        sql = f'create database {dbName} vgroups {vgNum}'
        tdSql.execute(sql)
        sql = f'alter database {dbName} replica 3'
        tdSql.execute(sql)
        tdLog.debug("start check time: %s"%(str(datetime.datetime.now())))
        res = self.checkVgroups(dbName, vgNum)       
        tdLog.debug("end   check time: %s"%(str(datetime.datetime.now())))
        if (0 != res):
            tdLog.exit(f'fail: alter database {dbName} replica 3') 
        
        # create db, stable, child tables, and insert data, then alter replica
        tdLog.debug("====alter db repica 2====")
        dbName = 'db2'
        sql = f'create database {dbName} vgroups {vgNum}'
        tdSql.execute(sql)        
        sql = f'use {dbName}'
        tdSql.execute(sql)
        sql = f'create stable stb (ts timestamp, c int) tags (t int)'
        tdSql.execute(sql)
        sql = f'create table ctb using stb tags (1)'
        tdSql.execute(sql)
        sql = f'insert into ctb values (now, 1) (now+1s, 2) (now+2s, 3)'
        tdSql.execute(sql)
        sql = f'alter database {dbName} replica 3'
        tdSql.execute(sql)
        tdLog.debug("start check time: %s"%(str(datetime.datetime.now())))
        res = self.checkVgroups(dbName, vgNum)
        tdLog.debug("end   check time: %s"%(str(datetime.datetime.now())))
        if (0 != res):
            tdLog.exit(f'fail: alter database {dbName} replica 3')        
        
        # firstly create db, stable, child tables, and insert data, then drop stable, and then alter replica
        tdLog.debug("====alter db repica 3====")
        dbName = 'db3'
        sql = f'create database {dbName} vgroups {vgNum}'
        tdSql.execute(sql)
        sql = f'use {dbName}'
        tdSql.execute(sql)
        sql = f'create stable stb (ts timestamp, c int) tags (t int)'
        tdSql.execute(sql)
        sql = f'create table ctb using stb tags (1)'
        tdSql.execute(sql)
        sql = f'insert into ctb values (now, 1) (now+1s, 2) (now+2s, 3)'
        tdSql.execute(sql)      
        sql = f'drop table stb'
        tdSql.execute(sql)        
        sql = f'alter database {dbName} replica 3'
        tdSql.execute(sql)
        tdLog.debug("start check time: %s"%(str(datetime.datetime.now())))
        res = self.checkVgroups(dbName, vgNum)
        tdLog.debug("end   check time: %s"%(str(datetime.datetime.now())))
        if (0 != res):
            tdLog.exit(f'fail: alter database {dbName} replica 3')


    #
    # alter replica from 1 to 3
    #
    def alter_replica_13(self):
        clusterComCheck.checkDnodes(4)
        sc.dnodeForceStop(3)
        sc.dnodeForceStop(4)
        clusterComCheck.checkDnodes(2)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

        tdLog.info(f"=============== step2: create database")

        tdSql.execute(f"create database db vgroups 4")
        tdSql.query(f"select * from information_schema.ins_databases where name='db' ")
        tdSql.checkRows(1)
        tdSql.checkKeyData("db", 4, 1)

        tdSql.query(f"show db.vgroups")
        tdSql.checkRows(4)

        tdSql.error(f"alter database db replica 3")

        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 binary(16)) comment "abd"'
        )
        tdSql.execute(f'create table db.ctb0 using db.stb tags(100, "100")')
        tdSql.execute(f'create table db.ctb1 using db.stb tags(101, "101")')
        tdSql.execute(f'create table db.ctb2 using db.stb tags(102, "102")')
        tdSql.execute(f'create table db.ctb3 using db.stb tags(103, "103")')
        tdSql.execute(f'create table db.ctb4 using db.stb tags(104, "104")')
        tdSql.execute(f'create table db.ctb5 using db.stb tags(105, "105")')
        tdSql.execute(f'create table db.ctb6 using db.stb tags(106, "106")')
        tdSql.execute(f'create table db.ctb7 using db.stb tags(107, "107")')
        tdSql.execute(f'create table db.ctb8 using db.stb tags(108, "108")')
        tdSql.execute(f'create table db.ctb9 using db.stb tags(109, "109")')
        tdSql.execute(f'insert into db.ctb0 values(now, 0, "0")')
        tdSql.execute(f'insert into db.ctb1 values(now, 1, "1")')
        tdSql.execute(f'insert into db.ctb2 values(now, 2, "2")')
        tdSql.execute(f'insert into db.ctb3 values(now, 3, "3")')
        tdSql.execute(f'insert into db.ctb4 values(now, 4, "4")')
        tdSql.execute(f'insert into db.ctb5 values(now, 5, "5")')
        tdSql.execute(f'insert into db.ctb6 values(now, 6, "6")')
        tdSql.execute(f'insert into db.ctb7 values(now, 7, "7")')
        tdSql.execute(f'insert into db.ctb8 values(now, 8, "8")')
        tdSql.execute(f'insert into db.ctb9 values(now, 9, "9")')
        tdSql.execute(f"flush database db;")

        tdLog.info(f"=============== step3: create dnodes")
        sc.dnodeStart(3)
        sc.dnodeStart(4)
        clusterComCheck.checkDnodes(4)

        tdLog.info(f"============= step4: alter database")
        tdSql.execute(f"alter database db replica 3")
        clusterComCheck.checkTransactions(300)

        tdSql.query(f"show db.vgroups")
        clusterComCheck.checkDbReady("db")

        tdLog.info(f"============= step5: check data")
        tdSql.query(f"select * from db.ctb0")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.ctb1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.ctb2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(10)


    #
    # alter replica from 3 to 1
    #
    def alter_replica_31(self):
        clusterComCheck.checkDnodes(4)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

        tdLog.info(f"=============== step2: create database")
        tdSql.execute(f"drop database if exists db")
        tdSql.execute(f"create database db vgroups 4 replica 3")
        tdSql.query(f"select * from information_schema.ins_databases where name ='db' ")
        tdSql.checkRows(1)
        tdSql.checkKeyData("db", 4, 3)

        # vnodes
        tdSql.query(f"show db.vgroups")
        tdSql.checkRows(4)

        clusterComCheck.checkDbReady("db")

        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 binary(16)) comment "abd"'
        )
        tdSql.execute(f'create table db.ctb0 using db.stb tags(100, "100")')
        tdSql.execute(f'create table db.ctb1 using db.stb tags(101, "101")')
        tdSql.execute(f'create table db.ctb2 using db.stb tags(102, "102")')
        tdSql.execute(f'create table db.ctb3 using db.stb tags(103, "103")')
        tdSql.execute(f'create table db.ctb4 using db.stb tags(104, "104")')
        tdSql.execute(f'create table db.ctb5 using db.stb tags(105, "105")')
        tdSql.execute(f'create table db.ctb6 using db.stb tags(106, "106")')
        tdSql.execute(f'create table db.ctb7 using db.stb tags(107, "107")')
        tdSql.execute(f'create table db.ctb8 using db.stb tags(108, "108")')
        tdSql.execute(f'create table db.ctb9 using db.stb tags(109, "109")')
        tdSql.execute(f'insert into db.ctb0 values(now, 0, "0")')
        tdSql.execute(f'insert into db.ctb1 values(now, 1, "1")')
        tdSql.execute(f'insert into db.ctb2 values(now, 2, "2")')
        tdSql.execute(f'insert into db.ctb3 values(now, 3, "3")')
        tdSql.execute(f'insert into db.ctb4 values(now, 4, "4")')
        tdSql.execute(f'insert into db.ctb5 values(now, 5, "5")')
        tdSql.execute(f'insert into db.ctb6 values(now, 6, "6")')
        tdSql.execute(f'insert into db.ctb7 values(now, 7, "7")')
        tdSql.execute(f'insert into db.ctb8 values(now, 8, "8")')
        tdSql.execute(f'insert into db.ctb9 values(now, 9, "9")')

        tdSql.query(f"show db.vgroups")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)}"
        )

        tdSql.query(f"select * from db.ctb0")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.ctb1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.ctb2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(10)

        tdLog.info(f"============= step3: alter database")
        tdSql.execute(f"alter database db replica 1")
        clusterComCheck.checkTransactions()

        tdSql.query(f"show db.vgroups")
        clusterComCheck.checkDbReady("db")

        tdLog.info(f"============= step5: stop dnode 2")
        tdSql.query(f"select * from db.ctb0")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.ctb1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.ctb2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(10)



    #
    # main
    #
    def test_alter_replica(self):
        """Alter database replica

        1. Alter replica basic operations
        2. Alter replica count from 1 to 3
        3. Alter replica count from 3 to 1

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-09-15 AlexDuan Migrated from uncatalog/system-test/1-insert/test_alter_replica.py
            - 2025-09-15 AlexDuan Combined from cases/02-Databases/03-Alter/test_db_alter_replicas_13.py
            - 2025-09-15 AlexDuan Combined from cases/02-Databases/03-Alter/test_db_alter_replicas_31.py

        """
        # call sub
        self.alter_replica_basic()
        self.alter_replica_13()
        self.alter_replica_31()
        
        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
