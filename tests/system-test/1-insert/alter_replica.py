import taos
import sys
import time
import socket
import os
import threading
import datetime

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)        
        
    def checkVgroups(self, dbName, vgNum):
        sleepNum = vgNum * 60
        flag = 0
        while (sleepNum > 0):
            sql = f'show {dbName}.vgroups'
            tdSql.query(sql)
            flag = 0
            for vgid in range (vgNum) :
                v1_status = tdSql.queryResult[vgid][4]
                v2_status = tdSql.queryResult[vgid][6]
                v3_status = tdSql.queryResult[vgid][8]
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
        
    def alter_replica(self):        
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

    def run(self):
        self.alter_replica()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
