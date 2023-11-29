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


from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
import glob

def scanFiles(pattern):
    res = []
    for f in glob.iglob(pattern):
        res += [f]
    return res

def checkFiles(pattern, state):
    res = scanFiles(pattern)
    tdLog.info(res)
    num = len(res)
    if num:
        if state:
            tdLog.info("%s: %d files exist. expect: files exist" % (pattern, num))
        else:
            tdLog.exit("%s: %d files exist. expect: files not exist." % (pattern, num))
    else:
        if state:
            tdLog.exit("%s: %d files exist. expect: files exist" % (pattern, num))
        else:
            tdLog.info("%s: %d files exist. expect: files not exist." % (pattern, num))

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):

        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()

    def basic(self):
        tdLog.info("============== basic test ===============")
        cfg={
            '/mnt/data1' : 'dataDir',
            '/mnt/data2 0 0' : 'dataDir'
        }
        tdSql.createDir('/mnt/data1')
        tdSql.createDir('/mnt/data2')

        tdLog.info("================= step1")
        tdDnodes.stop(1)
        tdDnodes.deploy(1,cfg)
        tdDnodes.start(1)
        
        tdLog.info("================= step2")
        checkFiles(r'/mnt/data1/*/*',1)
        checkFiles(r'/mnt/data2/*/*',0)
        tdDnodes.stop(1)
    def dir_not_exist(self):
        tdLog.info("============== dir_not_exist test ===============")
        cfg={
            '/mnt/data1 0 0' : 'dataDir',
            '/mnt/data2 0 0' : 'dataDir'
        }
        tdSql.createDir('/mnt/data1')
        os.system('rm -rf /mnt/data2')
        

        tdLog.info("================= step1")
        tdDnodes.deploy(1,cfg)
        tdDnodes.startWithoutSleep(1)
        
        tdLog.info("================= step2")
        tdSql.taosdStatus(0)

    def dir_permission_denied(self):
        tdDnodes.stop(1)
        tdLog.info("============== dir_permission_denied test ===============")
        cfg={
            '/mnt/data1 0 0' : 'dataDir',
            '/mnt/data2 0 0' : 'dataDir'
        }
        tdSql.createDir('/mnt/data1')
        tdSql.createDir('/mnt/data2')
        os.system('chmod 111 /mnt/data2')

        tdLog.info("================= step1")
        tdDnodes.deploy(1,cfg)
        tdDnodes.startWithoutSleep(1)
        
        tdLog.info("================= step2")
        tdSql.taosdStatus(0)
    
    def file_distribution_same_level(self):
        tdLog.info("============== file_distribution_same_level test ===============")
        dataDir = ['data00','data01','data02','data03','data04']
        dataDict = {'data00':0,'data01':0,'data02':0,'data03':0,'data04':0}
        tdDnodes.stop(1)
        self.ntables = 1000             
        self.ts = 1520000010000
        tdLog.info("================= step1")
        cfg={
            '/mnt/data00 0 1' : 'dataDir',
            '/mnt/data01 0 0' : 'dataDir',
            '/mnt/data02 0 0' : 'dataDir',
            '/mnt/data03 0 0' : 'dataDir',
            '/mnt/data04 0 0' : 'dataDir'
        }
        dir_list = ['/mnt/data00','/mnt/data01','/mnt/data02','/mnt/data03','/mnt/data04']
        for i in dir_list:
            tdSql.createDir(i)
        tdDnodes.deploy(1,cfg)
        tdDnodes.start(1)

        tdSql.execute("create database test duration 8h")
        tdSql.execute("use test")

        tdSql.execute("create table stb(ts timestamp, c int) tags(t int)")

        for i in range(self.ntables):
            tdSql.execute("create table tb%d using stb tags(%d)" %(i, i))            
            tdSql.execute("insert into tb%d values(%d, 1)" % (i,self.ts + int (i / 100) * 86400000))

        tdLog.info("================= step2")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select * from test.stb")
        tdSql.checkRows(1000)
        tdLog.info("================= step3")
        tdSql.execute('drop database test')
        for i in range(10):
            tdSql.execute("create database test%d duration 8h" %(i))
            tdSql.execute("use test%d" %(i))
            tdSql.execute("create table tb (ts timestamp,i int)") 
            for j in range(10):           
                tdSql.execute("insert into tb values(%d, 1)" % (self.ts + int (i / 100) * 86400000))
        tdDnodes.stop(1)
        tdDnodes.start(1)
        flag = True
        for i in range(4):
            if dataDict[dataDir[i]] == dataDict[dataDir[i+1]]:
                flag = flag & True
            else:
                flag = flag & False
                break
        if not flag : tdLog.exit("%s failed, expect  not occured" % (sys.argv[0]))
    
    def three_level_basic(self):
        tdLog.info("============== three_level_basic test ===============")
        tdDnodes.stop(1)
        # Test1 1 dataDir
        cfg={
            '/mnt/data000 0 1' : 'dataDir',
            '/mnt/data001 0 0' : 'dataDir',
            '/mnt/data002 0 0' : 'dataDir',
            '/mnt/data010 1 0' : 'dataDir',
            '/mnt/data011 1 0' : 'dataDir',
            '/mnt/data012 1 0' : 'dataDir',
            '/mnt/data020 2 0' : 'dataDir',
            '/mnt/data021 2 0' : 'dataDir',
            '/mnt/data022 2 0' : 'dataDir'          
        }
        dir_list = ['/mnt/data000','/mnt/data001','/mnt/data002','/mnt/data010','/mnt/data011','/mnt/data012','/mnt/data020','/mnt/data021''/mnt/data022']
        for i in dir_list:
            tdSql.createDir(i)
        
        tdDnodes.deploy(1,cfg)
        tdDnodes.start(1)
        for i in dir_list:
            if i == '/mnt/data000':
                checkFiles("%s/*/*" % i, 1)
            else:
                checkFiles("%s/*/*" % i, 0)
    
    def more_than_16_disks(self):
        tdLog.info("============== more_than_16_disks test ===============")
        cfg={}
        for i in range(17):
            if i == 0 :
                datadir = '/mnt/data%d 0 1' % (i+1)
            else:
                datadir = '/mnt/data%d 0 0' % (i+1)
            cfg.update({ datadir : 'dataDir' })
            tdSql.createDir('/mnt/data%d' % (i+1))
        
        tdLog.info("================= step1")
        tdDnodes.stop(1)
        tdDnodes.deploy(1,cfg)
        tdDnodes.startWithoutSleep(1)
        
        tdLog.info("================= step2")
        tdSql.taosdStatus(0)
    
    def missing_middle_level(self):
        tdLog.info("============== missing_middle_level test ===============")
        tdDnodes.stop(1)
        # Test1 1 dataDir
        cfg={
            '/mnt/data1 1 0' : 'dataDir'           
        }
        tdSql.createDir('/mnt/data1')
        
        tdDnodes.deploy(1,cfg)
        tdDnodes.startWithoutSleep(1)
        
        tdSql.taosdStatus(0)
        tdDnodes.stop(1)
        # Test2 2 dataDir
        cfg = {
            '/mnt/data1 0 1' : 'dataDir',
            '/mnt/data1 2 0' : 'dataDir'
        }
        tdSql.createDir('/mnt/data1')
        
        tdDnodes.deploy(1,cfg)
        tdDnodes.startWithoutSleep(1)
        
        tdSql.taosdStatus(0)

    def trim_database(self):
        tdLog.info("============== trim_database test ===============")
        tdDnodes.stop(1)
        cfg = {
            '/mnt/data1 0 1' : 'dataDir'
            
        }
        tdSql.createDir('/mnt/data1')
        tdDnodes.deploy(1,cfg)
        tdDnodes.start(1)

        tdSql.execute('create database dbtest duration 3')
        tdSql.execute('use dbtest')
        tdSql.execute('create table stb (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute('create table tb1 using stb tags(1)')
        for i in range(10,30):
            tdSql.execute(f'insert into tb1 values(now-{i}d,10)')
        tdSql.execute('flush database dbtest')
        time.sleep(3)
        checkFiles('/mnt/data1/vnode/*/tsdb/v*',1)
        tdDnodes.stop(1)
        cfg={
            '/mnt/data1 0 1' : 'dataDir',
            '/mnt/data2 1 0' : 'dataDir',
            '/mnt/data3 2 0' : 'dataDir',        
        }
        tdSql.createDir('/mnt/data2')
        tdSql.createDir('/mnt/data3')
        tdDnodes.deploy(1,cfg)
        tdDnodes.start(1)
        checkFiles('/mnt/data1/vnode/*/tsdb/v*',1)
        checkFiles('/mnt/data2/vnode/*/tsdb/v*',0)
        checkFiles('/mnt/data3/vnode/*/tsdb/v*',0)
        tdSql.execute('alter database dbtest keep 10d,365d,3650d')
        tdSql.execute('trim database dbtest')
        time.sleep(3)
        checkFiles('/mnt/data1/vnode/*/tsdb/v*',1)
        checkFiles('/mnt/data2/vnode/*/tsdb/v*',1)

    def run(self):
        self.basic()
        self.dir_not_exist()
        self.dir_permission_denied()
        self.file_distribution_same_level()
        self.three_level_basic()
        self.more_than_16_disks()
        self.trim_database()
        self.missing_middle_level()
        


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
