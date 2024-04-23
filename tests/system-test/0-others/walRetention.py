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

#
#  The option for wal_retetion_period and wal_retention_size is work well
#

import taos
from taos.tmq import Consumer

from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *


import os
import threading
import json
import time
from datetime import date
from datetime import datetime
from datetime import timedelta
from os       import path


#
# --------------    util   --------------------------
#
def pathSize(path):

    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for i in filenames:
            # use join to concatenate all the components of path
            f = os.path.join(dirpath, i)
            # use getsize to generate size in bytes and add it to the total size
            total_size += os.path.getsize(f)
            # print(dirpath)

    print(" %s  %.02f MB" % (path, total_size/1024/1024))
    return total_size


# load json from file
def jsonFromFile(jsonFile):
    fp = open(jsonFile)
    return json.load(fp)


#
# ----------------- class ------------------
#

# wal file object
class WalFile:
    def __init__(self, pathFile, fileName):
        self.mtime = os.path.getmtime(pathFile)
        self.startVer = int(fileName)
        self.fsize = os.path.getsize(pathFile)
        self.endVer = -1
        self.pathFile = pathFile

    def needDelete(self, delTsLine):
        return True    

# VNode object
class VNode :
    # init
    def __init__(self, dnodeId, path, walPeriod, walSize, walStayRange):
        self.path = path
        self.dnodeId = dnodeId
        self.vgId = 0
        self.snapVer = 0
        self.firstVer = 0
        self.lastVer = -1
        self.walPeriod = walPeriod
        self.walSize   = walSize
        self.walStayRange = walStayRange
        self.walFiles = []
        self.load(path)

    # load
    def load(self, path):
        # load wal
        walPath = os.path.join(path, "wal")
        metaFile = ""
        with os.scandir(walPath) as items:
            for item in items:
                if item.is_file():
                    fileName, fileExt = os.path.splitext(item.name)
                    pathFile = os.path.join(walPath, item)
                    if fileExt == ".log":
                        self.walFiles.append(WalFile(pathFile, fileName))
                    elif fileExt == "":
                        if fileName[:8] == "meta-ver":
                            metaFile = pathFile
        # load config
        tdLog.info(f' meta-ver file={metaFile}')
        if metaFile != "":
            try:
                jsonVer = jsonFromFile(metaFile)
                metaNode = jsonVer["meta"]
                self.snapVer  = int(metaNode["snapshotVer"])
                self.firstVer = int(metaNode["firstVer"])
                self.lastVer  = int(metaNode["lastVer"])
            except Exception as e:
                tdLog.info(f' read json file except.')

        # sort with startVer
        self.walFiles = sorted(self.walFiles, key=lambda x : x.startVer, reverse=True)
        # set endVer
        startVer = -1
        for walFile in self.walFiles:
            if startVer == -1:
                startVer = walFile.startVer
                continue
            walFile.endVer = startVer - 1
            startVer = walFile.startVer

        # print total
        tdLog.info(f" ----  dnode{self.dnodeId} snapVer={self.snapVer} firstVer={self.firstVer} lastVer={self.lastVer} {self.path}  --------")
        for walFile in self.walFiles:
            mt = datetime.fromtimestamp(walFile.mtime)
            tdLog.info(f" {walFile.pathFile} {mt} startVer={walFile.startVer} endVer={walFile.endVer}")

    # snapVer compare
    def canDelete(self, walFile):
        if walFile.endVer == -1:
            # end file
            return False
                
        # check snapVer
        ret = False
        if  self.snapVer > walFile.endVer:
            ret = True

        # check stayRange
        if self.lastVer != -1 and ret:
            # first wal file ignore
            if walFile.startVer == self.firstVer:
                tdLog.info(f"    can del {walFile.pathFile}, but is first. snapVer={self.snapVer} firstVer={self.firstVer}")
                return False

            # ver in stay range 
            smallVer = self.snapVer - self.walStayRange -1
            if walFile.startVer >= smallVer:
                tdLog.info(f"    can del {walFile.pathFile}, but range not arrived. snapVer={self.snapVer} smallVer={smallVer}")
                return False

        return ret
    
    # get log size
    def getWalsSize(self):
        size = 0
        lastSize = 0
        max = -1
        for walFile in self.walFiles:
            if self.canDelete(walFile) == False:
                tdLog.info(f"  calc vnode size {walFile.pathFile} size={walFile.fsize} startVer={walFile.startVer}")
                size += walFile.fsize
                if max < walFile.startVer:
                    max = walFile.startVer
                    lastSize = walFile.fsize

        
        if lastSize > 0:
            tdLog.info(f" last file size need reduct . lastSize={lastSize}")
            size -= lastSize
        return size
    
    # vnode
    def check_retention(self):
        #
        # check period
        #
        delta = self.walPeriod
        if self.walPeriod == 0:
            delta += 1 * 60  # delete after 1 minutes
        elif self.walPeriod < 3600: 
            delta += 3 * 60  # 5 minutes
        else:
            delta += 5 * 60 # 10 minutes

        delTsLine = datetime.now() - timedelta(seconds = delta)
        delTs = delTsLine.timestamp()
        for walFile in self.walFiles:
            mt = datetime.fromtimestamp(walFile.mtime)
            info = f" {walFile.pathFile} size={walFile.fsize} mt={mt} line={delTsLine}  start={walFile.startVer} snap={self.snapVer} end= {walFile.endVer}"
            tdLog.info(info) 
            if walFile.mtime < delTs and self.canDelete(walFile):
                # wait a moment then check file exist
                time.sleep(1) 
                if os.path.exists(walFile.pathFile):
                    #report error
                    tdLog.exit(f" wal file expired need delete. \n   {walFile.pathFile} \n   modify time={mt} \n   delTsLine={delTsLine}\n   start={walFile.startVer} snap={self.snapVer} end= {walFile.endVer}")
                    return False            

        #
        #  check size
        # 
        if self.walSize == 0:
            return True
        
        time.sleep(2)
        vnodeSize = self.getWalsSize()
        # need over 20%
        if vnodeSize < self.walSize * 1.2:
            tdLog.info(f" wal size valid. {self.path} real = {vnodeSize} set = {self.walSize}. allow over 20%.")
            return True
        
        # check over
        tdLog.exit(f" wal size over set. {self.path} real = {vnodeSize} set = {self.walSize} ")
        return False


# insert by async
def thread_insert(testCase, tbname, rows):
    print(f"start thread... {tbname} - {rows} \n")
    new_conn = testCase.new_connect()
    testCase.insert_data(tbname, rows, new_conn)
    new_conn.close()
    print("end thread\n")

# case
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.ts = 1670000000000
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        self.conn = conn

        # init cluster path
        selfPath = os.path.dirname(os.path.realpath(__file__))
        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]
        self.projDir = f"{projPath}sim/"
        tdLog.info(f" init projPath={self.projDir}")

        self.column_dict = {
            'ts': 'timestamp',
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'tinyint unsigned',
            'col6': 'smallint unsigned',
            'col7': 'int unsigned',
            'col8': 'bigint unsigned',
            'col9': 'float',
            'col10': 'double',
            'col11': 'bool',
            'col12': 'varchar(120)',
            'col13': 'nchar(100)',
        }
        self.tag_dict = {
            't1': 'tinyint',
            't2': 'smallint',
            't3': 'int',
            't4': 'bigint',
            't5': 'tinyint unsigned',
            't6': 'smallint unsigned',
            't7': 'int unsigned',
            't8': 'bigint unsigned',
            't9': 'float',
            't10': 'double',
            't11': 'bool',
            't12': 'varchar(120)',
            't13': 'nchar(100)',         
        }

    # malloc new connect
    def new_connect(self):
        return taos.connect(host     = self.conn._host, 
                            user     = self.conn._user, 
                            password = self.conn._password, 
                            database = self.dbname,
                            port     = self.conn._port, 
                            config   = self.conn._config)

    def set_stb_sql(self,stbname,column_dict,tag_dict):
        column_sql = ''
        tag_sql = ''
        for k,v in column_dict.items():
            column_sql += f"{k} {v}, "
        for k,v in tag_dict.items():
            tag_sql += f"{k} {v}, "
        create_stb_sql = f'create stable {stbname} ({column_sql[:-2]}) tags ({tag_sql[:-2]})'
        return create_stb_sql
    
    def create_database(self, dbname, wal_period, wal_size_kb, vgroups):
        self.wal_period = wal_period
        self.wal_size = wal_size_kb * 1024
        self.vgroups = vgroups
        self.dbname = dbname
        tdSql.execute(f"create database {dbname} wal_retention_period {wal_period} wal_retention_size {wal_size_kb} vgroups {vgroups} replica 3")
        tdSql.execute(f'use {dbname}')
    
    # create stable and child tables
    def create_table(self, stbname, tbname, count):
        self.child_count = count
        self.stbname = stbname
        self.tbname  = tbname
        
        # create stable
        create_table_sql = self.set_stb_sql(stbname, self.column_dict, self.tag_dict)
        tdSql.execute(create_table_sql)

        batch_size = 1000
        # create child table
        for i in range(count):
            ti = i % 128
            tags = f'{ti},{ti},{i},{i},{ti},{ti},{i},{i},{i}.000{i},{i}.000{i},true,"var{i}","nch{i}"'
            sql  = f'create table {tbname}{i} using {stbname} tags({tags});'
            tdSql.execute(sql)            
            if i % batch_size == 0:
               tdLog.info(f" create child table {i} ...")

        tdLog.info(f" create {count} child tables ok.")


    # insert to child table d1 data
    def insert_data(self, tbname, insertTime):
        start = time.time()
        values = ""
        child_name = ""
        cnt = 0
        rows = 10000000000
        for j in range(rows):
            for i in range(self.child_count):
                tj = j % 128
                cols = f'{tj},{tj},{j},{j},{tj},{tj},{j},{j},{j}.000{j},{j}.000{j},true,"var{j}","nch{j}涛思数据codepage is utf_32_le"'
                sql = f'insert into {tbname}{i} values ({self.ts},{cols});' 
                tdSql.execute(sql)
                self.ts += 1
                #tdLog.info(f" child table={i} rows={j} insert data.")
            cost = time.time() - start
            if j % 100 == 0:
                tdSql.execute(f"flush database {self.dbname}")
                tdLog.info("   insert row cost time = %ds rows = %d"%(cost, j))
                self.consume_topic("topic1", 5)

            if cost > insertTime and j > 100:
                tdLog.info(" insert finished. cost time = %ds rows = %d"%(cost, j))
                return
   
    # create tmq
    def create_tmq(self):
        sql = f"create topic topic1 as select ts, col1, concat(col12,t12) from {self.stbname};" 
        tdSql.execute(sql)
        sql = f"create topic topic2 as select * from {self.stbname};" 
        tdSql.execute(sql)
        #tdLog.info(sql)

    def check_retention(self, walStayRange):
        # flash database
        tdSql.execute(f"flush database {self.dbname}")
        time.sleep(0.5)

        vnodes = []
        # put all vnode to list
        for dnode in os.listdir(self.projDir):
            vnodeDir = self.projDir + f"{dnode}/data/vnode/"
            print(f"vnodeDir={vnodeDir}")
            if os.path.isdir(vnodeDir) == False or dnode[:5] != "dnode":
                continue
            # enum all vnode
            for entry in os.listdir(vnodeDir):
                entryPath = path.join(vnodeDir, entry)
                
                if os.path.isdir(entryPath):
                    if path.exists(path.join(entryPath, "vnode.json")):
                        vnode = VNode(int(dnode[5:]), entryPath, self.wal_period, self.wal_size, walStayRange)
                        vnodes.append(vnode)
        
        # do check
        for vnode in vnodes:
            vnode.check_retention()

    # consume topic 
    def consume_topic(self, topic_name, consume_cnt):
        print("start consume...")
        consumer = Consumer(
            {
                "group.id": "tg2",
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "enable.auto.commit": "true",
            }
        )
        print("start subscrite...")
        consumer.subscribe([topic_name])

        cnt = 0
        try:
            while True and cnt < consume_cnt:
                res = consumer.poll(1)
                if not res:
                    break
                err = res.error()
                if err is not None:
                    raise err
                val = res.value()
                cnt += 1
                print(f" consume {cnt} ")
                for block in val:
                    print(block.fetchall())
        finally:
            consumer.unsubscribe()
            consumer.close()


    # test db1
    def test_db(self, dbname, checkTime ,wal_period, wal_size_kb):
        # var        
        stable = "meters"
        tbname = "d"
        vgroups = 6
        count = 10

        # do 
        self.create_database(dbname, wal_period, wal_size_kb, vgroups)
        self.create_table(stable, tbname, count)

        # create tmq
        self.create_tmq()

        # insert data
        self.insert_data(tbname, checkTime)

        #stopInsert = False
        #tobj = threading.Thread(target = thread_insert, args=(self, tbname, rows))
        #tobj.start()

        # check retention 
        tdLog.info(f" -------------- do check retention ---------------")
        self.check_retention(walStayRange = 256)


        # stop insert and wait exit
        tdLog.info(f" {dbname} stop insert ...")
        tdLog.info(f" {dbname} test_db end.")


    # run
    def run(self):
        # period
        #self.test_db("db1", 10, 60, 0)
        # size
        #self.test_db("db2", 5, 10*24*3600, 2*1024) # 2M size
        
        # period + size        
        self.test_db("db", checkTime = 3*60, wal_period = 60, wal_size_kb=500)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
