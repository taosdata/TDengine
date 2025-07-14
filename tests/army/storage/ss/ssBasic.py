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

from new_test_framework.utils import tdLog, tdSql, epath, sc, etool, eutil, eos
import os
import time
import random
import string
import taos

#  
# 192.168.1.52 MINIO
#

'''
Common:
    ssEnabled               : 2,
    ssPageCacheSize         : 10240,
    ssUploadDelaySec        : 10,
    ssAutoMigrateIntervalSec: 600,

ssAccessString Common:
    Endpoint        : 192.168.1.52:9000
    Protocol        : http
    UriStyle        : path
    ChunkSize       : 64MB
    MaxChunks       : 10000
    MaxRetry        : 3

ssAccessTring For CI:
    Bucket          : ci-bucket
    AccessKeyId     : zOgllR6bSnw2Ah3mCNel
    SecretAccessKey : cdO7oXAu3Cqdb1rUdevFgJMi0LtRwCXdWKQx4bhX

ssAccessString For Test:
    Bucket          : test-bucket
    AccessKeyId     : fGPPyYjzytw05nw44ViA
    SecretAccessKey : vK1VcwxgSOykicx6hk8fL1x15uEtyDSFU3w4hTaZ
'''

<<<<<<<< HEAD:test/cases/uncatalog/army/storage/s3/test_s3_basic.py

class TestS3Basic:
========
class TDTestCase(TBase):
>>>>>>>> 3.0:tests/army/storage/ss/ssBasic.py
    index = eutil.cpuRand(40) + 1
    bucketName = f"ci-bucket{index}"
    updatecfgDict = {
        "supportVnodes":"1000",
        "ssEnabled": "2",
        "ssAccessString": f's3:endpoint=192.168.1.52:9000;bucket={bucketName};uriStyle=path;protocol=http;accessKeyId=zOgllR6bSnw2Ah3mCNel;secretAccessKey=cdO7oXAu3Cqdb1rUdevFgJMi0LtRwCXdWKQx4bhX;chunkSize=64;maxChunks=10000;maxRetry=3',
        'ssPageCacheSize': '10240',
        "ssUploadDelaySec": "10",
        'ssAutoMigrateIntervalSec': '600',
    }

    tdLog.info(f"assign bucketName is {bucketName}\n")
    maxFileSize = (128 + 10) * 1014 * 1024 # add 10M buffer

    def exit(self, log):
        self.dropDb(True)
        tdLog.exit(log)

    def insertData(self):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "ssBasic.json")
        etool.benchMark(json=json)

        tdSql.execute(f"use {self.db}")
        # come from ss_basic.json
        self.childtable_count = 6
        self.insert_rows = 2000000
        self.timestamp_step = 100

    def createStream(self, sname):
        sql = f"create stream {sname} fill_history 1 into stm1 as select count(*) from {self.db}.{self.stb} interval(10s);"
        tdSql.execute(sql)

    def migrateDbSs(self):
        sql = f"ssmigrate database {self.db}"
        tdSql.execute(sql, queryTimes=60, show=True)

    def checkDataFile(self, lines, maxFileSize):
        # ls -l
        # -rwxrwxrwx 1 root root  41652224 Apr 17 14:47 vnode2/tsdb/v2f1974ver47.3.data
        overCnt = 0
        for line in lines:
            cols = line.split()
            fileSize = int(cols[4])
            fileName = cols[8]
            #print(f" filesize={fileSize} fileName={fileName}  line={line}")
            if fileSize > maxFileSize:
                tdLog.info(f"error, {fileSize} over max size({maxFileSize}) {fileName}\n")
                overCnt += 1
            else:
                tdLog.info(f"{fileName}({fileSize}) check size passed.")
                
        return overCnt

    def checkUploadToSs(self):
        rootPath = sc.clusterRootPath()
        cmd = f"ls -l {rootPath}/dnode*/data/vnode/vnode*/tsdb/*.data"
        tdLog.info(cmd)
        loop = 0
        rets = []
        overCnt = 0
        while loop < 150:
            time.sleep(2)

            # check upload to ss
            rets = eos.runRetList(cmd)
            cnt = len(rets)
            if cnt == 0:
                overCnt = 0
                tdLog.info("All data file upload to server over.")
                break
            overCnt = self.checkDataFile(rets, self.maxFileSize)
            if overCnt == 0:
                uploadOK = True
                tdLog.info(f"All data files({len(rets)}) size bellow {self.maxFileSize}, check upload to ss ok.")
                break

            tdLog.info(f"loop={loop} no upload {overCnt} data files wait 3s retry ...")
            if loop == 3:
                sc.dnodeStop(1)
                time.sleep(2)
                sc.dnodeStart(1)
            loop += 1
            # migrate
            self.migrateDbSs()
                
        # check can pass
        if overCnt > 0:
            self.exit(f"ss have {overCnt} files over size.")


    def doAction(self):
        tdLog.info(f"do action.")

        self.flushDb(show=True)
        #self.compactDb(show=True)

        # sleep 70s
        self.migrateDbSs()

        # check upload to ss
        self.checkUploadToSs()

    def checkStreamCorrect(self):
        sql = f"select count(*) from {self.db}.stm1"
        count = 0
        for i in range(120):
            tdSql.query(sql)
            count = tdSql.getData(0, 0)
            if count == 100000 or count == 100001:
                return True
            time.sleep(1)
            
        self.exit(f"stream count is not expect . expect = 100000 or 100001 real={count} . sql={sql}")


    def checkCreateDb(self, keepLocal, chunkSize, compact):
        # keyword
        kw1 = kw2 = kw3 = "" 
        if keepLocal is not None:
            kw1 = f"ss_keeplocal {keepLocal}"
        if chunkSize is not None:
            kw2 = f"ss_chunkpages {chunkSize}"
        if compact is not None:
            kw3 = f"ss_compact {compact}"    

        sql = f" create database db1 vgroups 1 duration 1h {kw1} {kw2} {kw3}"
        tdSql.execute(sql, show=True)
        #sql = f"select name,ss_keeplocal,ss_chunkpages,ss_compact from information_schema.ins_databases where name='db1';"
        sql = f"select * from information_schema.ins_databases where name='db1';"
        tdSql.query(sql)
        # 29 30 31 -> chunksize keeplocal compact
        if chunkSize is not None:
            tdSql.checkData(0, 29, chunkSize)
        if keepLocal is not None:
            keepLocalm = keepLocal * 24 * 60
            tdSql.checkData(0, 30, f"{keepLocalm}m")
        if compact is not None:    
            tdSql.checkData(0, 31, compact)
        sql = "drop database db1"
        tdSql.execute(sql)

    def check_except(self):
        # errors
        sqls = [
            f"create database db2 ss_keeplocal -1",
            f"create database db2 ss_keeplocal 0",
            f"create database db2 ss_keeplocal 365001",
            f"create database db2 ss_chunkpages -1",
            f"create database db2 ss_chunkpages 0",
            f"create database db2 ss_chunkpages 900000000",
            f"create database db2 ss_compact -1",
            f"create database db2 ss_compact 100",
            f"create database db2 duration 1d ss_keeplocal 1d"
        ]
        tdSql.errors(sqls)


    def checkBasic(self):
        # create db
        keeps  = [1, 256, 1024, 365000, None]
        chunks = [131072, 600000, 820000, 1048576, None]
        comps  = [0, 1, None]

        for keep in keeps:
            for chunk in chunks:
                for comp in comps:
                    self.checkCreateDb(keep, chunk, comp)

        
        # --checkss
        idx = 1
        taosd = sc.taosdFile(idx)
        cfg   = sc.dnodeCfgPath(idx)
        cmd = f"{taosd} -c {cfg} --checkss"

        eos.exe(cmd)
        #output, error = eos.run(cmd)
        #print(lines)

        '''
        tips = [
            "put object sstest.txt: success",
            "listing bucket ci-bucket: success",
            "get object sstest.txt: success",
            "delete object sstest.txt: success"
        ]
        pos = 0
        for tip in tips:
            pos = output.find(tip, pos)
            #if pos == -1:
            #    tdLog.exit(f"checkss failed not found {tip}. cmd={cmd} output={output}")
        '''
        
        # except
        self.check_except()

    #
    def preDb(self, vgroups):
        cnt = int(time.time())%2 + 1
        for i in range(cnt):
             vg = eutil.cpuRand(9) + 1
             sql = f"create database predb vgroups {vg}"
             tdSql.execute(sql, show=True)
             sql = "drop database predb"
             tdSql.execute(sql, show=True)

    # history
    def insertHistory(self):
        tdLog.info(f"insert history data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "ssBasic_History.json")
        etool.benchMark(json=json)

        # come from ss_basic.json
        self.insert_rows += self.insert_rows/4
        self.timestamp_step = 50

    # delete
    def checkDelete(self):
        # del 1000 rows
        start = 1600000000000
        drows = 200
        for i in range(1, drows, 2):
            sql = f"from {self.db}.{self.stb} where ts = {start + i*500}"
            tdSql.execute("delete " + sql, show=True)
            tdSql.query("select * " + sql)
            tdSql.checkRows(0)
        
        # delete all 500 step
        self.flushDb()
        self.compactDb()
        self.insert_rows   -= drows/2
        sql = f"select count(*) from {self.db}.{self.stb}"
        tdSql.checkAgg(sql, self.insert_rows * self.childtable_count)

        # delete 10W rows from 100000
        drows = 100000
        sdel = start + 100000 * self.timestamp_step
        edel = start + 100000 * self.timestamp_step + drows * self.timestamp_step
        sql = f"from {self.db}.{self.stb} where ts >= {sdel} and ts < {edel}"
        tdSql.execute("delete " + sql, show=True)
        tdSql.query("select * " + sql)
        tdSql.checkRows(0)

        self.insert_rows   -= drows
        sql = f"select count(*) from {self.db}.{self.stb}"
        tdSql.checkAgg(sql, self.insert_rows * self.childtable_count)
        

    # run
    def test_s3_basic(self):
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
        tdLog.debug(f"start to excute {__file__}")
        self.sname = "stream1"
        if eos.isArm64Cpu():
            tdLog.success(f"{__file__} arm64 ignore executed")
        else:
            
            self.preDb(10)

            # insert data
            self.insertData()

            # creat stream
            self.createStream(self.sname)

            # check insert data correct
            #self.checkInsertCorrect()

            # save
            self.snapshotAgg()

            # do action
            self.doAction()

            # check save agg result correct
            self.checkAggCorrect()

            # check insert correct again
            self.checkInsertCorrect()


            # check stream correct and drop stream
            #self.checkStreamCorrect()

            # drop stream
            self.dropStream(self.sname)

            # insert history  disorder data
            self.insertHistory()

            # checkBasic
            self.checkBasic()

            #self.checkInsertCorrect()
            self.snapshotAgg()
            self.doAction()
            self.checkAggCorrect()
            self.checkInsertCorrect(difCnt=self.childtable_count*1499999)
            self.checkDelete()
            self.doAction()

            # drop database and free ss file
            self.dropDb()


            tdLog.success(f"{__file__} successfully executed")

        

