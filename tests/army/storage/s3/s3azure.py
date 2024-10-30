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

import sys
import time
import random

import taos
import frame
import frame.etool
import frame.eos
import frame.eutil
import requests

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame.srvCtl import *
from frame import *
from frame.eos import *
from azure import Azure

#
# 192.168.1.52 MINIO S3
#

'''
s3EndPoint     http://192.168.1.52:9000
s3AccessKey    'zOgllR6bSnw2Ah3mCNel:cdO7oXAu3Cqdb1rUdevFgJMi0LtRwCXdWKQx4bhX'
s3BucketName   ci-bucket
s3UploadDelaySec 60

for test:
"s3AccessKey" : "fGPPyYjzytw05nw44ViA:vK1VcwxgSOykicx6hk8fL1x15uEtyDSFU3w4hTaZ"
"s3BucketName": "test-bucket"
'''


class TDTestCase(TBase):
    def __init__(self):
        self.fileName = ""  # track the upload of S3 file

        account_name = 'fd2d01cd892f844eeaa2273'
        url = "http://192.168.0.21/azure_account_key.txt"
        response = requests.get(url)
        account_key = response.text.strip()
        container_name = 'td-test'

        self.azure_class = Azure(account_name, account_key, container_name)

    # index = eutil.cpuRand(20) + 1
    bucketName = "td-test"
    updatecfgDict = {
        "supportVnodes": "1000",
        's3EndPoint': 'http://192.168.1.49,http://192.168.1.49:81',
        's3AccessKey': 'FlIOwdr5HAnsMyEZ6FBwSPE5:87x1tZJll1SaK4hoiglC8zPRhDgbMeW6ufQqEt8,FlIOvHz0MePAf4KttYqfZM:Hve86EvB6KdvRPa5d1DG38jP6BSSSJC7IxYvSgEP',
        's3BucketName': f'{bucketName}',
        's3PageCacheSize': '10240',
        "s3UploadDelaySec": "10",
        's3MigrateIntervalSec': '600',
        's3MigrateEnabled': '1'
    }

    tdLog.info(f"assign bucketName is {bucketName}\n")
    maxFileSize = (128 + 10) * 1014 * 1024  # add 10M buffer

    def insertData(self):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "s3Basic.json")
        etool.benchMark(json=json)

        tdSql.execute(f"use {self.db}")
        # come from s3_basic.json
        self.childtable_count = 6
        self.insert_rows = 2000000
        self.timestamp_step = 100

    def createStream(self, sname):
        sql = f"create stream {sname} fill_history 1 into stm1 as select count(*) from {self.db}.{self.stb} interval(10s);"
        tdSql.execute(sql)

    def migrateDbS3(self):
        sql = f"s3migrate database {self.db}"
        tdSql.execute(sql, show=True)

    def checkDataFile(self, lines, maxFileSize):
        # ls -l
        # -rwxrwxrwx 1 root root  41652224 Apr 17 14:47 vnode2/tsdb/v2f1974ver47.3.data
        overCnt = 0
        for line in lines:
            cols = line.split()
            fileSize = int(cols[4])
            fileName = cols[8]
            # print(f" filesize={fileSize} fileName={fileName}  line={line}")
            if fileSize > maxFileSize:
                tdLog.info(f"error, {fileSize} over max size({maxFileSize}) {fileName}\n")
                overCnt += 1
                self.fileName = fileName
            else:
                tdLog.info(f"{fileName}({fileSize}) check size passed.")

        return overCnt

    def checkUploadToS3(self):
        rootPath = sc.clusterRootPath()
        cmd = f"ls -l {rootPath}/dnode*/data/vnode/vnode*/tsdb/*.data"
        tdLog.info(cmd)
        loop = 0
        rets = []
        overCnt = 0
        while loop < 200:
            time.sleep(3)

            # check upload to s3
            rets = eos.runRetList(cmd)
            cnt = len(rets)
            if cnt == 0:
                overCnt = 0
                tdLog.info("All data file upload to server over.")
                break
            overCnt = self.checkDataFile(rets, self.maxFileSize)
            if overCnt == 0:
                uploadOK = True
                tdLog.info(f"All data files({len(rets)}) size bellow {self.maxFileSize}, check upload to s3 ok.")
                break

            tdLog.info(f"loop={loop} no upload {overCnt} data files wait 3s retry ...")
            if loop == 3:
                sc.dnodeStop(1)
                time.sleep(2)
                sc.dnodeStart(1)
            loop += 1
            # migrate
            self.migrateDbS3()

        # check can pass
        if overCnt > 0:
            tdLog.exit(f"s3 have {overCnt} files over size.")

    def doAction(self):
        tdLog.info(f"do action.")

        self.flushDb(show=True)
        # self.compactDb(show=True)

        # sleep 70s
        self.migrateDbS3()

        # check upload to s3
        self.checkUploadToS3()

    def checkAzureDataExist(self):
        result = self.azure_class.blob_list()
        if not self.fileName:
            tdLog.exit("cannot find S3 file")
        datafile = self.fileName.split("/")[-1].split(".")[0]
        for blob in result["Blobs"]["Blob"]:
            if datafile in blob["Name"]:
                tdLog.info("Successfully found the file %s in Azure" % self.fileName)
                break
        else:
            tdLog.exit("failed found the file %s in Azure" % self.fileName)

    def checkAzureDataNotExist(self):
        datafile = self.fileName.split("/")[-1].split(".")[0]

        result = self.azure_class.blob_list()
        for blob in result["Blobs"]["Blob"]:
            if datafile in blob["Name"]:
                tdLog.exit("failed delete the file %s in Azure" % self.fileName)
                break
        else:
            tdLog.info("Successfully delete the file %s in Azure" % self.fileName)

    def checkStreamCorrect(self):
        sql = f"select count(*) from {self.db}.stm1"
        count = 0
        for i in range(120):
            tdSql.query(sql)
            count = tdSql.getData(0, 0)
            if count == 100000 or count == 100001:
                return True
            time.sleep(1)

        tdLog.exit(f"stream count is not expect . expect = 100000 or 100001 real={count} . sql={sql}")

    def checkCreateDb(self, keepLocal, chunkSize, compact):
        # keyword
        kw1 = kw2 = kw3 = ""
        if keepLocal is not None:
            kw1 = f"s3_keeplocal {keepLocal}"
        if chunkSize is not None:
            kw2 = f"s3_chunkpages {chunkSize}"
        if compact is not None:
            kw3 = f"s3_compact {compact}"

        sql = f" create database db1 vgroups 1 duration 1h {kw1} {kw2} {kw3}"
        tdSql.execute(sql, show=True)
        # sql = f"select name,s3_keeplocal,s3_chunkpages,s3_compact from information_schema.ins_databases where name='db1';"
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

    def checkExcept(self):
        # errors
        sqls = [
            f"create database db2 s3_keeplocal -1",
            f"create database db2 s3_keeplocal 0",
            f"create database db2 s3_keeplocal 365001",
            f"create database db2 s3_chunkpages -1",
            f"create database db2 s3_chunkpages 0",
            f"create database db2 s3_chunkpages 900000000",
            f"create database db2 s3_compact -1",
            f"create database db2 s3_compact 100",
            f"create database db2 duration 1d s3_keeplocal 1d"
        ]
        tdSql.errors(sqls)

    def checkBasic(self):
        # create db
        keeps = [1, 256, 1024, 365000, None]
        chunks = [131072, 600000, 820000, 1048576, None]
        comps = [0, 1, None]

        for keep in keeps:
            for chunk in chunks:
                for comp in comps:
                    self.checkCreateDb(keep, chunk, comp)

        # --checks3
        idx = 1
        taosd = sc.taosdFile(idx)
        cfg = sc.dnodeCfgPath(idx)
        cmd = f"{taosd} -c {cfg} --checks3"

        eos.exe(cmd)
        # output, error = eos.run(cmd)
        # print(lines)

        '''
        tips = [
            "put object s3test.txt: success",
            "listing bucket ci-bucket: success",
            "get object s3test.txt: success",
            "delete object s3test.txt: success"
        ]
        pos = 0
        for tip in tips:
            pos = output.find(tip, pos)
            #if pos == -1:
            #    tdLog.exit(f"checks3 failed not found {tip}. cmd={cmd} output={output}")
        '''

        # except
        self.checkExcept()

    #
    def preDb(self, vgroups):
        cnt = int(time.time()) % 2 + 1
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
        json = etool.curFile(__file__, "s3Basic1.json")
        etool.benchMark(json=json)

        # come from s3_basic.json
        self.insert_rows += self.insert_rows / 4
        self.timestamp_step = 50

    # delete
    def checkDelete(self):
        # del 1000 rows
        start = 1600000000000
        drows = 200
        for i in range(1, drows, 2):
            sql = f"from {self.db}.{self.stb} where ts = {start + i * 500}"
            tdSql.execute("delete " + sql, show=True)
            tdSql.query("select * " + sql)
            tdSql.checkRows(0)

        # delete all 500 step
        self.flushDb()
        self.compactDb()
        self.insert_rows -= drows / 2
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

        self.insert_rows -= drows
        sql = f"select count(*) from {self.db}.{self.stb}"
        tdSql.checkAgg(sql, self.insert_rows * self.childtable_count)

    # run
    def run(self):
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
            # self.checkInsertCorrect()

            # save
            self.snapshotAgg()

            # do action
            self.doAction()
            # check azure data exist
            self.checkAzureDataExist()
            # check save agg result correct
            self.checkAggCorrect()

            # check insert correct again
            self.checkInsertCorrect()

            # check stream correct and drop stream
            # self.checkStreamCorrect()

            # drop stream
            self.dropStream(self.sname)

            # drop database and free s3 file
            self.dropDb()
            # check azure data not exist
            self.checkAzureDataNotExist()

            tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
