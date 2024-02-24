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

import taos
import frame
import frame.etool
import frame.eos

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame.srvCtl import *
from frame import *
from frame.eos import *

#  
# 192.168.1.52 MINIO S3 
#

'''
s3EndPoint     http://192.168.1.52:9000
s3AccessKey    'zOgllR6bSnw2Ah3mCNel:cdO7oXAu3Cqdb1rUdevFgJMi0LtRwCXdWKQx4bhX'
s3BucketName   ci-bucket
s3UploadDelaySec 60
'''


class TDTestCase(TBase):
    updatecfgDict = {
        's3EndPoint': 'http://192.168.1.52:9000', 
        's3AccessKey': 'zOgllR6bSnw2Ah3mCNel:cdO7oXAu3Cqdb1rUdevFgJMi0LtRwCXdWKQx4bhX', 
        's3BucketName': 'ci-bucket',
        's3BlockSize': '10240',
        's3BlockCacheSize': '320',
        's3PageCacheSize': '10240',
        's3UploadDelaySec':'60'
    }    

    def insertData(self):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "s3_basic.json")
        etool.benchMark(json=json)

        tdSql.execute(f"use {self.db}")
        # come from s3_basic.json
        self.childtable_count = 4
        self.insert_rows = 1000000
        self.timestamp_step = 1000

    def createStream(self, sname):
        sql = f"create stream {sname} fill_history 1 into stm1 as select count(*) from {self.db}.{self.stb} interval(10s);"
        tdSql.execute(sql)

    def doAction(self):
        tdLog.info(f"do action.")

        self.flushDb()
        self.compactDb()

        # sleep 70s
        tdLog.info(f"wait 65s ...")
        time.sleep(65)
        self.trimDb(True)

        rootPath = sc.clusterRootPath()
        cmd = f"ls {rootPath}/dnode1/data2*/vnode/vnode*/tsdb/*.data"
        tdLog.info(cmd)
        loop = 0
        rets = []
        while loop < 180:
            time.sleep(3)
            rets = eos.runRetList(cmd)
            cnt = len(rets)
            if cnt == 0:
                tdLog.info("All data file upload to server over.")
                break            
            self.trimDb(True)
            tdLog.info(f"loop={loop} no upload {cnt} data files wait 3s retry ...")
            if loop == 0:
                sc.dnodeStop(1)
                time.sleep(2)
                sc.dnodeStart(1)
            loop += 1
                
        if len(rets) > 0:
            tdLog.exit(f"s3 can not upload all data to server. data files cnt={len(rets)} list={rets}")

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

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")
        self.sname = "stream1"
        if eos.isArm64Cpu():
            tdLog.success(f"{__file__} arm64 ignore executed")
        else:
            # insert data
            self.insertData()

            # creat stream
            self.createStream(self.sname)

            # check insert data correct
            self.checkInsertCorrect()

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

            # drop database and free s3 file
            self.dropDb()

            tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
