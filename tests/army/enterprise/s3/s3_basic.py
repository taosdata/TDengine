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
# 192.168.1.52 MINIO S3 API KEY: MQCEIoaPGUs1mhXgpUAu:XTgpN2dEMInnYgqN4gj3G5zgb39ROtsisKKy0GFa
#

'''
s3EndPoint     http://192.168.1.52:9000
s3AccessKey    MQCEIoaPGUs1mhXgpUAu:XTgpN2dEMInnYgqN4gj3G5zgb39ROtsisKKy0GFa
s3BucketName   ci-bucket
s3UploadDelaySec 60
'''


class TDTestCase(TBase):
    updatecfgDict = {
        's3EndPoint': 'http://192.168.1.52:9000', 
        's3AccessKey': 'MQCEIoaPGUs1mhXgpUAu:XTgpN2dEMInnYgqN4gj3G5zgb39ROtsisKKy0GFa', 
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
        etool.runBenchmark(json=json)

        tdSql.execute(f"use {self.db}")
        # set insert data information
        self.childtable_count = 4
        self.insert_rows = 1000000
        self.timestamp_step = 1000

    def doAction(self):
        tdLog.info(f"do action.")
        self.flushDb()
        self.compactDb()

        # sleep 70s
        tdLog.info(f"wait 65s ...")
        time.sleep(65)
        self.trimDb(True)

        rootPath = sc.clusterRootPath()
        cmd = f"ls {rootPath}/dnode1/data20/vnode/vnode*/tsdb/*.data"
        tdLog.info(cmd)
        loop = 0
        while len(eos.runRetList(cmd)) > 0 and loop < 40:
            time.sleep(5)
            self.trimDb(True)
            loop += 1

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")
        if eos.isArm64Cpu():
            tdLog.success(f"{__file__} arm64 ignore executed")
            return

        # insert data
        self.insertData()

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

        # drop database and free s3 file
        self.dropDb()

        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
