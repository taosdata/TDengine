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
import frame.etool

from frame.log import *
from frame.sql import *
from frame.cases import *
from frame.caseBase import *
from frame.srvCtl import *
from frame import *


class TDTestCase(TBase):
    def insertData(self):
        tdLog.info(f"insert data.")

        # set insert data information
        self.childtable_count = 10
        self.insert_rows      = 10000
        self.timestamp_step   = 1000

        # taosBenchmark run
        etool.benchMark(command = f"-d {self.db} -t {self.childtable_count} -n {self.insert_rows} -v 2 -y")

    def doTaos(self):
        tdLog.info(f"check taos command options...")


    def doTaosd(self):
        tdLog.info(f"check taosd command options...")
        idx = 1 # dnode1
        cfg = sc.dnodeCfgPath(idx)
        
        # -s
        sdb = "./sdb.json"
        eos.delFile(sdb)
        etool.runBinFile("taosd", f"-s -c {cfg}")
        self.checkFileExist(sdb)

        # -C
        etool.runBinFile("taosd", "-C")
        # -k 
        rets = etool.runBinFile("taosd", "-C")
        self.checkListNotEmpty(rets)
        # -V
        rets = etool.runBinFile("taosd", "-V")
        self.checkListNotEmpty(rets)
        # --help
        rets = etool.runBinFile("taosd", "--help")
        self.checkListNotEmpty(rets)

        # except input
        etool.exeBinFile("taosd", "-c")
        etool.exeBinFile("taosd", "-e")

        # stop taosd
        sc.dnodeStop(idx)
        # other
        etool.exeBinFile("taosd", f"-dm -c {cfg}")
        sc.dnodeStop(idx)
        etool.exeBinFile("taosd", "-a http://192.168.1.10")

        #exe 
        etool.exeBinFile("taosd", f"-E abc -c {cfg}")
        sc.dnodeStop(idx)
        etool.exeBinFile("taosd", f"-e def -c {cfg}")


    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # insert data
        self.insertData()

        # do taos
        self.doTaos()

        # do action
        self.doTaosd()


        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
