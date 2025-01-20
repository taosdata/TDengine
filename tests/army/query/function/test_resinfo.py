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
import hashlib

import taos
import frame
import frame.etool

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *

initial_hash_resinfoInt = "fbfd69d6f0aa6e015a7b5475b33ee8c8"
initial_hash_resinfo = "172d04aa7af0d8cd2e4d9df284079958"

class TDTestCase(TBase):
    def get_file_hash(self, file_path):
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            buf = f.read()
            hasher.update(buf)
        return hasher.hexdigest()

    def testFileChanged(self):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        resinfoIntFile = etool.curFile(__file__, "../../../../source/libs/function/inc/functionResInfoInt.h")
        resinfoFile = etool.curFile(__file__, "../../../../include/libs/function/functionResInfo.h")
        current_hash = self.get_file_hash(resinfoIntFile)
        tdLog.info(current_hash)
        if current_hash != initial_hash_resinfoInt:
            tdLog.exit(f"{resinfoIntFile} has been modified.")
        else:
            tdLog.success(f"{resinfoIntFile} is not modified.")
        current_hash = self.get_file_hash(resinfoFile)
        if current_hash != initial_hash_resinfo:
            tdLog.exit(f"{resinfoFile} has been modified.")
        else:
            tdLog.success(f"{resinfoFile} is not modified.")



    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # insert data
        self.testFileChanged()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
