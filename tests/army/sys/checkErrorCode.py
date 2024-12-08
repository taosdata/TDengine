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
import frame.eos
import frame.etime
import frame.etool
from frame.log import *
from frame.sql import *
from frame.cases import *
from frame.caseBase import *
from frame.srvCtl import *
from frame import *


class TDTestCase(TBase):
    # parse line
    def parseLine(self, line):
        line = line.strip()
        PRE_DEFINE = "#define TSDB_CODE_"
        n = len(PRE_DEFINE)
        if line[:n] != PRE_DEFINE:
            return None
        # TAOS_DEF_ERROR_CODE(0, 0x000B)
        pos = line.find("TAOS_DEF_ERROR_CODE(0, 0x", n)
        if pos == -1:
            tdLog.info(f"not found \"TAOS_DEF_ERROR_CODE(0, \" line={line}")
            return None
        
        code = line[pos:].strip()
        pos = code.find(")")
        if pos == -1:
            tdLog.info(f"not found \")\", line={line}")
            return None
        code = code[:pos]
        if len(code) != 4:
            tdLog.info(f"code is len not 4  len:{len(code)} subcode={code}\")\", line={line}")
            return None

        # return 
        return "0x8000" + code

    # ignore error
    def ignoreCode(self, code):
        ignoreCodes = {"0x00008, 0x000009"}
        if code in ignoreCodes:
            return True
        else:
            return False

    # read valid code
    def readHeadCodes(self, hFile):
        codes = []
        start = False
        # read
        with open(hFile) as file:
            for line in file:
                code = self.parseLine(line)
                # invalid
                if code == None:
                    continue
                # ignore
                if self.ignoreCode(code):
                    tdLog.info(f"ignore error {code}\n")
                # valid    
                if code == 0:
                    start = True
                if start:
                    codes.append(code)
        # return
        return codes
    
    # parse doc lines
    def parseDocLine(self, line):
        line = line.strip()
        PRE_DEFINE = "| 0x8000"
        n = len(PRE_DEFINE)
        if line[:n] != PRE_DEFINE:
            return None
        line = line[2:]
        cols = line.split("|")
        # remove blank
        cols = [col.strip() for col in cols]
        
        # return 
        return cols


    # read valid code
    def readDocCodes(self, docFile):
        codes = []
        start = False
        # read
        with open(docFile) as file:
            for line in file:
                code = self.parseDocLine(line)
                # invalid
                if code == None:
                    continue
                # valid    
                if start:
                    codes.append(code)
        # return
        return codes

    # check
    def checkConsistency(self, docCodes, codes):
        diff = False
        # len
        docLen = len(docCodes)
        len    = len(codes)
        tdLog.info("head file codes = {len} doc file codes={docLen} \n")

        if docLen > len:
            maxLen = docLen
        else:
            maxLen = len

        for i in range(maxLen):
            if i < len and i < docLen:
                if codes[i] == docCodes[i][0]:
                    tdLog.info(f" i={i} same head code: {codes[i]} doc code:{docCodes[i][0]}\n")
                else:
                    tdLog.info(f" i={i} diff head code: {codes[i]} doc code:{docCodes[i][0]}\n")
                    diff = True
            elif i < len:
                    tdLog.info(f" i={i} diff head code: {codes[i]} doc code: None\n")
                    diff = True
            elif i < docLen:
                    tdLog.info(f" i={i} diff head code: None doc code: {docCodes[i][0]}\n")
                    diff = True

        # result
        if diff:
            tdLog.exit("check error code consistency failed.\n")


    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # read head error code
        hFile = "../../include/util/taoserror.h"
        codes = self.readHeadCodes(hFile)

        # read zh codes
        zhDoc = "../../docs/zh/14-reference/09-error-code.md"
        zhCodes = self.readDocCodes(zhDoc, codes)

        # read en codes
        enDoc = "../../docs/en/14-reference/09-error-code.md"
        enCodes = self.readDocCodes(enDoc, codes)

        # check zh
        tdLog.info(f"check zh docs ...\n")
        self.checkConsistency(zhCodes, codes)

        # check en
        tdLog.info(f"check en docs ...\n")
        self.checkConsistency(enCodes, codes)

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
