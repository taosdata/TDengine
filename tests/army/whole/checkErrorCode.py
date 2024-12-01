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
        # PRE_DEFINE
        PRE_DEFINE = "#define TSDB_CODE_"
        n = len(PRE_DEFINE)
        if line[:n] != PRE_DEFINE:
            return None
        # MID_DEFINE
        MID_DEFINE = "TAOS_DEF_ERROR_CODE(0, 0x"
        pos = line.find(MID_DEFINE, n)
        if pos == -1:
            tdLog.info(f"not found \"{MID_DEFINE}\" line={line}")
            return None        
        start = pos + len(MID_DEFINE)
        code = line[start:].strip()
        # )
        pos = code.find(")")
        if pos == -1:
            tdLog.info(f"not found \")\", code={code}")
            return None
        # check len
        code = code[:pos]
        if len(code) != 4:
            tdLog.info(f"code is len not 4  len:{len(code)} subcode={code}\")\", line={line}")
            return None

        # return 
        return "0x8000" + code

    # ignore error
    def ignoreCode(self, code):
        ignoreCodes = {"0x0000000"}
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
                #print(code)
                # valid    
                if code == "0x8000000B":
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
        #print(cols)
        
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
                codes.append(code)

        # return
        return codes

    # check
    def checkConsistency(self, docCodes, codes, checkCol2=False, checkCol3=False):
        failed = 0
        # len
        hLen   = len(codes)
        docLen = len(docCodes)
        tdLog.info(f"head file codes items= {hLen} doc file codes items={docLen} \n")
        for i in range(hLen):
            problem = True
            action = "not found"
            for j in range(docLen):
                if codes[i] == docCodes[j][0]:
                    #tdLog.info(f" i={i} error code: {codes[i]} found in doc code:{docCodes[j][0]}\n")
                    problem = False
                    if docCodes[j][1] == "":
                        # describe col1 must not empty
                        problem = True
                        action = "found but \"Error Description\" column is empty"
                    # check col2 empty
                    elif checkCol2 and docCodes[j][2] == "":
                        problem = True
                        action = "found but \"Possible Cause\" column is empty"
                    # check col3 empty
                    elif checkCol3 and docCodes[j][3] == "":
                        problem = True
                        action = "found but \"Suggested Actions\" column is empty"
                    
                    # found stop search next
                    break

            if problem:
                tdLog.info(f" i={i} error code: {codes[i]} {action} in doc.")
                failed +=1

        # result
        if failed > 0:
            tdLog.exit(f"Check the consistency of error codes between header and doc. failed items={failed}. all items={hLen}\n")


    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # read head error code
        hFile = "../../include/util/taoserror.h"
        codes = self.readHeadCodes(hFile)

        # read zh codes
        zhDoc = "../../docs/zh/14-reference/09-error-code.md"
        zhCodes = self.readDocCodes(zhDoc)

        # read en codes
        enDoc = "../../docs/en/14-reference/09-error-code.md"
        enCodes = self.readDocCodes(enDoc)

        # check zh
        tdLog.info(f"check zh docs ...\n")
        self.checkConsistency(zhCodes, codes)

        # check en
        tdLog.info(f"check en docs ...\n")
        self.checkConsistency(enCodes, codes)

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
