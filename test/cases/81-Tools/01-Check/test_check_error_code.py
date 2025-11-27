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

from new_test_framework.utils import tdLog, tdSql, epath, sc, tdFindPath



ignoreCodes = [
    '0x80000023', '0x80000024', '0x80000025', '0x80000026', '0x80000027', '0x80000028', '0x80000029', '0x8000002A', '0x80000109', '0x80000110', 
    '0x80000129', '0x8000012C', '0x8000012D', '0x8000012E', '0x8000012F', '0x80000136', '0x80000137', '0x80000138', '0x80000139', '0x8000013A', 
    '0x8000013B', '0x80000200', '0x80000201', '0x80000202', '0x80000203', '0x80000204', '0x80000205', '0x80000206', '0x8000020B', '0x8000020E', 
    '0x80000210', '0x80000212', '0x80000214', '0x80000215', '0x80000217', '0x8000021B', '0x8000021C', '0x8000021D', '0x8000021E', '0x80000220', 
    '0x80000221', '0x8000022C', '0x80000232', '0x80000233', '0x80000234', '0x80000235', '0x80000236', '0x800002FF', '0x80000300', '0x80000316', 
    '0x80000317', '0x80000338', '0x80000339', '0x8000033F', '0x80000343', '0x80000345', '0x80000356', '0x80000359', '0x8000035A', '0x8000035B', 
    '0x8000035C', '0x8000035D', '0x80000362', '0x8000038C', '0x8000038E', '0x8000039B', '0x8000039C', '0x8000039D', '0x8000039E', '0x800003A6', 
    '0x800003A7', '0x800003AA', '0x800003AB', '0x800003AC', '0x800003AD', '0x800003B0', '0x800003B2', '0x800003B4', '0x800003B5', '0x800003BA', 
    '0x800003C0', '0x800003C1', '0x800003D0', '0x800003D8', '0x800003D9', '0x800003DA', '0x800003DB', '0x800003DC', '0x800003E2', '0x800003F4', '0x800003F8', '0x80000412', '0x80000413', 
    '0x80000414', '0x80000415', '0x80000416', '0x80000417', '0x80000418', '0x80000419', '0x80000420', '0x80000421', '0x80000422', '0x80000423', 
    '0x80000424', '0x80000425', '0x80000426', '0x80000427', '0x80000428', '0x80000429', '0x8000042A', '0x8000042B', '0x80000430', '0x80000431', 
    '0x80000432', '0x80000433', '0x80000434', '0x80000435', '0x80000436', '0x80000437', '0x80000438', '0x80000440', '0x80000441', '0x80000442', 
    '0x80000443', '0x80000444', '0x80000445', '0x80000446', '0x80000447', '0x80000485', '0x80000486', '0x80000490', '0x80000491', '0x80000492',
    '0x80000493', '0x80000494', '0x800004A0', '0x800004A1', '0x800004B1', '0x800004B2', '0x800004B3', '0x800004C1', '0x800004C2', '0x800004C3', '0x800004CA',
    '0x800004CB', '0x800004D0', '0x800004D1', '0x800004D2', '0x800004D3', '0x800004D4', '0x800004D5', '0x800004D6', '0x800004D7', '0x800004D8',
    '0x800004D9', '0x800004DA', '0x80000504', '0x80000528', '0x80000532', '0x80000533', '0x80000534', '0x80000535', '0x80000536', '0x80000537', '0x80000538', 
    '0x80000539', '0x80000541', '0x80000601', '0x80000607', '0x8000060A', '0x8000060D', '0x8000060E', '0x8000060F', '0x80000610', '0x80000611', '0x80000612', 
    '0x80000613', '0x80000614', '0x80000615', '0x80000616', '0x8000061C', '0x8000061E', '0x80000701', '0x80000705', '0x80000706', '0x80000707', 
    '0x80000708', '0x8000070B', '0x8000070C', '0x8000070E', '0x8000070F', '0x80000712', '0x80000723', '0x80000724', '0x80000725', '0x80000726', 
    '0x80000727', '0x80000728', '0x8000072A', '0x8000072C', '0x8000072D', '0x8000072E', '0x80000730', '0x80000731', '0x80000732', '0x80000733', 
    '0x80000734', '0x80000735', '0x80000736', '0x80000737', '0x80000738', '0x8000080E', '0x8000080F', '0x80000810', '0x80000811', '0x80000812', 
    '0x80000813', '0x80000814', '0x80000815', '0x80000816', '0x80000817', '0x80000818', '0x80000819', '0x80000820', '0x80000821', '0x80000822', 
    '0x80000823', '0x80000824', '0x80000825', '0x80000826', '0x80000827', '0x80000828', '0x80000829', '0x8000082A', '0x8000082B', '0x8000082C', 
    '0x8000082D', '0x8000082E', '0x8000082F', '0x80000830', '0x80000831', '0x80000832', '0x80000833', '0x80000834', '0x80000835', '0x80000836', '0x80000837', '0x80000838', '0x80000839', '0x80000907', '0x80000919', '0x8000091A', 
    '0x8000091B', '0x8000091C', '0x8000091D', '0x8000091E', '0x8000091F', '0x80000920', '0x80000921', '0x80000922', '0x80000A00',
    '0x80000A01', '0x80000A03', '0x80000A06', '0x80000A07', '0x80000A08', '0x80000A09', '0x80000A0A', '0x80000A0B', '0x80000A0E', '0x80002206', 
    '0x80002207', '0x80002406', '0x80002407', '0x80002503', '0x80002506', '0x80002507', '0x8000261B', '0x80002653', '0x80002668', '0x80002669',
    '0x8000266A', '0x8000266B', '0x8000266C', '0x8000266D', '0x8000266E', '0x8000266F', '0x80002670', '0x80002671', '0x80002672', '0x80002673', 
    '0x80002674', '0x80002675', '0x80002676', '0x80002677', '0x80002678', '0x80002679', '0x8000267A', '0x8000267B', '0x8000267C', '0x8000267D', 
    '0x8000267E', '0x8000267F', '0x80002680', '0x80002681', '0x80002682', '0x80002683', '0x80002684', '0x80002685', '0x80002686', '0x80002703', '0x80002806',
    '0x80002807', '0x80002808', '0x80002809', '0x8000280A', '0x8000280B', '0x8000280C', '0x8000280D', '0x8000280E', '0x8000280F', '0x80002810', 
    '0x80002811', '0x80002812', '0x80002813', '0x80002814', '0x80002815', '0x8000290B', '0x80002920', '0x80003003', '0x80003006', '0x80003106', 
    '0x80003107', '0x80003108', '0x80003109', '0x80003110', '0x80003111', '0x80003112', '0x80003159', '0x80003160', '0x80003161', '0x80003162',
    '0x80003250', '0x80004003', '0x80004004', '0x80004005',
    '0x80004006', '0x80004007', '0x80004008', '0x80004009', '0x80004010', '0x80004011', '0x80004012', '0x80004013', '0x80004014', '0x80004015', 
    '0x80004016', '0x80004102', '0x80004103', '0x80004104', '0x80004105', '0x80004106', '0x80004107', '0x80004108', '0x80004109', '0x80005100', 
    '0x80005101', '0x80006000', '0x80006100', '0x80006101', '0x80006102', '0x80000019', '0x80002639', '0x80002666', '0x80000237', '0x80004018',
    '0x80006300', '0x80006301', '0x80006302', '0x80006303', '0x80006304', '0x80006305', '0x80006306', '0x80004019', '0x8000042C', '0x80002691', 
    '0x80002692', '0x80002693', '0x8000410A', '0x8000410B', '0x8000410C', '0x8000410D', '0x8000410E', '0x8000410F', '0x80004110', '0x80004111',
    '0x80004112', '0x80004113', '0x80004114', '0x80004115', '0x80004116', '0x80004117', '0x80007000', '0x80007001', '0x80007002', '0x80007003',
    '0x80007004', '0x80007005', '0x80007006', '0x80007007', '0x80007008', '0x80007009', '0x8000700A', '0x8000700B', '0x8000700C', '0x8000700D',
    '0x8000700E', '0x8000700F', '0x80007010', '0x80007011', '0x80007012', '0x80007013', '0x80007014', '0x80002694', '0x80002695', '0x80007015',
    '0x800004B4', '0x800004B5', '0x800004B6', '0x80006001']


class TestCheckErrorCode:
    # parse line
    def parseLine(self, line, show):
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
            if show:
                tdLog.info(f"not found \"{MID_DEFINE}\" line={line}")
            return None        
        start = pos + len(MID_DEFINE)
        code = line[start:].strip()
        # )
        pos = code.find(")")
        if pos == -1:
            if show:
                tdLog.info(f"not found \")\", code={code}")
            return None
        # check len
        code = code[:pos]
        if len(code) != 4:
            if show:
                tdLog.info(f"code is len not 4  len:{len(code)} subcode={code}\")\", line={line}")
            return None

        # return 
        return "0x8000" + code

    # ignore error
    def ignoreCode(self, code):
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
                code = self.parseLine(line, start)
                # invalid
                if code == None:
                    continue
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
        with open(docFile, encoding="utf-8", errors="ignore") as file:
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
    def check_consistency(self, docCodes, codes, checkCol2=True, checkCol3=True):
        failed = 0
        ignored = 0
        # len
        hLen   = len(codes)
        docLen = len(docCodes)
        errCodes = []
        #tdLog.info(f"head file codes items= {hLen} doc file codes items={docLen} \n")
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
                # ignore
                if self.ignoreCode(codes[i]):
                    #tdLog.info(f" i={i} error code: {codes[i]} ignore ...")
                    ignored += 1
                else:
                    tdLog.info(f" i={i} error code: {codes[i]} {action} in doc.")
                    failed +=1
                    errCodes.append(codes[i])

        print(errCodes)
        # result
        if failed > 0:
            tdLog.exit("Failed to check the consistency of error codes between header and doc. "
                       f"failed:{failed}, ignored:{ignored}, all:{hLen}\n")
        
        tdLog.info(f"Check consistency successfully.  ok items={hLen - ignored}, ignored items={ignored}\n")


    # run
    def test_check_error_code(self):
        """Check the consistency of error codes between header file and doc files.

        1. Read all error codes from include/util/taoserror.h
        2. Read all error codes from docs/zh/14-reference/09-error-code.md
        3. Read all error codes from docs/en/14-reference/09-error-code.md
        4. Check whether all error codes in header file are documented in both doc files
        5. Check whether the description, possible cause and suggested actions are provided in both doc files
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-20 Alex Duan Migrated from uncatalog/army/whole/test_check_error_code.py
        """
        tdLog.debug(f"start to excute {__file__}")

        # read head error code
        hFile = f"{tdFindPath.getTDenginePath()}/include/util/taoserror.h"
        tdLog.info(f"hFile: {hFile}")
        codes = self.readHeadCodes(hFile)

        # read zh codes
        zhDoc = f"{tdFindPath.getTDenginePath()}/docs/zh/14-reference/09-error-code.md"
        zhCodes = self.readDocCodes(zhDoc)

        # read en codes
        enDoc = f"{tdFindPath.getTDenginePath()}/docs/en/14-reference/09-error-code.md"
        enCodes = self.readDocCodes(enDoc)

        # check zh
        tdLog.info(f"check zh docs ...")
        self.check_consistency(zhCodes, codes)

        # check en
        tdLog.info(f"check en docs ...")
        self.check_consistency(enCodes, codes)

        tdLog.success(f"{__file__} successfully executed")


