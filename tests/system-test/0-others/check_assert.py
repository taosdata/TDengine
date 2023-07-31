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

'''
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *

'''

import sys
import random
import os

#define code
NO_FOUND      = 0  # not found assert or ASSERT 
FOUND_OK      = 1  # found ASSERT and valid usage
FOUND_NOIF    = 2  # found ASSERT but no if like  ASSERT(...)
FOUND_LOWER   = 3  # found assert write with system assert
FOUND_HAVENOT = 4  # found ASSERT have if but have not like if(!ASSERT)

code_strs = ["not found", "valid", "found but no if", "system assert","found but have not"]


#
# check assert
#
class CheckAssert:
    def __init__(self):
        self.files = 0
        self.total = [0,0,0,0,0]

    def wholeMatched(self, line, pos, n):
        before = False
        after  = False
        if pos != -1:
            if pos == 0 or line[pos-1] == ' ' or line[pos-1] == '!' or line[pos-1] == '(':
                before = True
            if n > pos + 6 + 1:
                last = line[pos+6]
                if last == ' ' or last == '(':
                    after = True

        if before and after:
            return True
        else:
            return False            

    
    def parseLine(self, line):
        
        # see FOUND_xxx define below
        code = 0

        n = len(line)
        if(n < 7):
            return NO_FOUND
        s1 = line.find("//")  # commit line
 
        # assert
        pos = line.find("assert")
        if pos != -1:
            if self.wholeMatched(line, pos, n):
                # comment line check
                if not (s1 != -1 and s1 < pos):
                    return FOUND_LOWER
        
        # ASSERT
        pos = 0
        while pos < n:        
            pos = line.find("ASSERT", pos)
            #print(f"   pos={pos} {line}")
            if pos == -1:
                return NO_FOUND
            if self.wholeMatched(line, pos, n):
                break
            # next
            pos += 6

        #
        # found 
        #

        # comment line
        if s1 != -1 and s1 < pos:
            return NO_FOUND
        # check if
        s2 = line.find("if")
        if s2 == -1 or s2 > pos or  pos - s2 > 5:
            return FOUND_NOIF
        s3 = line.find("!")
        # if(!ASSERT
        if s3 > s2 and s3 < pos:
            return FOUND_HAVENOT
        
        return FOUND_OK


    def checkFile(self, pathfile):
        # check .h .c
        ext = pathfile[-2:].lower()
        if ext != ".h" and ext != ".c":
            return 
  
        print(" check file %s"%pathfile)
        self.files += 1
        err = 0
        ok  = 0
        i = 0

        # read file context 
        with open(pathfile, "r") as fp:
            lines = fp.readlines()
            for line in lines:
                i += 1
                code = self.parseLine(line)
                self.total[code] += 1
                if code == FOUND_OK:
                    ok += 1
                if code != NO_FOUND and code != FOUND_OK:
                    err += 1
                if code != NO_FOUND:
                    print(f"    line: {i} code: {code} {line}")
        
        # parse end output total
        if err > 0 or ok > 0:
            print(f" found problem: {err} \n")
            

    def scanPath(self, path):
        #print(" check path %s"%path)

        ignores = ["/test/"]
        for ignore in ignores:
            if ignore in path:
                print(f"  ignore {path}  keyword:  {ignore}")
                return

        with os.scandir(path) as childs:
            for child in childs:
                if child.is_file():
                    self.checkFile(os.path.join(path, child.name))
                elif child.is_dir():
                    self.scanPath(child.path)   


    def doCheck(self, path):
        print(f" start check path:{path}")
        self.scanPath(path)

        # print total
        print("\n")
        print(f" ---------------  total ({self.files} files)--------------")
        for i in range(5):
            print(f"    code : {i}  num: {self.total[i]}    ({code_strs[i]})")
        print(" ---------------  end ----------------")    
        print(f"\n")

#
#  main function
#

if __name__ == "__main__":
    print(" hello, welcome to use check assert tools 1.0.")
    path = os.path.dirname(os.path.realpath(__file__))
    label = "TDengine"
    pos = path.find(label)
    if pos != -1:
        pos += len(label) + 1
        src = path[0:pos] + "source"
    else:
        src = path

    checker = CheckAssert()
    checker.doCheck(src)
    print(" check assert finished")
    


'''
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.checker = CheckAssert()

    # run
    def run(self):
        # calc
        selfPath = os.path.dirname(os.path.realpath(__file__))
        projPath = ""
        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        src = self.projPath + "src/"
        self.checker.checkAssert(src)    

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

'''