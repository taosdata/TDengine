###################################################################
#           Copyright (c) 2023 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

#
# about system funciton extension
#

import sys
import os
import time
import datetime
import frame.epath
import frame.eos
from frame.log import *


# taos
def taosFile():
    bmFile = frame.epath.binFile("taos")
    if frame.eos.isWin():
        bmFile += ".exe"
    return bmFile

# taosdump
def taosDumpFile():
    bmFile = frame.epath.binFile("taosdump")
    if frame.eos.isWin():
        bmFile += ".exe"
    return bmFile

# taosBenchmark
def benchMarkFile():
    bmFile = frame.epath.binFile("taosBenchmark")
    if frame.eos.isWin():
        bmFile += ".exe"
    return bmFile

# taosAdapter
def taosAdapterFile():
    bmFile = frame.epath.binFile("taosAdapter")
    if frame.eos.isWin():
        bmFile += ".exe"
    return bmFile

# run taosBenchmark with command or json file mode
def benchMark(command = "", json = "") :
    # get taosBenchmark path
    bmFile = benchMarkFile()

    # run
    if command != "":
        status = frame.eos.run(bmFile + " " + command)
    if json != "":
        cmd = f"{bmFile} -f {json}"
        print(cmd)
        status = frame.eos.exe(cmd)
        if status !=0:
          tdLog.exit(f"run failed {cmd} status={status}")
       

# get current directory file name
def curFile(fullPath, filename):
    return os.path.dirname(fullPath) + "/" + filename


# run build/bin file
def runBinFile(fname, command, show = True, checkRun = False, retFail = False ):
    binFile = frame.epath.binFile(fname)
    if frame.eos.isWin():
        binFile += ".exe"

    cmd = f"{binFile} {command}"
    if show:
        tdLog.info(cmd)
    return frame.eos.runRetList(cmd, show, checkRun, retFail)

# exe build/bin file
def exeBinFile(fname, command, wait=True, show=True):
    binFile = frame.epath.binFile(fname)
    if frame.eos.isWin():
        binFile += ".exe"

    cmd = f"{binFile} {command}"
    if wait:
        if show:
            tdLog.info("wait exe:" + cmd)
        return frame.eos.exe(f"{binFile} {command}")
    else:
        if show:
            tdLog.info("no wait exe:" + cmd)
        return frame.eos.exeNoWait(cmd)