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
import utils.army.frame.epath
import utils.army.frame.eos
from utils.army.frame.log import *

# taosdump
def taosDumpFile():
    bmFile = utils.army.frame.epath.binFile("taosdump")
    if utils.army.frame.eos.isWin():
        bmFile += ".exe"
    return bmFile

# taosBenchmark
def benchMarkFile():
    bmFile = utils.army.frame.epath.binFile("taosBenchmark")
    if utils.army.frame.eos.isWin():
        bmFile += ".exe"
    return bmFile

# taosAdapter
def taosAdapterFile():
    bmFile = utils.army.frame.epath.binFile("taosAdapter")
    if utils.army.frame.eos.isWin():
        bmFile += ".exe"
    return bmFile

# run taosBenchmark with command or json file mode
def benchMark(command = "", json = "") :
    # get taosBenchmark path
    bmFile = benchMarkFile()

    # run
    if command != "":
        utils.army.frame.eos.exe(bmFile + " " + command)
    if json != "":
        cmd = f"{bmFile} -f {json}"
        print(cmd)
        status = utils.army.frame.eos.exe(cmd)
        if status !=0:
          tdLog.exit(f"run failed {cmd} status={status}")
       

# get current directory file name
def curFile(fullPath, filename):
    return os.path.dirname(fullPath) + "/" + filename


# run build/bin file
def runBinFile(fname, command, show=True):
    binFile = utils.army.frame.epath.binFile(fname)
    if utils.army.frame.eos.isWin():
        binFile += ".exe"

    cmd = f"{binFile} {command}"
    if show:
        tdLog.info(cmd)
    return utils.army.frame.eos.runRetList(cmd)

# exe build/bin file
def exeBinFile(fname, command, wait=True, show=True):
    binFile = utils.army.frame.epath.binFile(fname)
    if utils.army.frame.eos.isWin():
        binFile += ".exe"

    cmd = f"{binFile} {command}"
    if wait:
        if show:
            tdLog.info("wait exe:" + cmd)
        return utils.army.frame.eos.exe(f"{binFile} {command}")
    else:
        if show:
            tdLog.info("no wait exe:" + cmd)
        return utils.army.frame.eos.exeNoWait(cmd)