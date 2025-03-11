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

import logging
import os
import platform
import subprocess

logger = logging.getLogger(__name__)

# run taosBenchmark with command or json file mode
def run_taosbenchMark(command = "", json = "") :
    # get taosBenchmark exe file
    bmFile = "taosBenchmark.exe" if platform.system() == "Windows" else "taosBenchmark"
    # run
    logger.info(f"run taosBenchmark with command: {command}, json: {json}")
    try:
        if command != "":
            logger.info(f"run taosBenchmark with command: {command}")
            result = subprocess.run(f"{bmFile} {command} -y", check=True, shell=True)
        if json != "":
            result = subprocess.run(f"{bmFile} -f {json}", check=True, shell=True)
        if result.returncode != 0:
            logger.error(f"run taosBenchmark failed  status={result.returncode}")
            raise Exception(f"run taosBenchmark failed command={command}, json={json} status={result.returncode}")
    except Exception as e:
        logger.error(f"run taosBenchmark failed command={command},json={json} error={e}")
        raise e

       

# get current directory file name
def curFile(fullPath, filename):
    return os.path.dirname(fullPath) + "/" + filename


# run build/bin file
def runBinFile(fname, command, show=True):
    binFile = frame.epath.binFile(fname)
    if frame.eos.isWin():
        binFile += ".exe"

    cmd = f"{binFile} {command}"
    if show:
        tdLog.info(cmd)
    return frame.eos.runRetList(cmd)

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