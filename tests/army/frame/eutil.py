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
# about tools funciton extension
#

import sys
import os
import time
import datetime
import psutil
import frame.etool
from frame.log import *


# cpu frequent as random
def cpuRand(max):
    decimal = int(str(psutil.cpu_freq().current).split(".")[1])
    return decimal % max

def getProcessInfoByKeywork(keyword: str):
    matching_processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if keyword in ' '.join(proc.info['cmdline']):
                matching_processes.append((proc.info['pid'], proc.info['name']))
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return matching_processes

def runTaosShellWithSpecialCfg(sql: str, cfg: str):
   if not os.path.exists(cfg):
       tdLog.exit(f'config file does not exist, file: {cfg}')

   if sql[-1] != ";":
       sql = sql + ';'
   rets = frame.etool.runBinFile("taos", f"-s '{sql}' -c {cfg}")
   if not frame.etool.checkErrorFromBinFile(rets):
       return rets
   else:
       tdLog.exit(f"failed to run sql '{sql}' in taos shell with speccial config: {cfg}, error info: {rets}")

