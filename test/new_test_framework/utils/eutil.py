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
from .srvCtl import *
from .eos    import *

# cpu frequent as random
def cpuRand(max):
    decimal = int(str(psutil.cpu_freq().current).split(".")[1])
    return decimal % max

# remove single and doulbe quotation
def removeQuota(origin):
    value = ""
    for c in origin:
        if c != '\'' and c != '"':
            value += c

    return value

def findTaosdLog(key, dnodeIdx=1, retry=60):
    logPath = sc.dnodeLogPath(dnodeIdx)
    logFile = logPath + "/taosdlog.0"
    return findLog(logFile, key, retry)


# find string in log
def findLog(logFile, key, retry = 60):
    for i in range(retry):
        cmd = f"grep '{key}' {logFile} | wc -l"
        output, error = run(cmd)
        try:
            cnt = int(output)
            if cnt > 0:
                return cnt
        except ValueError:
            print(f"cmd ={cmd} out={output} err={error}")
        print(f"not found string: {key} in {logFile} , retry again {i} ...")    
        time.sleep(1)
    return 0
