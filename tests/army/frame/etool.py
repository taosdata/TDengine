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
import epath
import eos

# run taosBenchmark with command or json file mode
def runBenchmark(command = "", json = "") :
    # get taosBenchmark path
    bmFile = epath.binFile("taosBenchmark")
    if eos.isWin():
        bmFile += ".exe"

    # run
    if command != "":
        eos.exe(bmFile + " " + command)
    if json != "":
        eos.exe(bmFile + " -f " + json)

# get current directory file name
def curFile(fullPath, file):
    return os.path.dirname(fullPath) + "/" + file
