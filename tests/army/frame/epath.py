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

#
# about path function extension
#

import os
import platform
from frame.log import *

# build/bin path
binDir = ""

def binPath():
    global binDir

    if binDir != "":
        return binDir

    selfPath = os.path.dirname(os.path.realpath(__file__))
    if platform.system().lower() == "windows":
        split = "\\"
        taosd = "taosd.exe"
    else:
        split = "/"
        taosd = "taosd"

    pos = selfPath.find(f"community{split}tests")
    if (pos != -1):
        projPath = selfPath[:pos]
    else:
        projPath = selfPath[:selfPath.find(f"TDengine{split}tests")]

    buildPath = ""

    for root, dirs, files in os.walk(projPath):
        if (taosd in files):
            rootRealPath = os.path.dirname(os.path.realpath(root))
            if ("packaging" not in rootRealPath):
                buildPath = root[:len(root) - len("/build/bin")]
                break
    # check
    if (buildPath == ""):
        tdLog.exit("buildPath is empty, taosd not found!")
    else:
        tdLog.info(f"taosd found in {buildPath}")
    # return
    binDir = buildPath + "/build/bin/"
    return binDir

def binFile(filename):
    return binPath() + filename



