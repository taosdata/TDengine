# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-

#
# about path function extension
#

import os
from .log import *
from .server.dnodes import *

# build/bin path
binDir = ""

def binPath():
    return os.path.dirname(tdDnodes.binPath)
    #global binDir

    #if binDir != "":
    #    return binDir

    #selfPath = os.path.dirname(os.path.realpath(__file__))

    #if ("community/test" in selfPath):
    #    projPath = selfPath[:selfPath.find("community/test")]
    #else:
    #    projPath = selfPath[:selfPath.find("TDengine/test")]

    #for root, dirs, files in os.walk(projPath):
    #    if ("taosd" in files):
    #        rootRealPath = os.path.dirname(os.path.realpath(root))
    #        if ("packaging" not in rootRealPath):
    #            buildPath = root[:len(root)-len("/build/bin")]
    #            break
    # check        
    #if (buildPath == ""):
    #    tdLog.exit("taosd not found!")
    #else:
    #    tdLog.info(f"taosd found in {buildPath}")
    # return
    #binDir = buildPath + "/build/bin/"
    #return binDir

def binFile(filename):
    return os.path.join(binPath(), filename)



