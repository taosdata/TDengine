#
# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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
#


import os
import taos
import time
import util

import argparse
import sys

import deployCluster
import prepareEnv
import writeData
import doTest
import cmdLine

from outLog import log
from outMetrics import metrics
from deployCluster import cluster



def doScene(scene):
    print("Running scene: %s" % scene)

    # prepare env
    prep = prepareEnv.PrepareEnv()
    prep.prepare_env(scene)

    
    # write data
    writer = writeData.WriteData(scene)
    writer.run()
    
    # do test
    tester = doTest.DoTest(scene)
    tester.run()

# 
# --------------------- man ---------------------------
#
if __name__ == "__main__":
    print("TSBS TDengine Tool Ver 1.0")
    cmd = cmdLine.CmdLine()
    cmd.init()
    
    log.init_log("tsbs_tdengine.log")
    metrics.init_metrics("tsbs_tdengine.metrics")
            
    # create cluster
    cluster.create_cluster(1, 1)
    
    # do test
    scenes = cmd.get_scenes()
    for scene in scenes:
        doScene(scene)

    # output metrics
    metrics.output_metrics()
    log.close_log()