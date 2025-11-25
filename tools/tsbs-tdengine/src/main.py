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
import signal

import argparse
import sys

import deployCluster
import prepareEnv
import writeData
import doTest


from outMetrics import metrics
from deployCluster import cluster
from cmdLine import cmd
from outLog import log


def doScene(scene):    
    log.out("\n\n************ Running scene: %s ************\n" % scene.name)
    
    # create cluster
    cluster.create_cluster(dnode=1, snode=1, use_previous=cmd.use_previous)
    
    # scene init
    metrics.scenarioId[scene.name]     = scene.name
    metrics.classification[scene.name] = scene.classification
    metrics.data_rows[scene.name]      = 0
    metrics.output_rows[scene.name]    = 0
    metrics.set_status(scene.name, "Init")

    # prepare env
    env = prepareEnv.PrepareEnv(scene)
    if env.run() == False:
        return False

    if cmd.user_canceled:
        log.out("User canceled the operation.")
        return False
    
    # write data
    writer = writeData.WriteData(scene)
    if writer.run() == False:
        return False

    if cmd.user_canceled:
        log.out("User canceled the operation.")
        return False
    
    # do test
    tester = doTest.DoTest(scene)
    tester.run()
    
    return True

# Handle SIGINT signal
def signal_handler(sig, frame):
    cmd.user_canceled = True
    log.out('\nYou pressed Ctrl+C exiting ...')

# 
# --------------------- man ---------------------------
#
if __name__ == "__main__":
    print("TSBS TDengine Tool Ver 1.0")
    signal.signal(signal.SIGINT, signal_handler)
    
    # Initialize command line arguments
    cmd.init()
    
    # Show current configuration
    cmd.show_config()    
    
    log.init_log("tsbs_tdengine.log")
    metrics.init_metrics("tsbs_tdengine.metrics")
                
    # do test
    metrics.start()
    scenes = cmd.get_scenes()
    for scene in scenes:
        doScene(scene)
        if cmd.user_canceled:
            log.out("User canceled the operation.")
            break
    
    metrics.end()

    # output metrics
    metrics.output_metrics()
    log.close()