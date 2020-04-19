#!/usr/bin/python
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
# install pip
# pip install src/connector/python/linux/python2/

# -*- coding: utf-8 -*-
import sys
import getopt
from util.log import *
from util.dnodes import *
from util.cases import *

import taos

# add testcase here:
from insert.basic import *

if __name__ == "__main__":
    fileName = "all"
    deployPath = ""
    masterIp = ""
    testCluster = False
    opts, args = getopt.getopt(sys.argv[1:], 'f:p:m:sch', [
                               'file=', 'path=', 'master', 'stop', 'cluster', 'help'])
    for key, value in opts:
        if key in ['-h', '--help']:
            tdLog.printNoPrefix(
                'A collection of test cases written using Python')
            tdLog.printNoPrefix('-f Name of test case file written by Python')
            tdLog.printNoPrefix('-p Deploy Path for Simulator')
            tdLog.printNoPrefix('-m Master Ip for Simulator')
            tdLog.printNoPrefix('-c Test Cluster Flag')
            tdLog.printNoPrefix('-s stop All dnodes')
            sys.exit(0)
        if key in ['-f', '--file']:
            fileName = value
        if key in ['-p', '--path']:
            deployPath = value
        if key in ['-m', '--master']:
            masterIp = value
        if key in ['-c', '--cluster']:
            testCluster = True
        if key in ['-s', '--stop']:
            cmd = "ps -ef|grep -w taosd | grep 'taosd' | grep -v grep | awk '{print $2}' && pkill -9 taosd"
            os.system(cmd)
            tdLog.exit('stop All dnodes')

    if masterIp == "":
        tdDnodes.init(deployPath)
        if testCluster:
            tdLog.notice("Procedures for testing cluster")
            if fileName == "all":
                tdCases.runAllCluster()
            else:
                tdCases.runOneCluster(fileName)
        else:
            tdLog.notice("Procedures for testing self-deployment")
            tdDnodes.stopAll()
            tdDnodes.deploy(1)
            tdDnodes.start(1)
            conn = taos.connect(
                host='192.168.0.1',
                config=tdDnodes.getSimCfgPath())
            if fileName == "all":
                tdCases.runAllLinux(conn)
            else:
                tdCases.runOneLinux(conn, fileName)
            conn.close()
    else:
        tdLog.notice("Procedures for tdengine deployed in %s" % (masterIp))
        cfgPath = "../../build/test/cfg"   # was: tdDnodes.getSimCfgPath()
        conn = taos.connect(host=masterIp, config=cfgPath)
        if fileName == "all":
            tdCases.runAllWindows(conn)
        else:
            tdCases.runOneWindows(conn, fileName)
        conn.close()
