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
# pip install src/connector/python/

# -*- coding: utf-8 -*-
import sys
import getopt
import subprocess
from distutils.log import warn as printf

from util.log import *
from util.dnodes import *
from util.cases import *
from util.sql import *

import taos


if __name__ == "__main__":
    fileName = "all"
    deployPath = ""
    testCluster = False
    valgrind = 0
    logSql = True
    stop = 0
    opts, args = getopt.gnu_getopt(sys.argv[1:], 'l:sgh', [
        'logSql', 'stop', 'valgrind', 'help'])
    for key, value in opts:
        if key in ['-h', '--help']:
            tdLog.printNoPrefix(
                'A collection of test cases written using Python')
            tdLog.printNoPrefix('-l <True:False> logSql Flag')
            tdLog.printNoPrefix('-s stop All dnodes')
            tdLog.printNoPrefix('-g valgrind Test Flag')
            sys.exit(0)

        if key in ['-l', '--logSql']:
            if (value.upper() == "TRUE"):
                logSql = True
            elif (value.upper() == "FALSE"):
                logSql = False
            else:
                tdLog.printNoPrefix("logSql value %s is invalid" % logSql)
                sys.exit(0)

        if key in ['-g', '--valgrind']:
            valgrind = 1

        if key in ['-s', '--stop']:
            stop = 1

    if (stop != 0):
        if (valgrind == 0):
            toBeKilled = "taosd"
        else:
            toBeKilled = "valgrind.bin"

        killCmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}' | xargs kill -HUP > /dev/null 2>&1" % toBeKilled

        psCmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}'" % toBeKilled
        processID = subprocess.check_output(psCmd, shell=True)

        while(processID):
            os.system(killCmd)
            time.sleep(1)
            processID = subprocess.check_output(psCmd, shell=True)

        for port in range(6030, 6041):
            usePortPID = "lsof -i tcp:%d | grep LISTEn | awk '{print $2}'" % port
            processID = subprocess.check_output(usePortPID, shell=True)

            if processID:
                killCmd = "kill -TERM %s" % processID
                os.system(killCmd)
            fuserCmd = "fuser -k -n tcp %d" % port
            os.system(fuserCmd)
        if valgrind:
            time.sleep(2)

        tdLog.info('stop All dnodes')
        sys.exit(0)

    tdDnodes.init(deployPath)
    tdDnodes.setTestCluster(testCluster)
    tdDnodes.setValgrind(valgrind)

    tdDnodes.stopAll()
    tdDnodes.addSimExtraCfg("minTablesPerVnode", "100")
    tdDnodes.deploy(1)
    tdDnodes.start(1)

    host = '127.0.0.1'

    tdLog.info("Procedures for tdengine deployed in %s" % (host))

    tdCases.logSql(logSql)

    conn = taos.connect(
        host,
        config=tdDnodes.getSimCfgPath())

    tdSql.init(conn.cursor(), True)

    tdSql.execute("DROP DATABASE IF EXISTS db")
    tdSql.execute("CREATE DATABASE IF NOT EXISTS db")
    tdSql.execute("USE db")

    for i in range(0, 100):
        tdSql.execute(
            "CREATE TABLE IF NOT EXISTS tb%d (ts TIMESTAMP, temperature INT, humidity FLOAT)" % i)

    for i in range(1, 6):
        tdSql.execute("INSERT INTO tb99 values (now + %da, %d, %f)" % (i, i, i * 1.0))

    tdSql.execute("DROP TABLE tb99")
    tdSql.execute(
            "CREATE TABLE IF NOT EXISTS tb99 (ts TIMESTAMP, temperature INT, humidity FLOAT)")
    tdSql.query("SELECT * FROM tb99")
    tdSql.checkRows(0)
    
    conn.close()
