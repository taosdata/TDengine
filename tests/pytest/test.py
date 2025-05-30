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
import time
from distutils.log import warn as printf
import platform
import ast

from util.log import *
from util.dnodes import *
from util.cases import *

import taos

def get_local_classes_in_order(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        tree = ast.parse(file.read(), filename=file_path)
    
    classes = [node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]
    return classes

def dynamicLoadModule(fileName):
    moduleName = fileName.replace(".py", "").replace(os.sep, ".")
    return importlib.import_module(moduleName, package='..')


if __name__ == "__main__":
    fileName = "all"
    deployPath = ""
    masterIp = ""
    testCluster = False
    valgrind = 0
    logSql = True
    stop = 0
    restart = False
    windows = 0
    if platform.system().lower() == 'windows':
        windows = 1
    opts, args = getopt.gnu_getopt(sys.argv[1:], 'f:p:m:l:scghr', [
        'file=', 'path=', 'master', 'logSql', 'stop', 'cluster', 'valgrind', 'help', 'restart'])
    for key, value in opts:
        if key in ['-h', '--help']:
            tdLog.printNoPrefix(
                'A collection of test cases written using Python')
            tdLog.printNoPrefix('-f Name of test case file written by Python')
            tdLog.printNoPrefix('-p Deploy Path for Simulator')
            tdLog.printNoPrefix('-m Master Ip for Simulator')
            tdLog.printNoPrefix('-l <True:False> logSql Flag')
            tdLog.printNoPrefix('-s stop All dnodes')
            tdLog.printNoPrefix('-c Test Cluster Flag')
            tdLog.printNoPrefix('-g valgrind Test Flag')
            tdLog.printNoPrefix('-r taosd restart test')
            sys.exit(0)

        if key in ['-r', '--restart']: 
            restart = True

        if key in ['-f', '--file']:
            fileName = value

        if key in ['-p', '--path']:
            deployPath = value

        if key in ['-m', '--master']:
            masterIp = value 

        if key in ['-l', '--logSql']:
            if (value.upper() == "TRUE"):
                logSql = True
            elif (value.upper() == "FALSE"):
                logSql = False
            else:
                tdLog.printNoPrefix("logSql value %s is invalid" % logSql)
                sys.exit(0)

        if key in ['-c', '--cluster']:
            testCluster = True

        if key in ['-g', '--valgrind']:
            valgrind = 1

        if key in ['-s', '--stop']:
            stop = 1

    if (stop != 0):
        if (valgrind == 0):
            toBeKilled = "taosd"
        else:
            toBeKilled = "valgrind.bin"

        killCmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}' | xargs kill -TERM > /dev/null 2>&1" % toBeKilled

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

    if masterIp == "":
        host = '127.0.0.1'
    else:
        host = masterIp

    if (windows):
        tdCases.logSql(logSql)
        tdLog.info("Procedures for testing self-deployment")
        if masterIp == "" or masterIp == "localhost":
            tdDnodes.init(deployPath)
            tdDnodes.setTestCluster(testCluster)
            tdDnodes.setValgrind(valgrind)
            tdDnodes.stopAll()
            is_test_framework = 0
            key_word = 'tdCases.addWindows'
            try:
                if key_word in open(fileName, encoding='UTF-8').read():
                    is_test_framework = 1
            except:
                pass
            if is_test_framework:
                uModule = dynamicLoadModule(fileName)
                try:
                    class_names = get_local_classes_in_order(fileName)
                    case_class = getattr(uModule, class_names[-1])
                    ucase = case_class()
                    tdDnodes.deploy(1,ucase.updatecfgDict)
                except :
                    tdDnodes.deploy(1,{})
            else:
                pass
                tdDnodes.deploy(1,{})
            tdDnodes.start(1)
        else:
            remote_conn = Connection("root@%s"%host)
            with remote_conn.cd('/var/lib/jenkins/workspace/TDinternal/community/tests/pytest'):
                remote_conn.run("python3 ./test.py")
        tdDnodes.init(deployPath)
        conn = taos.connect(
            host="%s" % (host),
            config=tdDnodes.sim.getCfgDir())
        tdCases.runOneWindows(conn, fileName)
        tdCases.logSql(logSql)
    else:
        tdDnodes.init(deployPath)
        tdDnodes.setTestCluster(testCluster)
        tdDnodes.setValgrind(valgrind)
        tdDnodes.stopAll()
        is_test_framework = 0
        key_word = 'tdCases.addLinux'
        try:
            if key_word in open(fileName).read():
                is_test_framework = 1
        except:
            pass
        if is_test_framework:
            uModule = dynamicLoadModule(fileName)
            try:
                class_names = get_local_classes_in_order(fileName)
                case_class = getattr(uModule, class_names[-1])
                ucase = case_class()
                tdDnodes.deploy(1,ucase.updatecfgDict)
            except :
                tdDnodes.deploy(1,{})
        else:
            tdDnodes.deploy(1,{})
        tdDnodes.start(1)

        tdLog.info("Procedures for tdengine deployed in %s" % (host))

        tdCases.logSql(logSql)

        if testCluster:
            tdLog.info("Procedures for testing cluster")
            if fileName == "all":
                tdCases.runAllCluster()
            else:
                tdCases.runOneCluster(fileName)
        else:
            tdLog.info("Procedures for testing self-deployment")
            conn = taos.connect(
                host,
                config=tdDnodes.getSimCfgPath())
            if fileName == "all":
                tdCases.runAllLinux(conn)
            else:
                tdCases.runOneLinux(conn, fileName)
        if restart:
            if fileName == "all":
                tdLog.info("not need to query ")
            else:    
                sp = fileName.rsplit(".", 1)
                if len(sp) == 2 and sp[1] == "py":
                    tdDnodes.stopAll()
                    tdDnodes.start(1)
                    time.sleep(1)            
                    conn = taos.connect( host, config=tdDnodes.getSimCfgPath())
                    tdLog.info("Procedures for tdengine deployed in %s" % (host))
                    tdLog.info("query test after taosd restart")
                    tdCases.runOneLinux(conn, sp[0] + "_" + "restart.py")
                else:
                    tdLog.info("not need to query")
    conn.close()
