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
import os
import sys
import getopt
import subprocess
import time
import base64
import json
import platform
import socket
import threading
import importlib

import toml

from frame.log import *
from frame.server.dnodes import *
from frame.server.cluster import *
from frame.cases import *
from frame.taosadapter import *

import taos
import taosrest
import taosws

def checkRunTimeError():
    import win32gui
    timeCount = 0
    while 1:
        time.sleep(1)
        timeCount = timeCount + 1
        print("checkRunTimeError",timeCount)
        if (timeCount>600):
            print("stop the test.")
            os.system("TASKKILL /F /IM taosd.exe")
            os.system("TASKKILL /F /IM taos.exe")
            os.system("TASKKILL /F /IM tmq_sim.exe")
            os.system("TASKKILL /F /IM mintty.exe")
            os.system("TASKKILL /F /IM python.exe")
            quit(0)
        hwnd = win32gui.FindWindow(None, "Microsoft Visual C++ Runtime Library")
        if hwnd:
            os.system("TASKKILL /F /IM taosd.exe")

# 
# run case on previous cluster
#
def runOnPreviousCluster(host, config, fileName):
    print("enter run on previeous")
    
    # load case module
    sep = "/"
    if platform.system().lower() == 'windows':
        sep = os.sep
    moduleName = fileName.replace(".py", "").replace(sep, ".")
    uModule = importlib.import_module(moduleName)
    case = uModule.TDTestCase()

    # create conn
    conn = taos.connect(host, config)

    # run case
    case.init(conn, False)
    try:
        case.run()
    except Exception as e:
        tdLog.notice(repr(e))
        tdLog.exit("%s failed" % (fileName))
    # stop
    case.stop()


if __name__ == "__main__":

    #
    #  analysis paramaters
    #
    fileName = "all"
    deployPath = ""
    masterIp = ""
    testCluster = False
    valgrind = 0
    killValgrind = 1
    logSql = True
    stop = 0
    restart = False
    dnodeNums = 1
    mnodeNums = 0
    updateCfgDict = {}
    adapter_cfg_dict = {}
    execCmd = ""
    queryPolicy = 1
    createDnodeNums = 1
    restful = False
    websocket = False
    replicaVar = 1
    asan = False
    independentMnode = False
    previousCluster = False
    level = 1
    disk  = 1

    opts, args = getopt.gnu_getopt(sys.argv[1:], 'f:p:m:l:scghrd:k:e:N:M:Q:C:RWU:n:i:aP:L:D:', [
        'file=', 'path=', 'master', 'logSql', 'stop', 'cluster', 'valgrind', 'help', 'restart', 'updateCfgDict', 'killv', 'execCmd','dnodeNums','mnodeNums',
        'queryPolicy','createDnodeNums','restful','websocket','adaptercfgupdate','replicaVar','independentMnode',"asan",'previous','level','disk'])
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
            tdLog.printNoPrefix('-d update cfg dict, base64 json str')
            tdLog.printNoPrefix('-k not kill valgrind processer')
            tdLog.printNoPrefix('-e eval str to run')
            tdLog.printNoPrefix('-N start dnodes numbers in clusters')
            tdLog.printNoPrefix('-M create mnode numbers in clusters')
            tdLog.printNoPrefix('-Q set queryPolicy in one dnode')
            tdLog.printNoPrefix('-C create Dnode Numbers in one cluster')
            tdLog.printNoPrefix('-R restful realization form')
            tdLog.printNoPrefix('-W websocket connection')
            tdLog.printNoPrefix('-U taosadapter update cfg dict ')
            tdLog.printNoPrefix('-n the number of replicas')
            tdLog.printNoPrefix('-i independentMnode Mnode')
            tdLog.printNoPrefix('-a address sanitizer mode')
            tdLog.printNoPrefix('-P run case with [P]revious cluster, do not create new cluster to run case.')
            tdLog.printNoPrefix('-L set multiple level number.     range 1 ~ 3')
            tdLog.printNoPrefix('-D set disk number on each level. range 1 ~ 10')

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

        if key in ['-d', '--updateCfgDict']:
            try:
                updateCfgDict = eval(base64.b64decode(value.encode()).decode())
            except:
                print('updateCfgDict convert fail.')
                sys.exit(0)

        if key in ['-k', '--killValgrind']:
            killValgrind = 1

        if key in ['-e', '--execCmd']:
            try:
                execCmd = base64.b64decode(value.encode()).decode()
            except:
                print('execCmd run fail.')
                sys.exit(0)

        if key in ['-N', '--dnodeNums']:
            dnodeNums = value

        if key in ['-M', '--mnodeNums']:
            mnodeNums = value

        if key in ['-Q', '--queryPolicy']:
            queryPolicy = value

        if key in ['-C', '--createDnodeNums']:
            createDnodeNums = value

        if key in ['-i', '--independentMnode']:
            independentMnode = value

        if key in ['-R', '--restful']:
            restful = True
        
        if key in ['-W', '--websocket']:
            websocket = True

        if key in ['-a', '--asan']:
            asan = True

        if key in ['-U', '--adaptercfgupdate']:
            try:
                adaptercfgupdate = eval(base64.b64decode(value.encode()).decode())
            except:
                print('adapter cfg update convert fail.')
                sys.exit(0)

        if key in ['-n', '--replicaVar']:
            replicaVar = value

        if key in ['-P', '--previous']:
            previousCluster = True

        if key in ['-L', '--level']:
            level = value

        if key in ['-D', '--disk']:
            disk = value

    #
    # do exeCmd command
    #
    if not execCmd == "":
        if restful or websocket:
            tAdapter.init(deployPath)
        else:
            tdDnodes.init(deployPath)
        print(execCmd)
        exec(execCmd)
        quit()

    #
    # do stop option
    #
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
            usePortPID = "lsof -i tcp:%d | grep LISTEN | awk '{print $2}'" % port
            processID = subprocess.check_output(usePortPID, shell=True)

            if processID:
                killCmd = "kill -TERM %s" % processID
                os.system(killCmd)
            fuserCmd = "fuser -k -n tcp %d" % port
            os.system(fuserCmd)
        if valgrind:
            time.sleep(2)

        if restful or websocket:
            toBeKilled = "taosadapter"

            # killCmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}' | xargs kill -TERM > /dev/null 2>&1" % toBeKilled
            killCmd = f"pkill {toBeKilled}"

            psCmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}'" % toBeKilled
            # psCmd = f"pgrep {toBeKilled}"
            processID = subprocess.check_output(psCmd, shell=True)

            while(processID):
                os.system(killCmd)
                time.sleep(1)
                processID = subprocess.check_output(psCmd, shell=True)

            port = 6041
            usePortPID = f"lsof -i tcp:{port} | grep LISTEN | awk '{{print $2}}'"
            processID = subprocess.check_output(usePortPID, shell=True)

            if processID:
                killCmd = f"kill -TERM {processID}"
                os.system(killCmd)
            fuserCmd = f"fuser -k -n tcp {port}"
            os.system(fuserCmd)

            tdLog.info('stop taosadapter')

        tdLog.info('stop All dnodes')

    #
    # get hostname
    #
    if masterIp == "":
        host = socket.gethostname()
    else:
        try:
            config = eval(masterIp)
            host = config["host"]
        except Exception as r:
            host = masterIp
    tdLog.info("Procedures for tdengine deployed in %s" % (host))

    #
    # do previousCluster option
    #
    if previousCluster:
        tdDnodes.init(deployPath, masterIp)
        runOnPreviousCluster(host, tdDnodes.getSimCfgPath(), fileName)
        tdLog.info("run on previous cluster end.")
        quit()

    #
    # windows run
    #
    if platform.system().lower() == 'windows':
        fileName = fileName.replace("/", os.sep)
        if (masterIp == "" and not fileName == "0-others\\udf_create.py"):
            threading.Thread(target=checkRunTimeError,daemon=True).start()
        tdLog.info("Procedures for testing self-deployment")
        tdDnodes.init(deployPath, masterIp)
        tdDnodes.setTestCluster(testCluster)
        tdDnodes.setValgrind(valgrind)
        tdDnodes.stopAll()
        key_word = 'tdCases.addWindows'
        is_test_framework = 0
        try:
            if key_word in open(fileName, encoding='UTF-8').read():
                is_test_framework = 1
        except Exception as r:
            print(r)
        updateCfgDictStr = ''
        # adapter_cfg_dict_str = ''
        if is_test_framework:
            moduleName = fileName.replace(".py", "").replace(os.sep, ".")
            uModule = importlib.import_module(moduleName)
            try:
                ucase = uModule.TDTestCase()
                if ((json.dumps(updateCfgDict) == '{}') and hasattr(ucase, 'updatecfgDict')):
                    updateCfgDict = ucase.updatecfgDict
                    updateCfgDictStr = "-d %s"%base64.b64encode(json.dumps(updateCfgDict).encode()).decode()
                if ((json.dumps(adapter_cfg_dict) == '{}') and hasattr(ucase, 'taosadapter_cfg_dict')):
                    adapter_cfg_dict = ucase.taosadapter_cfg_dict
                    # adapter_cfg_dict_str = f"-D {base64.b64encode(toml.dumps(adapter_cfg_dict).encode()).decode()}"
            except Exception as r:
                print(r)
        else:
            pass
        # if restful:
        tAdapter.init(deployPath, masterIp)
        tAdapter.stop(force_kill=True)

        if dnodeNums == 1 :
            tdDnodes.setLevelDisk(level, disk)
            tdDnodes.deploy(1,updateCfgDict)
            tdDnodes.start(1)
            tdCases.logSql(logSql)
            if restful or websocket:
                tAdapter.deploy(adapter_cfg_dict)
                tAdapter.start()

            if queryPolicy != 1:
                queryPolicy=int(queryPolicy)
                if restful:
                    conn = taosrest.connect(url=f"http://{host}:6041",timezone="utc")
                elif websocket:
                    conn = taosws.connect(f"taosws://root:taosdata@{host}:6041")
                else:
                    conn = taos.connect(host,config=tdDnodes.getSimCfgPath())

                cursor = conn.cursor()
                cursor.execute("create qnode on dnode 1")
                cursor.execute(f'alter local "queryPolicy" "{queryPolicy}"')
                cursor.execute("show local variables")
                res = cursor.fetchall()
                for i in range(cursor.rowcount):
                    if res[i][0] == "queryPolicy" :
                        if int(res[i][1]) == int(queryPolicy):
                            tdLog.info(f'alter queryPolicy to {queryPolicy} successfully')
                        else:
                            tdLog.debug(res)
                            tdLog.exit(f"alter queryPolicy to  {queryPolicy} failed")
        else :
            tdLog.debug("create an cluster  with %s nodes and make %s dnode as independent mnode"%(dnodeNums,mnodeNums))
            dnodeslist = cluster.configure_cluster(dnodeNums=dnodeNums, mnodeNums=mnodeNums, independentMnode=independentMnode, level=level, disk=disk)
            clusterDnodes.init(dnodeslist, deployPath, masterIp)
            clusterDnodes.setTestCluster(testCluster)
            clusterDnodes.setValgrind(valgrind)
            clusterDnodes.setAsan(asan)
            clusterDnodes.stopAll()
            for dnode in clusterDnodes.dnodes:
                clusterDnodes.deploy(dnode.index, updateCfgDict)
            for dnode in clusterDnodes.dnodes:
                clusterDnodes.starttaosd(dnode.index)
            tdCases.logSql(logSql)
                            
            if restful or websocket:
                tAdapter.deploy(adapter_cfg_dict)
                tAdapter.start()

            if restful:
                conn = taosrest.connect(url=f"http://{host}:6041",timezone="utc")
            elif websocket:
                conn = taosws.connect(f"taosws://root:taosdata@{host}:6041")
            else:
                conn = taos.connect(host,config=tdDnodes.getSimCfgPath())
            # tdLog.info(tdDnodes.getSimCfgPath(),host)
            if createDnodeNums == 1:
                createDnodeNums=dnodeNums
            else:
                createDnodeNums=createDnodeNums
            cluster.create_dnode(conn,createDnodeNums)
            cluster.create_mnode(conn,mnodeNums)
            try:
                if cluster.check_dnode(conn) :
                    print("check dnode ready")
            except Exception as r:
                print(r)
            if queryPolicy != 1:
                queryPolicy=int(queryPolicy)
                if restful:
                    conn = taosrest.connect(url=f"http://{host}:6041",timezone="utc")
                elif websocket:
                    conn = taosws.connect(f"taosws://root:taosdata@{host}:6041")
                else:
                    conn = taos.connect(host,config=tdDnodes.getSimCfgPath())

                cursor = conn.cursor()
                cursor.execute("create qnode on dnode 1")
                cursor.execute(f'alter local "queryPolicy" "{queryPolicy}"')
                cursor.execute("show local variables")
                res = cursor.fetchall()
                for i in range(cursor.rowcount):
                    if res[i][0] == "queryPolicy" :
                        if int(res[i][1]) == int(queryPolicy):
                            tdLog.info(f'alter queryPolicy to {queryPolicy} successfully')
                        else:
                            tdLog.debug(res)
                            tdLog.exit(f"alter queryPolicy to  {queryPolicy} failed")
                            
        if ucase is not None and hasattr(ucase, 'noConn') and ucase.noConn == True:
            conn = None
        else:
            if restful:
                    conn = taosrest.connect(url=f"http://{host}:6041",timezone="utc")
            elif websocket:
                conn = taosws.connect(f"taosws://root:taosdata@{host}:6041")
            else:
                conn = taos.connect(host=f"{host}", config=tdDnodes.getSimCfgPath())

        if testCluster:
            tdLog.info("Procedures for testing cluster")
            if fileName == "all":
                tdCases.runAllCluster()
            else:
                tdCases.runOneCluster(fileName)
        else:
            tdLog.info("Procedures for testing self-deployment")
            if restful:
                    conn = taosrest.connect(url=f"http://{host}:6041",timezone="utc")
            elif websocket:
                conn = taosws.connect(f"taosws://root:taosdata@{host}:6041")
            else:
                conn = taos.connect(host=f"{host}", config=tdDnodes.getSimCfgPath())

            if fileName == "all":
                tdCases.runAllWindows(conn)
            else:
                tdCases.runOneWindows(conn, fileName, replicaVar)

        if restart:
            if fileName == "all":
                tdLog.info("not need to query ")
            else:
                sp = fileName.rsplit(".", 1)
                if len(sp) == 2 and sp[1] == "py":
                    tdDnodes.stopAll()
                    tdDnodes.start(1)
                    time.sleep(1)
                    if restful:
                        conn = taosrest.connect(url=f"http://{host}:6041",timezone="utc")
                    elif websocket:
                        conn = taosws.connect(f"taosws://root:taosdata@{host}:6041")
                    else:
                        conn = taos.connect(host=f"{host}", config=tdDnodes.getSimCfgPath())
                    tdLog.info("Procedures for tdengine deployed in %s" % (host))
                    tdLog.info("query test after taosd restart")
                    tdCases.runOneWindows(conn, sp[0] + "_" + "restart.py", replicaVar)
                else:
                    tdLog.info("not need to query")
    else:
        # except windows
        tdDnodes.setKillValgrind(killValgrind)
        tdDnodes.init(deployPath, masterIp)
        tdDnodes.setTestCluster(testCluster)
        tdDnodes.setValgrind(valgrind)
        tdDnodes.setAsan(asan)
        tdDnodes.stopAll()
        is_test_framework = 0
        key_word = 'tdCases.addLinux'
        try:
            if key_word in open(fileName).read():
                is_test_framework = 1
        except:
            pass
        if is_test_framework:
            moduleName = fileName.replace(".py", "").replace("/", ".")
            uModule = importlib.import_module(moduleName)
            try:
                ucase = uModule.TDTestCase()
                if (json.dumps(updateCfgDict) == '{}'):
                    updateCfgDict = ucase.updatecfgDict
                if (json.dumps(adapter_cfg_dict) == '{}'):
                    adapter_cfg_dict = ucase.taosadapter_cfg_dict
            except:
                pass

        if restful or websocket:
            tAdapter.init(deployPath, masterIp)
            tAdapter.stop(force_kill=True)

        if dnodeNums == 1 :
            # dnode is one
            tdDnodes.setLevelDisk(level, disk)
            tdDnodes.deploy(1,updateCfgDict)
            tdDnodes.start(1)
            tdCases.logSql(logSql)

            if restful or websocket:
                tAdapter.deploy(adapter_cfg_dict)
                tAdapter.start()

            if queryPolicy != 1:
                queryPolicy=int(queryPolicy)
                if restful:
                    conn = taosrest.connect(url=f"http://{host}:6041",timezone="utc")
                elif websocket:
                    conn = taosws.connect(f"taosws://root:taosdata@{host}:6041")
                else:
                    conn = taos.connect(host=f"{host}", config=tdDnodes.getSimCfgPath())

                cursor = conn.cursor()
                cursor.execute("create qnode on dnode 1")
                cursor.execute(f'alter local "queryPolicy" "{queryPolicy}"')
                cursor.execute("show local variables")
                res = cursor.fetchall()
                for i in range(cursor.rowcount):
                    if res[i][0] == "queryPolicy" :
                        if int(res[i][1]) == int(queryPolicy):
                            tdLog.info(f'alter queryPolicy to {queryPolicy} successfully')
                        else:
                            tdLog.debug(res)
                            tdLog.exit(f"alter queryPolicy to  {queryPolicy} failed")

        else :
            # dnode > 1 cluster
            tdLog.debug("create an cluster  with %s nodes and make %s dnode as independent mnode"%(dnodeNums,mnodeNums))
            print(independentMnode,"independentMnode valuse")
            # create dnode list
            dnodeslist = cluster.configure_cluster(dnodeNums=dnodeNums, mnodeNums=mnodeNums, independentMnode=independentMnode, level=level, disk=disk)
            clusterDnodes.init(dnodeslist, deployPath, masterIp)
            clusterDnodes.setTestCluster(testCluster)
            clusterDnodes.setValgrind(valgrind)
            clusterDnodes.setAsan(asan)
            clusterDnodes.stopAll()
            for dnode in clusterDnodes.dnodes:
                clusterDnodes.deploy(dnode.index,updateCfgDict)
            for dnode in clusterDnodes.dnodes:
                clusterDnodes.starttaosd(dnode.index)
            tdCases.logSql(logSql)

            if restful or websocket:
                tAdapter.deploy(adapter_cfg_dict)
                tAdapter.start()

            # create taos connect
            if restful:
                    conn = taosrest.connect(url=f"http://{host}:6041",timezone="utc")
            elif websocket:
                conn = taosws.connect(f"taosws://root:taosdata@{host}:6041")
            else:
                conn = taos.connect(host=f"{host}", config=tdDnodes.getSimCfgPath())
            print(tdDnodes.getSimCfgPath(),host)
            if createDnodeNums == 1:
                createDnodeNums=dnodeNums
            else:
                createDnodeNums=createDnodeNums
            cluster.create_dnode(conn,createDnodeNums)
            cluster.create_mnode(conn,mnodeNums)

            try:
                if cluster.check_dnode(conn) :
                    print("check dnode ready")
            except Exception as r:
                print(r)

            # do queryPolicy option
            if queryPolicy != 1:
                queryPolicy=int(queryPolicy)
                if restful:
                    conn = taosrest.connect(url=f"http://{host}:6041",timezone="utc")
                elif websocket:
                    conn = taosws.connect(f"taosws://root:taosdata@{host}:6041")
                else:
                    conn = taos.connect(host=f"{host}", config=tdDnodes.getSimCfgPath())

                cursor = conn.cursor()
                cursor.execute("create qnode on dnode 1")
                cursor.execute(f'alter local "queryPolicy" "{queryPolicy}"')
                cursor.execute("show local variables")
                res = cursor.fetchall()
                for i in range(cursor.rowcount):
                    if res[i][0] == "queryPolicy" :
                        if int(res[i][1]) == int(queryPolicy):
                            tdLog.info(f'alter queryPolicy to {queryPolicy} successfully')
                        else:
                            tdLog.debug(res)
                            tdLog.exit(f"alter queryPolicy to  {queryPolicy} failed")
                            

        # run case
        if testCluster:
            tdLog.info("Procedures for testing cluster")
            if fileName == "all":
                tdCases.runAllCluster()
            else:
                tdCases.runOneCluster(fileName)
        else:
            tdLog.info("Procedures for testing self-deployment")
            if restful:
                    conn = taosrest.connect(url=f"http://{host}:6041",timezone="utc")
            elif websocket:
                conn = taosws.connect(f"taosws://root:taosdata@{host}:6041")
            else:
                conn = taos.connect(host=f"{host}", config=tdDnodes.getSimCfgPath())

            if fileName == "all":
                tdCases.runAllLinux(conn)
            else:
                tdCases.runOneLinux(conn, fileName, replicaVar)

        # do restart option
        if restart:
            if fileName == "all":
                tdLog.info("not need to query ")
            else:
                sp = fileName.rsplit(".", 1)
                if len(sp) == 2 and sp[1] == "py":
                    tdDnodes.stopAll()
                    tdDnodes.start(1)
                    time.sleep(1)
                    if restful:
                        conn = taosrest.connect(url=f"http://{host}:6041",timezone="utc")
                    elif websocket:
                        conn = taosws.connect(f"taosws://root:taosdata@{host}:6041")
                    else:
                        conn = taos.connect(host=f"{host}", config=tdDnodes.getSimCfgPath())
                    tdLog.info("Procedures for tdengine deployed in %s" % (host))
                    tdLog.info("query test after taosd restart")
                    tdCases.runOneLinux(conn, sp[0] + "_" + "restart.py", replicaVar)
                else:
                    tdLog.info("not need to query")

    # close for end
    if conn is not None:
        conn.close()
    if asan:
        tdDnodes.StopAllSigint()
        tdLog.info("Address sanitizer mode finished")
    sys.exit(0)
