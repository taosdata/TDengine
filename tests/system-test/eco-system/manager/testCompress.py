import taos
import sys
import os
import subprocess
import time
import random
import json
from datetime import datetime


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
dataDir = "/var/lib/taos"
templateFile = "json/template.json"
Number  = 0

def showLog(str):
    print(str)

def exec(command, show=True):
    if(show):
        print(f"exec {command}\n")
    return os.system(command)

# run return output and error
def run(command, timeout = 60, show=True):
    if(show):
        print(f"run {command} timeout={timeout}s\n")    

    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.wait(timeout)

    output = process.stdout.read().decode(encoding="gbk")
    error = process.stderr.read().decode(encoding="gbk")

    return output, error

# return list after run
def runRetList(command, timeout=10):
    output,error = run(command, timeout)
    return output.splitlines()

def readFileContext(filename):
    file = open(filename)
    context = file.read()
    file.close()
    return context
    

def writeFileContext(filename, context):
    file = open(filename, "w")
    file.write(context)
    file.close()

def appendFileContext(filename, context):
    try:
        file = open(filename, "a")
        wsize = file.write(context)
        file.close()
    except:
        print(f"appand file error  context={context} .")

def getFolderSize(folder):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            total_size += os.path.getsize(filepath)
    return total_size

def waitClusterAlive(loop):
    for i in range(loop):
        command = 'taos -s "show cluster alive\G;" '
        out,err = run(command)
        print(out)
        if out.find("status: 1") >= 0:
            showLog(f" i={i} wait cluster alive ok.\n")
            return True
        
        showLog(f" i={i} wait cluster alive ...\n")
        time.sleep(1)
    
    showLog(f" i={i} wait cluster alive failed.\n")
    return False

def waitCompactFinish(loop):
    for i in range(loop):
        command = 'taos -s "show compacts;" '
        out,err = run(command)
        if out.find("Query OK, 0 row(s) in set") >= 0:
            showLog(f" i={i} wait compact finish ok\n")
            return True
        
        showLog(f" i={i} wait compact ...\n")
        time.sleep(1)
    
    showLog(f" i={i} wait compact failed.\n")
    return False


def getTypeName(datatype):
    str1 = datatype.split(",")[0]
    str2 =     str1.split(":")[1]
    str3 = str2.replace('"','').replace(' ','')
    return str3


def getMatch(datatype, algo):
    if algo == "tsz":
        if datatype == "float" or datatype == "double":
            return True
        else:
            return False
    else:
        return True


def generateJsonFile(datatype, typeName, algo):
    print(f"doTest dataType: {typeName} algo: {algo} \n")
    
    # replace compress
    datatype = datatype.replace("@COMPRESS", algo)
    # replace datatype
    context = readFileContext(templateFile)
    context = context.replace("@DATATYPE", datatype)
    # write to file
    fileName = f"json/{typeName}.json"
    if os.path.exists(fileName):
      os.remove(fileName)
    writeFileContext(fileName, context)

    return fileName

def taosdStart():
    cmd = "nohup /usr/bin/taosd 2>&1 & "
    ret = exec(cmd)
    print(f"exec taosd ret = {ret}\n")
    time.sleep(3)
    waitClusterAlive(10)

def taosdStop():
    i = 1
    toBeKilled = "taosd"
    killCmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}' | xargs kill -TERM > /dev/null 2>&1" % toBeKilled
    psCmd   = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}'" % toBeKilled
    processID = subprocess.check_output(psCmd, shell=True)
    while(processID):
        os.system(killCmd)
        time.sleep(1)
        processID = subprocess.check_output(psCmd, shell=True)
        print(f"i={i} kill taosd pid={processID}")
        i += 1

def cleanAndStartTaosd():
    
    taosdStop()
    # clear  
    exec(f"rm -rf {dataDir}")
    # start
    taosdStart()

def findContextValue(context, label):
    start = context.find(label)
    if start == -1 :
        return ""
    start += len(label) + 2
    # skip blank
    while context[start] == ' ':
        start += 1

    # find end ','
    end = start
    ends = [',','}',']', 0]
    while context[end] not in ends:
        end += 1

    print(f"start = {start} end={end}\n")
    return context[start:end]


def writeTemplateInfo(resultFile):
    context = readFileContext(templateFile)
    vgroups    = findContextValue(context, "vgroups")
    childCount = findContextValue(context, "childtable_count")
    insertRows = findContextValue(context, "insert_rows")
    line = f"vgroups = {vgroups}\nchildtable_count = {childCount}\ninsert_rows = {insertRows}\n\n"
    print(line)
    appendFileContext(resultFile, line)

def totalCompressRate(typeName, algo, resultFile):
    global Number
    # flush
    command = 'taos -s "flush database dbrate;"'
    rets = exec(command)
    command = 'taos -s "compact database dbrate;"'
    rets = exec(command)
    waitCompactFinish(60)

    # read compress rate
    command = 'taos -s "show table distributed dbrate.meters\G;"'
    rets = runRetList(command)
    print(rets)
    str1 = rets[5]
    str2 = str1.split(" ")[6]
    pos  = str2.find("=[")
    rate = str2[pos+2:]
    print("rate =" + rate)

    # total data file size
    dataSize = getFolderSize(f"{dataDir}/vnode/")
    dataSizeMB = int(dataSize/1024/1024)

    # appand to file
    
    Number += 1
    context =  "%10s %10s %10s %10s %10s\n"%( Number, algo, typeName, str(dataSizeMB)+"M", rate+"%")
    showLog(context)
    appendFileContext(resultFile, context)


def doTest(dataType, typeName, algo, resultFile):
    print(f"doTest dataType: {typeName} algo: {algo} \n")
    cleanAndStartTaosd()

    # json
    jsonFile = generateJsonFile(dataType, typeName, algo)
    # run taosBenchmark
    exec(f"taosBenchmark -f {jsonFile}")

    # total compress rate
    totalCompressRate(typeName, algo, resultFile)

def main():
    # test types 
    dataTypes = [
        '{"type": "bool",     "count":100, "min": 0,   "max": 1,       "compress":"@COMPRESS"}',
        '{"type": "tinyint",  "count":100, "min": 0,   "max": 100,     "compress":"@COMPRESS"}',
        '{"type": "smallint", "count":100, "min": 0,   "max": 1000,    "compress":"@COMPRESS"}',
        '{"type": "int",      "count":100, "min": 0,   "max": 100000,  "compress":"@COMPRESS"}',
        '{"type": "bigint",   "count":100, "min": 0,   "max": 1000000, "compress":"@COMPRESS"}',
        '{"type": "float",    "count":100, "min": 0,   "max": 1000,    "compress":"@COMPRESS"}',
        '{"type": "double",   "count":100, "min": 0,   "max": 1000,    "compress":"@COMPRESS"}',
        '{"type": "binary",   "count":100, "len": 64,  "compress":"@COMPRESS"}',
        '{"type": "nchar",    "count":100, "len": 64,  "compress":"@COMPRESS"}'
    ]

    # test compress method
    algos = ["lz4", "zlib", "zstd", "xz", "tsz"]

    # record result
    resultFile = "./result.txt"
    timestamp = time.time()

    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
    context  = f"\n----------------------  test rate ({now}) ---------------------------------\n"
    appendFileContext(resultFile, context)
    # json info
    writeTemplateInfo(resultFile)
    # head
    context = "%10s %10s %10s %10s %10s\n"%("No","compress","dataType", "dataSize","rate")
    appendFileContext(resultFile, context)


    # loop for all types
    for dataType in dataTypes:
        typeName = getTypeName(dataType)
        for algo in algos:
            if getMatch(typeName, algo) is False:
                print(f"warning dataType {typeName} not match with algo {algo} \n")
                continue
            # do test
            doTest(dataType, typeName, algo, resultFile)
        appendFileContext(resultFile, "    \n")

if __name__ == "__main__":
    print("welcome use TDengine compress rate test tools.\n")
    main()