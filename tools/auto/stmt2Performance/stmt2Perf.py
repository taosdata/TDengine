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
dataDir = "/var/lib/taos/"
templateFile = "json/template.json"
Number = 0
resultContext = ""


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
def runRetList(command, timeout=10, first=True):
    output,error = run(command, timeout)
    if first:
        return output.splitlines()
    else:
        return error.splitlines()


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
    global resultContext
    resultContext += context
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


def generateJsonFile(stmt, interlace):
    print(f"doTest stmt: {stmt} interlace_rows={interlace}\n")
    
    # replace datatype
    context = readFileContext(templateFile)
    # replace compress
    context = context.replace("@STMT_MODE", stmt)
    context = context.replace("@INTERLACE_MODE", interlace)

    # write to file
    fileName = f"json/test_{stmt}_{interlace}.json"
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
    
    # stop
    taosdStop()
    # clean
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
    return context[start:end]


def writeTemplateInfo(resultFile):
    # create info
    context = readFileContext(templateFile)
    vgroups    = findContextValue(context, "vgroups")
    childCount = findContextValue(context, "childtable_count")
    insertRows = findContextValue(context, "insert_rows")
    line = f"vgroups = {vgroups}\nchildtable_count = {childCount}\ninsert_rows = {insertRows}\n\n"
    print(line)
    appendFileContext(resultFile, line)


def totalCompressRate(stmt, interlace, resultFile, writeSpeed, querySpeed):
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
    arr = str1.split(" ")

    # Total_Size KB
    str2 = arr[2]
    pos  = str2.find("=[")
    totalSize = int(float(str2[pos+2:])/1024)

    # Compression_Ratio
    str2 = arr[6]
    pos  = str2.find("=[")
    rate = str2[pos+2:]
    print("rate =" + rate)

    # total data file size
    #dataSize = getFolderSize(f"{dataDir}/vnode/")
    #dataSizeMB = int(dataSize/1024/1024)

    # appand to file
    
    Number += 1
    context =  "%10s %10s %15s %10s %10s %30s %15s\n"%( Number, stmt, interlace, str(totalSize)+" MB", rate+"%", writeSpeed + " Records/second", querySpeed)
    showLog(context)
    appendFileContext(resultFile, context)

def testWrite(jsonFile):
    command = f"taosBenchmark -f {jsonFile}"
    output, context = run(command, 60000)
    # SUCC: Spent 0.960248 (real 0.947154) seconds to insert rows: 100000 with 1 thread(s) into dbrate 104139.76 (real 105579.45) records/second

    # find second real
    pos = context.find("(real ")
    if pos == -1:
        print(f"error, run command={command} output not found first \"(real\" keyword. error={context}")
        exit(1)
    pos = context.find("(real ", pos + 5)
    if pos == -1:
        print(f"error, run command={command} output not found second \"(real\" keyword. error={context}")
        exit(1)    

    pos += 5
    length = len(context)
    while pos < length and context[pos] == ' ':
        pos += 1
    end = context.find(".", pos)
    if end == -1:
        print(f"error, run command={command} output not found second \".\" keyword. error={context}")
        exit(1)

    speed = context[pos: end]
    #print(f"write pos ={pos} end={end} speed={speed}\n output={context} \n")
    return speed

def testQuery():
    command = f"taosBenchmark -f json/query.json"
    lines = runRetList(command, 60000)
    # INFO: Spend 6.7350 second completed total queries: 10, the QPS of all threads:      1.485
    speed = None

    for i in range(20, len(lines)):        
        # find second real
        pos = context.find("the QPS of all threads:")
        context = lines[26]
        if pos == -1 :
            continue
        pos += 24
        speed = context[pos:]
        break
    #print(f"query pos ={pos} speed={speed}\n output={context} \n")

    if speed is None:
        print(f"error, run command={command} output not found second \"the QPS of all threads:\" keyword. error={lines}")
        exit(1)
    else:
        return speed

def doTest(stmt, interlace, resultFile):
    print(f"doTest stmtMode: {stmt} interlaceRows={interlace}\n")
    #cleanAndStartTaosd()


    # json
    jsonFile = generateJsonFile(stmt, interlace)

    # run taosBenchmark
    t1 = time.time()
    writeSpeed = testWrite(jsonFile)
    t2 = time.time()
    # total write speed
    querySpeed = testQuery()

    # total compress rate
    totalCompressRate(stmt, interlace, resultFile, writeSpeed, querySpeed)


def main():

    # test compress method
    stmtModes = ["stmt", "stmt2", "taosc"]
    interlaceModes = ["0", "1"]

    # record result
    resultFile = "./result.txt"
    timestamp = time.time()
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
    context  = f"\n----------------------  test rate ({now}) ---------------------------------\n"

    appendFileContext(resultFile, context)
    # json info
    writeTemplateInfo(resultFile)
    # head
    context = "\n%10s %10s %15s %10s %10s %30s %15s\n"%("No", "stmtMode", "interlaceRows", "dataSize", "rate", "writeSpeed", "query-QPS")
    appendFileContext(resultFile, context)


    # loop for all compression
    for stmt in stmtModes:
        # do test
        for interlace in interlaceModes:
            doTest(stmt, interlace, resultFile)
    appendFileContext(resultFile, "    \n")

    timestamp = time.time()
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
    appendFileContext(resultFile, f"\n{now} finished test!\n")


if __name__ == "__main__":
    print("welcome use TDengine compress rate test tools.\n")
    main()
    # show result 
    print(resultContext)