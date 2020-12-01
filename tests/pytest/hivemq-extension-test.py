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
import sys
import os
import os.path
import time
import glob
import getopt
import subprocess
from shutil import which
from multipledispatch import dispatch

@dispatch(str, str)
def v_print(msg: str, arg: str):
    if verbose:
        print(msg % arg)


@dispatch(str, int)
def v_print(msg: str, arg: int):
    if verbose:
        print(msg % int(arg))


@dispatch(str, int, int)
def v_print(msg: str, arg1: int, arg2: int):
    if verbose:
        print(msg % (int(arg1), int(arg2)))


@dispatch(str, int, int, int)
def v_print(msg: str, arg1: int, arg2: int, arg3: int):
    if verbose:
        print(msg % (int(arg1), int(arg2), int(arg3)))


@dispatch(str, int, int, int, int)
def v_print(msg: str, arg1: int, arg2: int, arg3: int, arg4: int):
    if verbose:
        print(msg % (int(arg1), int(arg2), int(arg3), int(arg4)))

def isHiveMQInstalled():
    v_print("%s", "Check if HiveMQ installed")
    defaultHiveMQPath = "/opt/hivemq*"
    hiveMQDir = glob.glob(defaultHiveMQPath)
    if (len(hiveMQDir) == 0):
        return False
    else:
        v_print("HiveMQ installed at %s", hiveMQDir[0])
    return True

def isMosquittoInstalled():
    v_print("%s", "Check if mosquitto installed")
    if not which('mosquitto_pub'):
        v_print("%s", "mosquitto is not installed")
        return False
    else:
        return True

def installExtension():
    currentDir = os.getcwd()
    os.chdir('../../src/connector/hivemq-tdengine-extension')
    v_print("%s", "build extension..")
    os.system('mvn clean package')

    tdExtensionZip = 'target/hivemq-tdengine-extension*.zip'
    tdExtensionZipDir = glob.glob(tdExtensionZip)

    defaultHiveMQPath = "/opt/hivemq*"
    hiveMQDir = glob.glob(defaultHiveMQPath)
    extPath = hiveMQDir[0] + '/extensions'

    tdExtDir = glob.glob(extPath + '/hivemq-tdengine-extension')
    if len(tdExtDir):
        v_print("%s", "delete exist extension..")
        os.system('rm -rf %s' % tdExtDir[0])

    v_print("%s", "unzip extension..")
    os.system('unzip %s -d %s' % (tdExtensionZipDir[0], extPath))

    os.chdir(currentDir)

def stopHiveMQ():
    toBeKilled = "hivemq.jar"
    psCmd = "ps ax|grep -w %s| grep -v grep | awk '{print $1}'" % toBeKilled

    processID = subprocess.check_output(
        psCmd, shell=True).decode("utf-8")

    while(processID):
        killCmd = "kill -TERM %s > /dev/null 2>&1" % processID
        os.system(killCmd)
        time.sleep(1)
        processID = subprocess.check_output(
            psCmd, shell=True).decode("utf-8")
        print("hiveMQ processID: %s" % processID)
    v_print("%s", "hivemq is NOT running")

def runHiveMQ():
    defaultHiveMQPath = "/opt/hivemq*"
    hiveMQDir = glob.glob(defaultHiveMQPath)
    runPath = hiveMQDir[0] + '/bin/run.sh > /dev/null &'
    os.system(runPath)
    time.sleep(10)
    v_print("%s", "hivemq is running")

def runTDengine():
    pass

def reCreateDatabase():
    os.system('taos -s "DROP DATABASE IF EXISTS hivemq"')
    os.system('taos -s "CREATE DATABASE IF NOT EXISTS hivemq"')

def sendMqttMsg(topic: str, payload: str):
    testStr = 'mosquitto_pub -t %s -m "%s"' % (topic, payload)
    os.system(testStr)
    time.sleep(3)

def checkTDengineData(topic: str, payload: str):
    output = subprocess.check_output('taos -s "select * from hivemq.mqtt_payload"', shell=True).decode('utf-8')
    if (topic in output) and (payload in output):
        v_print("%s", output)
        return True
    else:
        v_print("%s", "ERROR: mqtt topic or payload NOT found")
        return False


if __name__ == "__main__":
    verbose = True
    testTopic = 'test'
    testPayload = 'hello world'

    if not isHiveMQInstalled():
        sys.exit(1)

    if not isMosquittoInstalled():
        sys.exit(1)

    stopHiveMQ()

    installExtension()

    runTDengine()

    reCreateDatabase()

    runHiveMQ()

    sendMqttMsg(testTopic, testPayload)

    if not checkTDengineData(testTopic, testPayload):
        sys.exit(1)

    sys.exit(0)
