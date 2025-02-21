###################################################################
#           Copyright (c) 2023 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

#
# about system funciton extension
#

import sys
import os
import time
import datetime
import platform
import subprocess

#
#  platform
#

# if windows platform return True
def isWin():
    return platform.system().lower() == 'windows'

def isArm64Cpu():
    system = platform.system()

    if system == 'Linux':
        machine = platform.machine().lower()

        # Check for ARM64 architecture on Linux systems
        return machine in ['aarch64', 'armv8l']
    elif system == 'Darwin' or system == 'Windows':
        processor = platform.processor().lower()

        # Check for ARM64 architecture on macOS and Windows systems
        return processor in ['arm64', 'aarch64']
    else:
        print("Unsupported operating system")
        return False

#
#  execute programe
#

# wait util execute file finished 
def exe(file):
    return os.system(file)

# execute file and return immediately
def exeNoWait(file):
    if isWin():
        cmd = f"mintty -h never {file}"
    else:
        cmd = f"nohup {file} > /dev/null 2>&1 & "
    return exe(cmd)

# run return output and error
def run(command, show=True):
    # out to file
    id = time.clock_gettime_ns(time.CLOCK_REALTIME) % 100000
    out = f"out_{id}.txt"
    err = f"err_{id}.txt"
    
    ret = exe(command + f" 1>{out} 2>{err}")

    # read from file
    output = readFileContext(out)
    error  = readFileContext(err)

    # del
    if os.path.exists(out):
        os.remove(out)
    if os.path.exists(err):
        os.remove(err)    

    return output, error


# return list after run
def runRetList(command, timeout=10):
    output,error = run(command, timeout)
    return output.splitlines()

#
#   file 
#

def delFile(file):
    return exe(f"rm -rf {file}")

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