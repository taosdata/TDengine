# Copyright (c) 2023 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
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
def exe(command, show = False):
    code = os.system(command)
    if show:
        print(f"eos retcode={code} command:{command}")
    return code    

# execute file and return immediately
def exeNoWait(file):
    if isWin():
        cmd = f"mintty -h never {file}"
    else:
        cmd = f"nohup {file} > /dev/null 2>&1 & "
    return exe(cmd)

def readFileContext(filename):
    file = open(filename)
    context = file.read()
    file.close()
    return context

# run return output and error
def run(command, timeout = 20, ret_code=False, show = True):
    id = time.time_ns() % 100000
    out = f"out_{id}.txt"
    err = f"err_{id}.txt"
    
    code = exe(command + f" 1>{out} 2>{err}", show)

    # read from file
    output = readFileContext(out)
    error  = readFileContext(err)

    # del
    if os.path.exists(out):
        os.remove(out)
    if os.path.exists(err):
        os.remove(err)

    if ret_code:
        return output, error, code
    else:
        return output, error


# return list after run
def runRetList(command, timeout=10, checkRun=False, retFail=False, show = True):
    output, error, code = run(command, ret_code=True, show=show)
    if checkRun and code != 0:
        if code != 0:
            raise Exception(f"run command failed. code={code} error={error} output={output}")
    
    rList = output.splitlines()
    if retFail and error != "":
        rList += error.splitlines()
    return rList

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