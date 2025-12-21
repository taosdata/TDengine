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
from .epath import *
from .eos import *
from .log import *
from .sql import tdSql

TAOS = "taos"
TAOSDUMP = "taosdump"
TAOSBENCHMARK = "taosBenchmark"
TAOSADAPTER = "taosadapter"

# taos
def taosFile():
    """Get the path to the `taos` binary file.

    Returns:
        str: The full path to the `taos` binary file, with `.exe` appended if on Windows.
    """

    bmFile = binFile(TAOS)
    if isWin():
        bmFile += ".exe"
    return bmFile

# taosdump
def taosDumpFile():
    """Get the path to the `taosdump` binary file.

    Returns:
        str: The full path to the `taosdump` binary file, with `.exe` appended if on Windows.
    """
    bmFile = binFile(TAOSDUMP)
    if isWin():
        bmFile += ".exe"
    return bmFile

# taosBenchmark
def benchMarkFile():
    """Get the path to the `taosBenchmark` binary file.
    
    Args:
        None

    Returns:
        str: The full path to the `taosBenchmark` binary file, with `.exe` appended if on Windows.
    """
    bmFile = binFile(TAOSBENCHMARK)
    if isWin():
        bmFile += ".exe"
    return bmFile

# taosAdapter
def taosAdapterFile():
    """Get the path to the `taosAdapter` binary file.
    
    Args:
        None

    Returns:
        str: The full path to the `taosAdapter` binary file, with `.exe` appended if on Windows.
    """
    bmFile = binFile(TAOSADAPTER)
    if isWin():
        bmFile += ".exe"
    return bmFile

# run taosBenchmark with command or json file mode
def benchMark(command = "", json = "") :
    """Run the `taosBenchmark` binary with a command or JSON file.

    Args:
        command (str, optional): The command to execute. Defaults to an empty string.
        json (str, optional): The path to a JSON file for execution. Defaults to an empty string.

    Raises:
        SystemExit: If the execution of the JSON file fails.
    """
    # get taosBenchmark path
    bmFile = benchMarkFile()

    # run
    if command != "":
        if "-a" in command or "--replica" in command:
            exe(bmFile + " " + command + " -y")
        else:
            exe(bmFile + " " + command + f" -y -a {tdSql.replica}")
    if json != "":
        cmd = f"{bmFile} -f {json}"
        print(cmd)
        status = exe(cmd)
        if status !=0:
            tdLog.exit(f"run failed {cmd} status={status}")


# get current directory file name
def curFile(fullPath, filename):
    """Get the full path to a file in the current directory.

    Args:
        fullPath (str): The full path to the current directory.
        filename (str): The name of the file.

    Returns:
        str: The full path to the file in the current directory.
    """
    return os.path.join(os.path.dirname(fullPath), filename)


# run build/bin file
def runBinFile(fname, command, show = True, checkRun = False, retFail = False ):
    """Run a binary file with the specified command.

    Args:
        fname (str): The name of the binary file.
        command (str): The command to execute.
        show (bool, optional): Whether to log the command. Defaults to True.

    Returns:
        list: The output of the command as a list of strings.
    """
    bin_file = binFile(fname)
    if isWin():
        bin_file += ".exe"

    cmd = f"{bin_file} {command}"
    if show:
        tdLog.info(cmd)
    return runRetList(cmd, checkRun=checkRun, retFail=retFail, show=show)

# exe build/bin file
def exeBinFile(fname, command, wait=True, show=True):
    """Execute a binary file with the specified command.

    This method uses `utils.army.frame.eos.exe` or `utils.army.frame.eos.exeNoWait` 
    to execute the binary file. The `exe` function waits for the command to finish, 
    while `exeNoWait` runs the command in the background and returns immediately.

    Args:
        fname (str): The name of the binary file.
        command (str): The command to execute.
        wait (bool, optional): Whether to wait for the command to finish. Defaults to True.
            - If True, uses `utils.army.frame.eos.exe`.
            - If False, uses `utils.army.frame.eos.exeNoWait`.
        show (bool, optional): Whether to log the command. Defaults to True.

    Returns:
        int: The exit status of the command execution. A return value of `0` indicates success, 
             while a non-zero value indicates failure.
             - If `wait` is False, the return value is the exit status of the `nohup` or `mintty` command.
    """
    bin_file = binFile(fname)
    if isWin():
        bin_file += ".exe"

    cmd = f"{bin_file} {command}"
    if wait:
        if show:
            tdLog.info("wait exe:" + cmd)
        return exe(f"{bin_file} {command}")
    else:
        if show:
            tdLog.info("no wait exe:" + cmd)
        return exeNoWait(cmd)

#
#  run bin file
#

# taos
def taos(command, show = True, checkRun = False):
    return runBinFile(TAOS, command, show, checkRun)

def taosdump(command, show = True, checkRun = True, retFail = True):
    return runBinFile(TAOSDUMP, command, show, checkRun, retFail)

def benchmark(command, show = True, checkRun = True, retFail = True):
    return runBinFile(TAOSBENCHMARK, command, show, checkRun, retFail)        

def getFilePath(base_dir, *parts):
    """
    Get the full path to a file, ensuring compatibility with Windows paths.
    
    Args:
        *parts (str): The parts of the file path.
        
    Returns:
        str: The full path to the file.
    """
    file_path = os.path.join(os.path.dirname(base_dir), *parts)
    if platform.system().lower() == 'windows':
        file_path = file_path.replace("\\", "\\\\")
    return file_path