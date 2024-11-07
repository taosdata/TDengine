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
import sys, os
import re
import platform
import getopt
import subprocess
# from this import d
import time

# input for server

opts, args = getopt.gnu_getopt(sys.argv[1:], 'v:m:u', ['version=', 'verMode='])
serverHost = ""
serverPort = 0
version = ""
uninstall = False
verMode = ""
for key, value in opts:
    if key in ['--help']:
        print('A collection of test cases written using Python')
        print('-v test client version')
        print('-u test uninstall process, will uninstall TDengine')
        sys.exit(0)

    if key in ['-v']:
        version = value
    if key in ['-u']:
        uninstall = True
    if key in ['-m']:
        verMode = value
if not version:
    print("No version specified, will not run version check.")


system = platform.system()
arch = platform.machine()

databaseName = re.sub(r'[^a-zA-Z0-9]', '', subprocess.getoutput("hostname")).lower()
# install taospy
taospy_version = ""
if system == 'Windows':
    taospy_version = subprocess.getoutput("pip3 show taospy|findstr Version")
else:
    taospy_version = subprocess.getoutput("pip3 show taospy|grep Version| awk -F ':' '{print $2}' ")

print("taospy version %s " % taospy_version)
if taospy_version == "":
    subprocess.getoutput("pip3 install git+https://github.com/taosdata/taos-connector-python.git")
    print("install taos python connector")
else:
    subprocess.getoutput("pip3 install taospy")

# start taosd server
if system == 'Windows':
    cmd = ["C:\\TDengine\\start-all.bat"]
elif system == 'Linux':
    cmd = "systemctl start taosd".split(' ')
else:
    cmd = "sudo launchctl start com.tdengine.taosd".split(' ')
process_out = subprocess.Popen(cmd,
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
print(cmd)
time.sleep(5)

#get taosc version info
version_test_result = False
if version:
    import taos
    conn = taos.connect()
    server_version = conn.server_info
    print("server_version", server_version)
    client_version = conn.client_info
    print("client_version", client_version)
    # Execute sql get version info
    result: taos.TaosResult = conn.query("SELECT server_version()")
    select_server = result.fetch_all()[0][0]
    print("SELECT server_version():" + select_server)
    result: taos.TaosResult = conn.query("SELECT client_version()")
    select_client = result.fetch_all()[0][0]
    print("SELECT client_version():" + select_client)
    conn.close()

    taos_V_output = ""
    taosd_V_output = ""
    taosadapter_V_output = ""
    taoskeeper_V_output = ""
    taosx_V_output = ""
    taosB_V_output = ""
    taosxVersion = False
    if system == "Windows":
        taos_V_output = subprocess.getoutput("taos -V | findstr version")
        taosd_V_output = subprocess.getoutput("taosd -V | findstr version")
        taosadapter_V_output = subprocess.getoutput("taosadapter -V | findstr version")
        taoskeeper_V_output = subprocess.getoutput("taoskeeper -V | findstr version")
        taosB_V_output = subprocess.getoutput("taosBenchmark -V | findstr version")
        if verMode == "Enterprise":
            taosx_V_output = subprocess.getoutput("taosx -V | findstr version")
    else:
        taos_V_output = subprocess.getoutput("taos -V | grep version | awk -F ' ' '{print $3}'")
        taosd_V_output = subprocess.getoutput("taosd -V | grep version | awk -F ' ' '{print $3}'")
        taosadapter_V_output = subprocess.getoutput("taosadapter -V | grep version | awk -F ' ' '{print $3}'")
        taoskeeper_V_output = subprocess.getoutput("taoskeeper -V | grep version | awk -F ' ' '{print $3}'")
        taosB_V_output = subprocess.getoutput("taosBenchmark -V | grep version | awk -F ' ' '{print $3}'")
        if verMode == "Enterprise":
            taosx_V_output = subprocess.getoutput("taosx -V | grep version | awk -F ' ' '{print $3}'")

    print("taos -V output is: %s" % taos_V_output)
    print("taosd -V output is: %s" % taosd_V_output)
    print("taosadapter -V output is: %s" % taosadapter_V_output)
    print("taoskeeper -V output is: %s" % taoskeeper_V_output)
    print("taosBenchmark -V output is: %s" % taosB_V_output)
    if verMode == "Enterprise":
        print("taosx -V output is: %s" % taosx_V_output)
        taosxVersion = version in taosx_V_output
    else:
        taosxVersion = True
    if (version in client_version
        and version in server_version
        and version in select_server
        and version in select_client
        and version in taos_V_output
        and version in taosd_V_output
        and version in taosadapter_V_output
        and version in taoskeeper_V_output
        and version in taosB_V_output
        and taosxVersion
    ):
        version_test_result = True
leftFile = False
if uninstall:
    print("Start to run rmtaos")
    print("Platform: ", system)
    # stop taosd server
    if system == 'Windows':
        cmd = "C:\\TDengine\\stop_all.bat"
    elif system == 'Linux':
        cmd = "systemctl stop taosd"
    else:
        cmd = "sudo launchctl stop com.tdengine.taosd"
    process_out = subprocess.getoutput(cmd)
    print(cmd)
    time.sleep(10)
    if system == "Linux":
        # 创建一个subprocess.Popen对象，并使用stdin和stdout进行交互
        process = subprocess.Popen(['rmtaos'],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        # 向子进程发送输入
        process.stdin.write("y\n")
        process.stdin.flush()  # 确保输入被发送到子进程
        process.stdin.write("I confirm that I would like to delete all data, log and configuration files\n")
        process.stdin.flush()  # 确保输入被发送到子进程
        # 关闭子进程的stdin，防止它无限期等待更多输入
        process.stdin.close()
        # 等待子进程结束
        process.wait()
        # 检查目录清除情况
        out = subprocess.getoutput("ls /etc/systemd/system/taos*")
        if "No such file or directory" not in out:
            print("Uninstall left some files: %s" % out)
            leftFile = True
        out = subprocess.getoutput("ls /usr/bin/taos*")
        if "No such file or directory" not in out:
            print("Uninstall left some files: %s" % out)
            leftFile = True
        out = subprocess.getoutput("ls /usr/local/bin/taos*")
        if "No such file or directory" not in out:
            print("Uninstall left some files: %s" % out)
            leftFile = True
        out = subprocess.getoutput("ls /usr/lib/libtaos*")
        if "No such file or directory" not in out:
            print("Uninstall left some files: %s" % out)
            leftFile = True
        out = subprocess.getoutput("ls /usr/lib64/libtaos*")
        if "No such file or directory" not in out:
            print("Uninstall left some files: %s" % out)
            leftFile = True
        out = subprocess.getoutput("ls /usr/include/taos*")
        if "No such file or directory" not in out:
            print("Uninstall left some files: %s" % out)
            leftFile = True
        out = subprocess.getoutput("ls /usr/local/taos")
        #print(out)
        if "No such file or directory" not in out:
            print("Uninstall left some files in /usr/local/taos：%s" % out)
            leftFile = True
        if not leftFile:
            print("*******Test Result: uninstall test passed ************")

    elif system == "Darwin":
        # 创建一个subprocess.Popen对象，并使用stdin和stdout进行交互
        process = subprocess.Popen(['sudo', 'rmtaos'],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        # 向子进程发送输入
        process.stdin.write("y\n")
        process.stdin.flush()  # 确保输入被发送到子进程
        process.stdin.write("I confirm that I would like to delete all data, log and configuration files\n")
        process.stdin.flush()  # 确保输入被发送到子进程
        # 关闭子进程的stdin，防止它无限期等待更多输入
        process.stdin.close()
        # 等待子进程结束
        process.wait()
        # 检查目录清除情况
        out = subprocess.getoutput("ls /usr/local/bin/taos*")
        if "No such file or directory" not in out:
            print("Uninstall left some files: %s" % out)
            leftFile = True
        out = subprocess.getoutput("ls /usr/local/lib/libtaos*")
        if "No such file or directory" not in out:
            print("Uninstall left some files: %s" % out)
            leftFile = True
        out = subprocess.getoutput("ls /usr/local/include/taos*")
        if "No such file or directory" not in out:
            print("Uninstall left some files: %s" % out)
            leftFile = True
        #out = subprocess.getoutput("ls /usr/local/Cellar/tdengine/")
        #print(out)
        #if out:
        #    print("Uninstall left some files: /usr/local/Cellar/tdengine/%s" % out)
        #    leftFile = True
        #if not leftFile:
        #    print("*******Test Result: uninstall test passed ************")

    elif system == "Windows":
        process = subprocess.Popen(['unins000','/silent'],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        process.wait()
        time.sleep(10)
        out = subprocess.getoutput("ls C:\TDengine")
        print(out)
        if len(out.split("\n")) > 3:
            leftFile = True
            print("Uninstall left some files: %s" % out)

if version_test_result:
    print("**********Test Result: version test passed! **********")
else:
    print("!!!!!!!!!!!Test Result: version test failed! !!!!!!!!!!")
if not leftFile:
    print("**********Test Result: uninstall test passed! **********")
else:
    print("!!!!!!!!!!!Test Result: uninstall test failed! !!!!!!!!!!")
if version_test_result and not leftFile:
    sys.exit(0)
else:
    sys.exit(1)

