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

opts, args = getopt.gnu_getopt(sys.argv[1:], 'h:P:v:u', [
        'host=', 'Port=', 'version='])
serverHost = ""
serverPort = 0
version = ""
uninstall = False
for key, value in opts:
    if key in ['--help']:
        print('A collection of test cases written using Python')
        print('-h serverHost')
        print('-P serverPort')
        print('-v test client version')
        print('-u test uninstall process, will uninstall TDengine')
        sys.exit(0)

    if key in ['-h']:
        serverHost = value
    if key in ['-P']:
        serverPort = int(value)
    if key in ['-v']:
        version = value
    if key in ['-u']:
        uninstall = True
if not serverHost:
    print("Please input use -h to specify your server host.")
    sys.exit(0)
if not version:
    print("No version specified, will not run version check.")
if serverPort == 0:
    serverPort = 6030
    print("No server port specified, use default 6030.")


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

# prepare data by taosBenchmark
cmd = "taosBenchmark -y -a 3 -n 100 -t 100 -d %s -h %s -P %d &" % (databaseName, serverHost, serverPort)
process_out = subprocess.getoutput(cmd)
print(cmd)
#os.system("taosBenchmark -y -a 3 -n 100 -t 100 -d %s -h %s -P %d" % (databaseName, serverHost, serverPort))
taosBenchmark_test_result = True
time.sleep(10)
import taos

conn = taos.connect(host=serverHost,
                                         user="root",
                                         password="taosdata",
                                         database=databaseName,
                                         port=serverPort,
                                         timezone="Asia/Shanghai")  # default your host's timezone

server_version = conn.server_info
print("server_version", server_version)
client_version = conn.client_info
print("client_version", client_version)  # 3.0.0.0

# Execute a sql and get its result set. It's useful for SELECT statement
result: taos.TaosResult = conn.query("SELECT count(*) from meters")

data = result.fetch_all()
print(data)
if data[0][0] !=10000:
    print(" taosBenchmark work not as expected ")
    print("!!!!!!!!!!!Test Result: taosBenchmark test failed! !!!!!!!!!!")
    sys.exit(1)
#else:
#    print("**********Test Result: taosBenchmark test passed **********")


# drop database of test
taos_test_result = False
print("drop database test")
print("run taos -s 'drop database %s;'  -h %s  -P %d" % (databaseName, serverHost, serverPort))
taos_cmd_outpur = subprocess.getoutput('taos -s "drop database %s;"  -h %s  -P %d' % (databaseName, serverHost, serverPort))
print(taos_cmd_outpur)
if ("Drop OK" in taos_cmd_outpur):
    taos_test_result = True
    #print("*******Test Result: taos test passed ************")

version_test_result = False
if version:
    print("Client info is: %s"%conn.client_info)
    taos_V_output = ""
    if system == "Windows":
        taos_V_output = subprocess.getoutput("taos -V | findstr version")
    else:
        taos_V_output = subprocess.getoutput("taos -V | grep version")

    print("taos -V output is: %s" % taos_V_output)
    if version in taos_V_output and version in conn.client_info:
        version_test_result = True
        #print("*******Test Result: Version check passed ************")

conn.close()
if uninstall:
    print("Start to run rmtaos")
    leftFile = False
    print("Platform: ", system)

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

if taosBenchmark_test_result:
    print("**********Test Result: taosBenchmark test passed! **********")
if taos_test_result:
    print("**********Test Result: taos test passed! **********")
else:
    print("!!!!!!!!!!!Test Result: taos test failed! !!!!!!!!!!")
if version_test_result:
    print("**********Test Result: version test passed! **********")
else:
    print("!!!!!!!!!!!Test Result: version test failed! !!!!!!!!!!")
if not leftFile:
    print("**********Test Result: uninstall test passed! **********")
else:
    print("!!!!!!!!!!!Test Result: uninstall test failed! !!!!!!!!!!")
if taosBenchmark_test_result and taos_test_result and version_test_result and not leftFile:
    sys.exit(0)
else:
    sys.exit(1)

