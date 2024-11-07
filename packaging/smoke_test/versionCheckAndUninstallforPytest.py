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
from lib import run_cmd


# input for server
def UninstallTaos(version, verMode, uninstall, name):
    if not version:
        raise "No version specified, will not run version check."

    system = platform.system()
    arch = platform.machine()
    leftFile = False
    if uninstall:
        print("Start to run rm%s" % name)
        print("Platform: ", system)
        # stop taosd server
        if system == 'Windows':
            cmd = "C:\\TDengine\\stop_all.bat"
        else:
            cmd = "stop_all.sh"
        process_out = subprocess.getoutput(cmd)
        print(cmd)
        time.sleep(5)
        print("start to rm%s" % name)
        if system == "Linux":
            # 启动命令
            process = subprocess.Popen(['rm%s' % name], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, text=True)

            # 发送交互输入
            stdout, stderr = process.communicate(
                input="y\nI confirm that I would like to delete all data, log and configuration files\n")

            # 打印输出（可选）
            print(stdout)
            print(stderr)
            # 检查目录清除情况
            out = subprocess.getoutput("ls /etc/systemd/system/%s*" % name)
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/bin/%s*" % name)
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/local/bin/%s*" % name)
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/lib/lib%s*" % name)
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/lib64/lib%s*" % name)
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/include/%s*" % name)
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/local/%s" % name)
            # print(out)
            if "No such file or directory" not in out:
                print("Uninstall left some files in /usr/local/%s：%s" % (name, out))
                leftFile = True
            if not leftFile:
                print("*******Test Result: uninstall test passed ************")

        elif system == "Darwin":
            # 创建一个subprocess.Popen对象，并使用stdin和stdout进行交互
            process = subprocess.Popen(['sudo', 'rm%s' % name],
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
            out = subprocess.getoutput("ls /usr/local/bin/%s*" % name)
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/local/lib/lib%s*" % name)
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            out = subprocess.getoutput("ls /usr/local/include/%s*" % name)
            if "No such file or directory" not in out:
                print("Uninstall left some files: %s" % out)
                leftFile = True
            # out = subprocess.getoutput("ls /usr/local/Cellar/tdengine/")
            # print(out)
            # if out:
            #    print("Uninstall left some files: /usr/local/Cellar/tdengine/%s" % out)
            #    leftFile = True
            # if not leftFile:
            #    print("*******Test Result: uninstall test passed ************")

        elif system == "Windows":
            process = subprocess.Popen(['unins000', '/silent'],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            process.wait()
            time.sleep(10)
            for file in ["C:\TDengine\\taos.exe", "C:\TDengine\\unins000.exe", "C:\ProDB\prodb.exe",
                         "C:\ProDB\\unins000.exe"]:
                if os.path.exists(file):
                    leftFile = True
    if leftFile:
        raise "uninstall %s fail, please check" % name
    else:
        print("**********Test Result: uninstall test passed! **********")
