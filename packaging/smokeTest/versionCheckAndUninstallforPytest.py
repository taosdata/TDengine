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
import glob
import sys, os
import re
import platform
import getopt
import subprocess
# from this import d
import time
from lib import run_cmd


def _filter_leftover_output(output, ignored_paths=None):
    ignored_paths = ignored_paths or set()
    lines = []
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped or "No such file or directory" in stripped:
            continue
        if stripped in ignored_paths:
            continue
        lines.append(stripped)
    return lines


def _collect_leftover_paths(path_pattern, ignored_paths=None):
    ignored_paths = ignored_paths or set()
    matches = glob.glob(path_pattern)
    if not matches and not glob.has_magic(path_pattern) and os.path.exists(path_pattern):
        matches = [path_pattern]

    leftovers = []
    for match in sorted(matches):
        if match in ignored_paths:
            continue
        leftovers.append(match)
    return leftovers


# input for server
def UninstallTaos(version, verMode, uninstall, name):
    if not version:
        raise ValueError("No version specified, will not run version check.")

    system = platform.system()
    leftFile = False
    uninstall_failed = False
    leftover_files = []
    ignored_paths = set()
    if system == "Darwin" and name == "taos":
        # macOS packages do not include taosk, so a pre-existing taosk binary
        # should not fail taos uninstall verification.
        ignored_paths.add("/usr/local/bin/taosk")
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
            if process.returncode != 0:
                uninstall_failed = True
                print("rm%s exited with code %s" % (name, process.returncode))
            paths_to_check = [
                ("/etc/systemd/system/%s*" % name, False),
                ("/usr/bin/%s*" % name, False),
                ("/usr/local/bin/%s*" % name, False),
                ("/usr/lib/lib%s*" % name, False),
                ("/usr/lib64/lib%s*" % name, False),
                ("/usr/include/%s*" % name, False),
                ("/usr/local/%s" % name, True),
            ]
            for path_pattern, is_install_dir in paths_to_check:
                filtered = _collect_leftover_paths(path_pattern, ignored_paths)
                if filtered:
                    if is_install_dir:
                        print("Uninstall left some files in /usr/local/%s：%s" % (name, "\n".join(filtered)))
                    else:
                        print("Uninstall left some files: %s" % "\n".join(filtered))
                    leftFile = True
                    leftover_files.extend(filtered)
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
            if process.returncode != 0:
                uninstall_failed = True
                print("rm%s exited with code %s" % (name, process.returncode))
            paths_to_check = [
                "/usr/local/bin/%s*" % name,
                "/usr/local/lib/lib%s*" % name,
                "/usr/local/include/%s*" % name,
            ]
            for path_pattern in paths_to_check:
                filtered = _collect_leftover_paths(path_pattern, ignored_paths)
                if filtered:
                    print("Uninstall left some files: %s" % "\n".join(filtered))
                    leftFile = True
                    leftover_files.extend(filtered)
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
            if process.returncode != 0:
                uninstall_failed = True
                print("unins000 exited with code %s" % process.returncode)
            time.sleep(10)
            for file in [r"C:\TDengine\taos.exe", r"C:\TDengine\unins000.exe", r"C:\ProDB\prodb.exe",
                         r"C:\ProDB\unins000.exe"]:
                if os.path.exists(file):
                    leftFile = True
                    leftover_files.append(file)
    if uninstall_failed or leftFile:
        failure_reasons = []
        if uninstall_failed:
            failure_reasons.append("uninstall command failed")
        if leftover_files:
            failure_reasons.append("leftover files: %s" % ", ".join(leftover_files))
        raise AssertionError("uninstall %s failed, %s" % (name, "; ".join(failure_reasons)))
    else:
        print("**********Test Result: uninstall test passed! **********")
