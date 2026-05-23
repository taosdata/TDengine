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


import os
from .log import *


def find_proj_root(start_path=None):
    """Find project root directory, supporting multiple CI layouts.

    Detects:
      - TDinternal CI:  /home/TDinternal/...          → /home/TDinternal
      - tsdb CI:        /mnt/tsdb/source/taos-community/... → /mnt/tsdb
      - TDengine OSS:   /home/TDengine/...            → /home/TDengine

    Args:
        start_path: Starting path for detection. Defaults to os.getcwd().

    Returns:
        str: The project root path (where sim/, debug/ etc. are located).
    """
    path = start_path or os.getcwd()
    parts = path.split(os.sep)
    for marker in ("TDinternal", "taos-community", "TDengine"):
        if marker in parts:
            idx = parts.index(marker)
            if marker == "taos-community":
                # tsdb layout: .../source/taos-community → root is two levels up
                if idx >= 2 and parts[idx - 1] == "source":
                    return os.sep.join(parts[:idx - 1])
                return os.sep.join(parts[:idx])
            return os.sep.join(parts[:idx + 1])
    raise ValueError("Cannot determine project root from path: " + path)


def find_proj_path(start_path=None):
    """Find the source code project path for os.walk to locate binaries.

    Detects:
      - taos-community: /mnt/tsdb/source/taos-community/... → /mnt/tsdb/
                        (root level, so os.walk finds debug/build/bin/)
      - community:      /home/TDinternal/community/...      → /home/TDinternal/
      - TDengine:       /home/TDengine/...                   → /home/TDengine/
      - fallback:       .../test/...                         → up to test parent

    Args:
        start_path: Starting path for detection. Defaults to caller's __file__ dir.

    Returns:
        str: The project path suitable for os.walk to find taosd/taosBenchmark.
    """
    selfPath = start_path or os.path.dirname(os.path.realpath(__file__))
    norm_path = selfPath.replace("\\", "/")
    if "taos-community" in norm_path:
        idx = norm_path.find("source/taos-community")
        if idx != -1:
            return os.path.normpath(norm_path[:idx])
        idx = norm_path.find("taos-community")
        if idx != -1:
            return os.path.normpath(norm_path[:idx])
    elif "community" in norm_path:
        idx = norm_path.find("community")
        if idx != -1:
            return os.path.normpath(norm_path[:idx])
    elif "TDengine" in norm_path:
        idx = norm_path.find("TDengine")
        if idx != -1:
            return os.path.normpath(norm_path[: idx + len("TDengine")])

    idx = norm_path.find("test")
    if idx != -1:
        return os.path.normpath(norm_path[:idx])
    return os.path.normpath(os.path.dirname(selfPath))



class TDFindPath:
    """This class is for finding path within TDengine
    """
    def __init__(self):
        self.file = os.path.realpath(__file__)


    def init(self, file):
        """[summary]

        Args:
            file (str): the file location you want to start the query. Generally using __file__
        """
        self.file = file

    def getTaosdemoPath(self):
        """for finding the path of directory containing taosdemo

        Returns:
            str: the path to directory containing taosdemo
        """
        selfPath = os.path.dirname(os.path.realpath(self.file))
        projPath = find_proj_path(selfPath)

        for root, dirs, files in os.walk(projPath):
            if ".git" in root:
                continue
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info(f"taosd found in {buildPath}")
        return buildPath + "/build/bin/"  

    def getTDenginePath(self):
        """for finding the root path of TDengine

        Returns:
            str: the root path of TDengine
        """
        selfPath = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(self.file)))))
        return selfPath
        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("test")]
        print(projPath)
        for root, dirs, files in os.walk(projPath):
            if ".git" in root:
                continue
            if ("sim" in dirs):
                print(root)
                rootRealPath = os.path.realpath(root)
        if (rootRealPath == ""):
            tdLog.exit("TDengine not found!")
        else:
            tdLog.info(f"TDengine found in {rootRealPath}")  
        return rootRealPath

tdFindPath = TDFindPath()