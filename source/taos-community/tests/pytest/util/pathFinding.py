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
from util.log import *



class TDFindPath:
    """This class is for finding path within TDengine
    """
    def __init__(self):
        self.file = ""


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

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
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
        selfPath = os.path.dirname(os.path.realpath(self.file))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]
        print(projPath)
        for root, dirs, files in os.walk(projPath):
            if ("sim" in dirs):
                print(root)
                rootRealPath = os.path.realpath(root)
        if (rootRealPath == ""):
            tdLog.exit("TDengine not found!")
        else:
            tdLog.info(f"TDengine found in {rootRealPath}")  
        return rootRealPath

tdFindPath = TDFindPath()