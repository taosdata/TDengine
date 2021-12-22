#!/usr/bin/python
###################################################################
#           Copyright (c) 2016 by TAOS Technologies,  Inc.
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
import sys
import getopt
import os
from fabric2 import Connection

sys.path.append("pytest")
import importlib
import re


class CatalogGen:
    def __init__(self, CaseDirList, CatalogName, DirDepth):
        self.CaseDirList = CaseDirList
        self.CatalogName = CatalogName
        self.blacklist = []
        self.DirDepth = DirDepth

    def CatalogGen(self):
        for i in self.CaseDirList:
            self.GetCatalog(i)
            self.CollectLog(i)
        print("Catalog Generation done")

    def CollectLog(self, CaseDir):
        DirorFiles = os.listdir(CaseDir)
        for loop in range(self.DirDepth):
            for i in DirorFiles:
                fileName = os.path.join(CaseDir, i)
                if os.path.isdir(fileName):
                    self.CollectLog(fileName)
                else:
                    if i == self.CatalogName and fileName not in self.blacklist:
                        self.blacklist.append(fileName)
                        with open(fileName, "r") as f:
                            Catalog = f.read()
                        title = CaseDir.split("/")[-1]
                        TitleLevel = CaseDir.count("/")
                        with open(
                            os.path.dirname(CaseDir) + "/" + self.CatalogName, "a"
                        ) as f:
                            f.write("#" * TitleLevel + " %s\n" % title)
                            f.write(Catalog)

    def GetCatalog(self, CaseDir):
        for root, dirs, files in os.walk(CaseDir):
            for file in files:
                if file.endswith(".py"):
                    fileName = os.path.join(root, file)
                    moduleName = fileName.replace(".py", "").replace("/", ".")
                    uModule = importlib.import_module(moduleName)
                    title = file.split(".")[0]
                    TitleLevel = root.count("/") + 1
                    try:
                        ucase = uModule.TDTestCase()
                        with open(root + "/" + self.CatalogName, "a") as f:
                            f.write("#" * TitleLevel + " %s\n" % title)
                            for i in ucase.caseDescription.__doc__.split("\n"):
                                if i.lstrip() == "":
                                    continue
                                if re.match("^case.*:", i.strip()):
                                    f.write("* " + i.strip() + "\n")
                                else:
                                    f.write(i.strip() + "\n")
                    except:
                        print(fileName)

    def CleanCatalog(self):
        for i in self.CaseDirList:
            for root, dirs, files in os.walk(i):
                for file in files:
                    if file == self.CatalogName:
                        os.remove(root + "/" + self.CatalogName)
        print("clean is done")


if __name__ == "__main__":
    CaseDirList = []
    CatalogName = ""
    DirDepth = 0
    generate = True
    delete = True
    opts, args = getopt.gnu_getopt(sys.argv[1:], "d:c:v:n:th")
    for key, value in opts:
        if key in ["-h", "--help"]:
            print("A collection of test cases catalog written using Python")
            print(
                "-d root dir of test case files written by Python, default: system-test,develop-test"
            )
            print("-c catalog file name, default: catalog.md")
            print("-v dir depth of test cases.default: 5")
            print("-n <True:False> generate")
            print("-r <True:False> delete")
            sys.exit(0)

        if key in ["-d"]:
            CaseDirList = value.split(",")

        if key in ["-c"]:
            CatalogName = value

        if key in ["-v"]:
            DirDepth = int(value)

        if key in ["-n"]:
            if value.upper() == "TRUE":
                generate = True
            elif value.upper() == "FALSE":
                generate = False

        if key in ["-r"]:
            if value.upper() == "TRUE":
                delete = True
            elif value.upper() == "FALSE":
                delete = False

    print(CaseDirList, CatalogName)
    if CaseDirList == []:
        CaseDirList = ["system-test", "develop-test"]
    if CatalogName == "":
        CatalogName = "catalog.md"
    if DirDepth == 0:
        DirDepth = 5
    print(
        "opt:\n\tcatalogname: %s\n\tcasedirlist: %s\n\tdepth: %d\n\tgenerate: %s\n\tdelete: %s"
        % (CatalogName, ",".join(CaseDirList), DirDepth, generate, delete)
    )
    f = CatalogGen(CaseDirList, CatalogName, DirDepth)
    if delete:
        f.CleanCatalog()
    if generate:
        f.CatalogGen()
