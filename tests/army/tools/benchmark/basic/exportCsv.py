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
import json
import csv

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


# reomve single and double quotation
def removeQuotation(origin):
    value = ""
    for c in origin:
        if c != '\'' and c != '"':
            value += c

    return value

class TDTestCase:
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getPath(self, tool="taosBenchmark"):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if "community" in selfPath:
            projPath = selfPath[: selfPath.find("community")]
        elif "src" in selfPath:
            projPath = selfPath[: selfPath.find("src")]
        elif "/tools/" in selfPath:
            projPath = selfPath[: selfPath.find("/tools/")]
        else:
            projPath = selfPath[: selfPath.find("tests")]

        paths = []
        for root, dummy, files in os.walk(projPath):
            if (tool) in files:
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if "packaging" not in rootRealPath:
                    paths.append(os.path.join(root, tool))
                    break
        if len(paths) == 0:
            tdLog.exit("taosBenchmark not found!")
            return
        else:
            tdLog.info("taosBenchmark found in %s" % paths[0])
            return paths[0]
    
    # check correct    
    def checkCorrect(self, csvFile, allRows, interlaceRows):
        # open as csv 
        count = 0
        batch = 0
        name  = ""
        with open(csvFile) as file:
            rows = csv.reader(file)
            for row in rows:
                # interlaceRows
                if name == "":
                    name  = row[0]
                    batch = 1
                else:
                    if name == row[0]:
                        batch += 1
                    else:
                        # switch to another child table
                        if batch != interlaceRows:
                            tdLog.exit(f"interlaceRows invalid. tbName={name} real={batch} expect={interlaceRows} i={count} csvFile={csvFile}")
                        batch = 1
                        name  = row[0]             
                # count ++
                count += 1
        # batch
        if batch != interlaceRows:
            tdLog.exit(f"interlaceRows invalid. tbName={name} real={batch} expect={interlaceRows} i={count} csvFile={csvFile}")

        # check all rows
        if count != allRows:
            tdLog.exit(f"allRows invalid. real={count} expect={allRows} csvFile={csvFile}")

        tdLog.info(f"Check generate csv file successfully. csvFile={csvFile} count={count} interlaceRows={batch}")
    
    # check result
    def checResult(self, jsonFile):
         # csv
        with open(jsonFile) as file:
             data = json.load(file)

         # read json
        database = data["databases"][0]
        out      = data["csvPath"]
        dbName   = database["dbinfo"]["name"]
        stables  = database["super_tables"]
        for stable in stables:
            stbName = stable["name"]
            childs  = stable["childtable_count"]
            insertRows    = stable["insert_rows"]
            interlaceRows = stable["interlace_rows"]
            csvFile = f"{out}{dbName}-{stbName}.csv"
            rows = childs * insertRows
            if interlaceRows == 0:
                interlaceRows = insertRows
            # check csv context correct
            self.checkCorrect(csvFile, rows, interlaceRows)

    def checkExportCsv(self, benchmark, jsonFile, options=""):
        # exec
        cmd = f"{benchmark} {options} -f {jsonFile}"
        os.system(cmd)

        # check result
        self.checResult(jsonFile)
 
    def run(self):
        # path
        benchmark = self.getPath()

        # do check
        json = "taosbenchmark/json/exportCsv.json"
        self.checkExportCsv(benchmark, json)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
