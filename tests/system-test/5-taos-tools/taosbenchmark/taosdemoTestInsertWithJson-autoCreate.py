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

import sys
import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

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
        return buildPath

    def run(self):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert*_res.txt*")
        os.system("rm -rf 5-taos-tools/taosbenchmark/%s.sql" % testcaseFilename )         

        # # insert: auto_create

        tdSql.execute('drop database if exists db')
        tdSql.execute('create database db')
        tdSql.execute('use db')
        os.system("%staosBenchmark -y -f 5-taos-tools/taosbenchmark/insert-drop-exist-auto-N00.json " % binPath) # drop = no, child_table_exists, auto_create_table varies
        tdSql.execute('use db')
        tdSql.query('show tables like \'NN123%\'')  #child_table_exists = no, auto_create_table varies = 123
        tdSql.checkRows(20)
        tdSql.query('show tables like \'NNN%\'')    #child_table_exists = no, auto_create_table varies = no
        tdSql.checkRows(20)
        tdSql.query('show tables like \'NNY%\'')    #child_table_exists = no, auto_create_table varies = yes
        tdSql.checkRows(20)
        tdSql.query('show tables like \'NYN%\'')    #child_table_exists = yes, auto_create_table varies = no
        tdSql.checkRows(0)
        tdSql.query('show tables like \'NY123%\'')  #child_table_exists = yes, auto_create_table varies = 123
        tdSql.checkRows(0)
        tdSql.query('show tables like \'NYY%\'')    #child_table_exists = yes, auto_create_table varies = yes
        tdSql.checkRows(0)

        tdSql.execute('drop database if exists db')
        os.system("%staosBenchmark -y -f 5-taos-tools/taosbenchmark/insert-drop-exist-auto-Y00.json " % binPath) # drop = yes, child_table_exists, auto_create_table varies
        tdSql.execute('use db')
        tdSql.query('show tables like \'YN123%\'')  #child_table_exists = no, auto_create_table varies = 123
        tdSql.checkRows(20)
        tdSql.query('show tables like \'YNN%\'')    #child_table_exists = no, auto_create_table varies = no
        tdSql.checkRows(20)
        tdSql.query('show tables like \'YNY%\'')    #child_table_exists = no, auto_create_table varies = yes
        tdSql.checkRows(20)
        tdSql.query('show tables like \'YYN%\'')    #child_table_exists = yes, auto_create_table varies = no
        tdSql.checkRows(20)
        tdSql.query('show tables like \'YY123%\'')  #child_table_exists = yes, auto_create_table varies = 123
        tdSql.checkRows(20)
        tdSql.query('show tables like \'YYY%\'')    #child_table_exists = yes, auto_create_table varies = yes
        tdSql.checkRows(20)


        # rm useless files
        os.system("rm -rf ./insert*_res.txt*")


        
        
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
