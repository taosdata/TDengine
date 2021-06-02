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
from util.log import *
from util.cases import *
from util.sql import *


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

        # insert: auto_create
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-drop-exist-auto-YYY.json -y " % binPath) # drop = yes, exist = yes, auto_create = yes
        tdSql.execute('use db')
        tdSql.query('show tables')
        tdSql.checkRows(20)

        os.system("%staosdemo -f tools/taosdemoAllTest/insert-drop-exist-auto-YYN.json -y " % binPath) # drop = yes, exist = yes, auto_create = no
        tdSql.execute('use db')
        tdSql.query('show tables')
        tdSql.checkRows(20)

        os.system("%staosdemo -f tools/taosdemoAllTest/insert-drop-exist-auto-YNY.json -y " % binPath) # drop = yes, exist = no, auto_create = yes
        tdSql.execute('use db')
        tdSql.query('show tables')
        tdSql.checkRows(20)

        os.system("%staosdemo -f tools/taosdemoAllTest/insert-drop-exist-auto-YNN.json -y " % binPath) # drop = yes, exist = no, auto_create = no
        tdSql.execute('use db')
        tdSql.query('show tables')
        tdSql.checkRows(20)

        tdSql.execute('drop database db')
        tdSql.execute('create database db')
        tdSql.execute('use db')
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-drop-exist-auto-NYY.json -y " % binPath) # drop = no, exist = yes, auto_create = yes
        tdSql.query('show tables')
        tdSql.checkRows(0)

        tdSql.execute('drop database db')
        tdSql.execute('create database db')
        tdSql.execute('use db')
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-drop-exist-auto-NYN.json -y " % binPath) # drop = no, exist = yes, auto_create = no
        tdSql.execute('use db')
        tdSql.query('show tables')
        tdSql.checkRows(0)

        tdSql.execute('drop database db')
        tdSql.execute('create database db')
        tdSql.execute('use db')
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-drop-exist-auto-NNY.json -y " % binPath) # drop = no, exist = no, auto_create = yes
        tdSql.execute('use db')
        tdSql.query('show tables')
        tdSql.checkRows(20)

        tdSql.execute('drop database db')
        tdSql.execute('create database db')
        tdSql.execute('use db')
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-drop-exist-auto-NNN.json -y " % binPath) # drop = no, exist = no, auto_create = no
        tdSql.execute('use db')
        tdSql.query('show tables')
        tdSql.checkRows(20)

        #the following four test cases are for the exception cases for param auto_create_table

        tdSql.execute('drop database db')
        tdSql.execute('create database db')
        tdSql.execute('use db')
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-drop-exist-auto-NN123.json -y " % binPath) # drop = no, exist = no, auto_create = 123
        tdSql.execute('use db')
        tdSql.query('show tables')
        tdSql.checkRows(20)

        tdSql.execute('drop database db')
        tdSql.execute('create database db')
        tdSql.execute('use db')
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-drop-exist-auto-NY123.json -y " % binPath) # drop = no, exist = yes, auto_create = 123
        tdSql.execute('use db')
        tdSql.query('show tables')
        tdSql.checkRows(0)

        os.system("%staosdemo -f tools/taosdemoAllTest/insert-drop-exist-auto-YN123.json -y " % binPath) # drop = yes, exist = no, auto_create = 123
        tdSql.execute('use db')
        tdSql.query('show tables')
        tdSql.checkRows(20)

        os.system("%staosdemo -f tools/taosdemoAllTest/insert-drop-exist-auto-YY123.json -y " % binPath) # drop = yes, exist = yes, auto_create = 123
        tdSql.execute('use db')
        tdSql.query('show tables')
        tdSql.checkRows(20)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
