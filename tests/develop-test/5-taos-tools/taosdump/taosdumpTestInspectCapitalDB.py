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
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def caseDescription(self):
        """
        case1<sdsang>: [TD-14544] taosdump data inspect
        """

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.tmpdir = "tmp"

    def getPath(self, tool="taosdump"):
        if (platform.system().lower() == 'windows'):
            tool = tool + ".exe"
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if ((tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, tool))
                    break
        if (len(paths) == 0):
            tdLog.exit("taosBenchmark not found!")
            return
        else:
            tdLog.info("taosBenchmark found in %s" % paths[0])
            return paths[0]

    def run(self):
        taosBenchBinPath = self.getPath(tool="taosBenchmark")
        os.system(f"{taosBenchBinPath} -E -d 'DBt' -n 100 -t 100 -y ")

        tdSql.execute("drop database if exists `DB` ")
        tdSql.execute("create database  `DB` keep 3649 ")

        tdSql.execute("use `DB`")
        tdSql.execute(
            "create table st(ts timestamp, c1 INT, c2 BOOL, c3 TINYINT, c4 SMALLINT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 TIMESTAMP, c9 BINARY(10), c10 NCHAR(10), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED) tags(n1 INT, w2 BOOL, t3 TINYINT, t4 SMALLINT, t5 BIGINT, t6 FLOAT, t7 DOUBLE, t8 TIMESTAMP, t9 BINARY(10), t10 NCHAR(10), t11 TINYINT UNSIGNED, t12 SMALLINT UNSIGNED, t13 INT UNSIGNED, t14 BIGINT UNSIGNED)"
        )
        tdSql.execute(
            "create table t1 using st tags(1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "insert into t1 values(1640000000000, 1, true, 1, 1, 1, 1.0, 1.0, 1, '1', '一', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "create table t2 using st tags(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        tdSql.execute(
            "insert into t2 values(1640000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )

        #        sys.exit(1)

        binPath = self.getPath()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % binPath)

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        os.system("%s -e --databases 'DB' -o %s -T 1" % (binPath, self.tmpdir))

        #        sys.exit(1)

        taosdumpInspectCmd = "%s -I %s/taosdump.*/*.avro* -s | grep 'Schema:'|wc -l" % (
            binPath,
            self.tmpdir,
        )
        schemaTimes = subprocess.check_output(taosdumpInspectCmd, shell=True).decode(
            "utf-8"
        )
        print("schema found times: %d" % int(schemaTimes))

        if int(schemaTimes) != 1:
            caller = inspect.getframeinfo(inspect.stack()[0][0])
            tdLog.exit(
                "%s(%d) failed: expected schema found times 1, actual %d"
                % (caller.filename, caller.lineno, int(schemaTimes))
            )

        taosdumpInspectCmd = (
            "%s -I %s/taosdump*/data*/*.avro* -s | grep 'Schema:'|wc -l"
            % (binPath, self.tmpdir)
        )
        schemaTimes = subprocess.check_output(taosdumpInspectCmd, shell=True).decode(
            "utf-8"
        )
        print("schema found times: %d" % int(schemaTimes))

        if int(schemaTimes) != 2:
            caller = inspect.getframeinfo(inspect.stack()[0][0])
            tdLog.exit(
                "%s(%d) failed: expected schema found times 2, actual %d"
                % (caller.filename, caller.lineno, int(schemaTimes))
            )

        taosdumpInspectCmd = (
            "%s -I %s/taosdump*/*.avro* | grep '=== Records:'|wc -l"
            % (binPath, self.tmpdir)
        )
        recordsTimes = subprocess.check_output(taosdumpInspectCmd, shell=True).decode(
            "utf-8"
        )
        print("records found times: %d" % int(recordsTimes))

        if int(recordsTimes) != 1:
            caller = inspect.getframeinfo(inspect.stack()[0][0])
            tdLog.exit(
                "%s(%d) failed: expected records found times 1, actual %d"
                % (caller.filename, caller.lineno, int(recordsTimes))
            )

        taosdumpInspectCmd = (
            "%s -I %s/taosdump*/data*/*.avro* | grep '=== Records:'|wc -l"
            % (binPath, self.tmpdir)
        )
        recordsTimes = subprocess.check_output(taosdumpInspectCmd, shell=True).decode(
            "utf-8"
        )
        print("records found times: %d" % int(recordsTimes))

        if int(recordsTimes) != 2:
            caller = inspect.getframeinfo(inspect.stack()[0][0])
            tdLog.exit(
                "%s(%d) failed: expected records found times 2, actual %d"
                % (caller.filename, caller.lineno, int(recordsTimes))
            )
        #import database DB 
        tdSql.execute("drop database if exists `DB` ")
        os.system("%s -e --databases 'DB' -i %s -T 1" % (binPath, self.tmpdir))
        tdSql.execute("use  `DB` ")
        tdSql.query("select * from t1 ; ")
        print(tdSql.queryCols)
        result_t1=[1640000000000, 1, 'True', 1, 1, 1, 1.0, 1.0, "1970-01-01 08:00:00.001000", '1', '一', 1, 1, 1, 1]
        for i in range(0,tdSql.queryCols):
            tdSql.checkData(0,i,result_t1[i])
        
        result_t2=[1640000000000, None , None , None , None , None , None , None , None , None , None , None , None , None , None ]
        tdSql.query("select * from t2 ; ")
        for i in range(0,tdSql.queryCols):
            tdSql.checkData(0,i,result_t2[i])
        tdSql.execute("use  `DBt` ")
        tdSql.query("select count(*) from meters ; ")
        tdSql.checkData(0,0,10000)

        if  os.path.exists(self.tmpdir):
            os.system("rm -rf %s" % self.tmpdir)
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
