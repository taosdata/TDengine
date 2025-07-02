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
from new_test_framework.utils import tdLog, tdSql, etool
import os


class TestReuseExistStb:
    def caseDescription(self):
        """
        [TD-22190] taosBenchmark reuse exist stb test cases
        """



    def test_reuse_exist_stb(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.res[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db")
        tdSql.execute("use db")
        tdSql.execute("create table stb (ts timestamp, c0 int)  tags (t0 int)")
        tdSql.execute("insert into stb_0 using stb tags (0) values (now, 0)")
        #        sys.exit(0)
        cmd = "%s -f ./tools/benchmark/basic/json/reuse-exist-stb.json" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.query("select count(*) from db.new_0")
        tdSql.checkData(0, 0, 5)
        tdSql.query("select count(*) from db.stb_0")
        tdSql.checkData(0, 0, 1)

        if major_ver == "3":
            tdSql.query("select count(*) from (select distinct(tbname) from db.stb)")
        else:
            tdSql.query("select count(tbname) from db.stb")
        tdSql.checkData(0, 0, 2)

        tdLog.success("%s successfully executed" % __file__)


