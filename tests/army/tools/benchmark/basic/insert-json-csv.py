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
import time
import frame
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def run(self):
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.res[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        if binPath == "":
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark found in %s" % binPath)

        # insert: create one  or multiple tables per sql and insert multiple rows per sql
        # test case for https://jira.taosdata.com:18080/browse/TD-4985
        os.system("%s -f ./tools/benchmark/basic/json/insert-json-csv.json -y " % binPath)

        tdSql.execute("use db")
        if major_ver == "3":
            tdSql.query("select count(*) from (select distinct(tbname) from stb0)")
        else:
            tdSql.query("select count(tbname) from stb0")
        tdSql.checkData(0, 0, 1)

        tdSql.query("select * from stb0 where  tbname like 'stb00_0'  limit 10")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 2)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
