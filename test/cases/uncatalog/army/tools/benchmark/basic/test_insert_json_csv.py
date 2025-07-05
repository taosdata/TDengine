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

class TestInsertJsonCsv:
    def test_insert_json_csv(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
        tdSql.query("select client_version()")
        client_ver = "".join(tdSql.queryResult[0])
        major_ver = client_ver.split(".")[0]

        binPath = etool.benchMarkFile()
        if binPath == "":
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark found in %s" % binPath)

        # insert: create one  or multiple tables per sql and insert multiple rows per sql
        # test case for https://jira.taosdata.com:18080/browse/TD-4985
        os.system("%s -f %s/json/insert-json-csv.json -y " % (binPath, os.path.dirname(__file__)))

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

        tdLog.success("%s successfully executed" % __file__)


