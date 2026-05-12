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

class TestTaosdumpNonRoot:
    def _datadir(self, subdir):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", subdir)

    def _prepare_dir(self, path):
        if os.path.exists(path):
            os.system("rm -rf %s" % path)
        os.makedirs(path)

    def test_taosdump_non_root(self):
        """taosdump privilege

        1. Create database and tables with various data types
        2. Insert data with non-null and null values
        3. Use taosdump to export the database with a non-root user
        4. Drop the original database
        5. Use taosdump to import the database back with a non-root user
        6. Verify the imported database, tables, and data

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_db_with_non_root.py

        """
        tdSql.prepare()

        backupPath = etool.taosDumpFile()
        datadir = self._datadir("privilege")

        # import and verify with taosBackup using pre-generated AVRO backup
        for tool_name, tool in [("taosBackup", backupPath)]:
            tdSql.execute("drop database if exists db")
            tdSql.execute("drop database if exists newdb")
            os.system("%s -i %s -T 1 -W db=newdb" % (tool, datadir))

            tdSql.query("show databases")
            dbresult = tdSql.queryResult

            found = False
            for i in range(len(dbresult)):
                print("Found db: %s" % dbresult[i][0])
                if dbresult[i][0] == "newdb":
                    found = True
                    break

            assert found == True

            tdSql.execute("use newdb")
            tdSql.query("show stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "st")

            tdSql.query("show tables")
            tdSql.checkRows(3)




