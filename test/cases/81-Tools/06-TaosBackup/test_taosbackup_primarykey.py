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
import json
import os


class TestTaosBackupPrimaryKey:
    def exec(self, command):
        tdLog.info(command)
        return os.system(command)

    def findPrograme(self):
        taosbackup = etool.taosBackupFile()
        if taosbackup == "":
            tdLog.exit("taosBackup not found!")
        else:
            tdLog.info("taosBackup found at %s" % taosbackup)

        benchmark = etool.benchMarkFile()
        if benchmark == "":
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark found at %s" % benchmark)

        tmpdir = "./tmp"
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)
        else:
            tdLog.info("tmp directory exists, clearing data.")
            os.system("rm -rf %s/*" % tmpdir)

        return taosbackup, benchmark, tmpdir

    def checkCorrectWithJson(self, jsonFile, newdb=None, checkInterval=False):
        with open(jsonFile, "r") as f:
            data = json.load(f)
        db = newdb if newdb else data["databases"][0]["dbinfo"]["name"]
        stb = data["databases"][0]["super_tables"][0]["name"]
        child_count = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows = data["databases"][0]["super_tables"][0]["insert_rows"]
        timestamp_step = data["databases"][0]["super_tables"][0]["timestamp_step"]

        tdLog.info(
            f"check json: db={db} stb={stb} child_count={child_count} insert_rows={insert_rows}"
        )
        sql = f"select * from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkRows(child_count * insert_rows)

        if checkInterval:
            sql = (
                f"select * from (select diff(ts) as dif from {db}.{stb} partition by tbname) "
                f"where dif != {timestamp_step};"
            )
            tdSql.query(sql)
            tdSql.checkRows(0)

    def insertData(self, benchmark, jsonFile, db):
        # Insert super table data via taosBenchmark
        self.exec(f"{benchmark} -f {jsonFile}")
        self.checkCorrectWithJson(jsonFile)

        # Insert normal table data
        sqls = [
            f"create table {db}.ntb(st timestamp, c1 int, c2 binary(32))",
            f"insert into {db}.ntb values(now, 1, 'abc1')",
            f"insert into {db}.ntb values(now, 2, 'abc2')",
            f"insert into {db}.ntb values(now, 3, 'abc3')",
            f"insert into {db}.ntb values(now, 4, 'abc4')",
            f"insert into {db}.ntb values(now, 5, 'abc5')",
        ]
        for sql in sqls:
            tdSql.execute(sql)

    def dumpOut(self, taosbackup, db, outdir):
        self.exec(f"{taosbackup} -D {db} -o {outdir}")

    def dumpIn(self, taosbackup, db, newdb, indir):
        self.exec(f'{taosbackup} -W "{db}={newdb}" -i {indir}')

    def check_same(self, db, newdb, stb, aggfun):
        sql = f"select {aggfun} from {db}.{stb}"
        tdSql.query(sql)
        sum1 = tdSql.getData(0, 0)
        sql = f"select {aggfun} from {newdb}.{stb}"
        tdSql.query(sql)
        sum2 = tdSql.getData(0, 0)
        if sum1 == sum2:
            tdLog.info(f"{aggfun} source:{sum1} import:{sum2} equal.")
        else:
            tdLog.exit(f"{aggfun} source:{sum1} import:{sum2} NOT equal.")

    def verifyResult(self, db, newdb, jsonFile):
        self.checkCorrectWithJson(jsonFile, newdb)
        stb = "meters"
        self.check_same(db, newdb, stb, "sum(pk)")
        self.check_same(db, newdb, stb, "sum(usi)")
        self.check_same(db, newdb, stb, "sum(ic)")
        self.check_same(db, newdb, "ntb", "sum(c1)")

    def test_taosbackup_primarykey(self):
        """taosBackup primary key

        1. Prepare database with primary key using taosBenchmark (json/primaryKey.json)
        2. Add a normal table with 5 rows
        3. Dump out database using taosBackup
        4. Dump in database using taosBackup with rename (-W db=newdb)
        5. Verify data correctness with sum aggregation on pk, usi, ic columns
        6. Verify normal table row count via sum(c1)

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-04 Alex Duan Migrated and adapted from 04-Taosdump/test_taosdump_primarykey.py

        """
        db = "pridb"
        newdb = "npridb"

        taosbackup, benchmark, tmpdir = self.findPrograme()
        jsonFile = f"{os.path.dirname(os.path.abspath(__file__))}/json/primaryKey.json"

        self.insertData(benchmark, jsonFile, db)
        tdSql.execute(f"drop database if exists {newdb}")
        self.dumpOut(taosbackup, db, tmpdir)
        self.dumpIn(taosbackup, db, newdb, tmpdir)
        self.verifyResult(db, newdb, jsonFile)

        tdLog.info("test_taosbackup_primarykey ................... [passed]")
