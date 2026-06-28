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
import json
import tempfile


class TestTaosdumpCommandline:
    def clearPath(self, path):
        os.system("rm -rf %s/*" % path)

    def exec(self, command):
        tdLog.info(command)
        return os.system(command)

    def insertBenchJson(self, json_file):
        """Run taosBenchmark with json_file and return (db, stb, child_count, insert_rows)."""
        with open(json_file, "r") as f:
            data = json.load(f)
        db = data["databases"][0]["dbinfo"]["name"]
        stb = data["databases"][0]["super_tables"][0]["name"]
        child_count = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows = data["databases"][0]["super_tables"][0]["insert_rows"]
        benchmark = etool.benchMarkFile()
        self.exec(f"{benchmark} -f {json_file}")
        return db, stb, child_count, insert_rows


    def checkCorrectWithJson(self, jsonFile, newdb = None, checkInterval = True):
        #
        # check insert result
        #
        with open(jsonFile, "r") as file:
            data = json.load(file)
        
        # db come from arguments
        if newdb is None:
            db = data["databases"][0]["dbinfo"]["name"]
        else:
            db = newdb

        stb            = data["databases"][0]["super_tables"][0]["name"]
        child_count    = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows    = data["databases"][0]["super_tables"][0]["insert_rows"]
        timestamp_step = data["databases"][0]["super_tables"][0]["timestamp_step"]

        tdLog.info(f"get json: db={db} stb={stb} child_count={child_count} insert_rows={insert_rows} \n")
        
        # all count insert_rows * child_table_count
        sql = f"select * from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkRows(child_count * insert_rows)

        # timestamp step
        if checkInterval:
            sql = f"select * from (select diff(ts) as dif from {db}.{stb} partition by tbname) where dif != {timestamp_step};"
            tdSql.query(sql)
            tdSql.checkRows(0)

    def insertData(self, json):
        # insert super table
        db, stb, child_count, insert_rows = self.insertBenchJson(json)
        
        # normal table
        sqls = [
            f"create table {db}.ntb(st timestamp, c1 int, c2 binary(32))",
            f"insert into {db}.ntb values('2025-01-01 10:00:01', 1, 'abc1')",
            f"insert into {db}.ntb values('2025-01-01 10:00:02', 2, 'abc2')",
            f"insert into {db}.ntb values('2025-01-01 10:00:03', 3, 'abc3')",
            f"insert into {db}.ntb values('2025-01-01 10:00:04', 4, 'abc4')",
            f"insert into {db}.ntb values('2025-01-01 10:00:05', 5, 'abc5')",
        ]
        for sql in sqls:
            tdSql.execute(sql)
        
        return db, stb, child_count, insert_rows

    def check_same(self, db, newdb, stb, aggfun):
        # sum pk db
        sql = f"select {aggfun} from {db}.{stb}"
        tdSql.query(sql)
        sum1 = tdSql.getData(0,0)
        # sum pk newdb
        sql = f"select {aggfun} from {newdb}.{stb}"
        tdSql.query(sql)
        sum2 = tdSql.getData(0,0)

        if sum1 == sum2:
            tdLog.info(f"{aggfun} source db:{sum1} import db:{sum2} both equal.")
        else:
            tdLog.exit(f"{aggfun} source db:{sum1} import db:{sum2} not equal.")

    def verifyResult(self, db, newdb, json):
        # compare with insert json
        self.checkCorrectWithJson(json, newdb)
        
        #  compare sum(pk)
        stb = "meters"
        self.check_same(db, newdb, stb, "sum(fc)")
        self.check_same(db, newdb, stb, "sum(ti)")
        self.check_same(db, newdb, stb, "sum(si)")
        self.check_same(db, newdb, stb, "sum(ic)")
        self.check_same(db, newdb, stb, "avg(bi)")
        self.check_same(db, newdb, stb, "sum(uti)")
        self.check_same(db, newdb, stb, "sum(usi)")
        self.check_same(db, newdb, stb, "sum(ui)")
        self.check_same(db, newdb, stb, "avg(ubi)")

        # check normal table
        self.check_same(db, newdb, "ntb", "sum(c1)")

    #  with Native and WebSocket
    def dumpInOutMode(self, mode, db, json_file, tmpdir):
        taosbackup = etool.taosDumpFile()
        newdb = "new" + db

        # dump out with the given connection mode
        self.clearPath(tmpdir)
        self.exec(f'{taosbackup} {mode} -D {db} -o {tmpdir}')

        # dump in and verify
        tdSql.execute(f"drop database if exists {newdb}")
        self.exec(f'{taosbackup} {mode} -W "{db}={newdb}" -i {tmpdir}')
        self.verifyResult(db, newdb, json_file)
        tdSql.execute(f"drop database if exists {newdb}")


    # (old_taosdump-specific commandline tests removed in Phase 2)

    # placeholder — was basicCommandLine
    def _removed_basicCommandLine(self, tmpdir):
        checkItems = [
            [f"-Z 0 -h 127.0.0.1 -P 6030 -uroot -ptaosdata -A -N -o {tmpdir}", ["OK: Database test dumped"]],
            [f"-r result -a -e test d0 -o {tmpdir}", ["OK: table: d0 dumped", "OK: 100 row(s) dumped out!"]],
            [f"-n -D test -o {tmpdir} -d lzma", ["OK: Database test dumped", "OK: 205 row(s) dumped out!"]],
            [f"-Z 0 -gg -P 6030 -n -D test -o {tmpdir}", ["OK: Database test dumped", "OK: 205 row(s) dumped out!"]],
            [f"-L -D test -o {tmpdir}", ["OK: Database test dumped", "OK: 205 row(s) dumped out!"]],
            [f"-s -D test -o {tmpdir}", ["dumping out schema: 1 from meters.d0", "OK: Database test dumped", "OK: 0 row(s) dumped out!"]],
            [f"-N -d deflate -S '2022-10-01 00:00:50.000' test meters  -o {tmpdir}",["OK: table: meters dumped", "OK: 100 row(s) dumped out!"]],
            [f"-N -d lzma    -S '2022-10-01 00:00:50.000' test meters  -o {tmpdir}",["OK: table: meters dumped", "OK: 100 row(s) dumped out!"]],
            [f"-N -d snappy  -S '2022-10-01 00:00:50.000' test meters  -o {tmpdir}",["OK: table: meters dumped", "OK: 100 row(s) dumped out!"]],
            [f" -S '2022-10-01 00:00:50.000' -E '2022-10-01 00:00:60.000' test meters  -o {tmpdir}",["OK: table: meters dumped", "OK: 22 row(s) dumped out!"]],
            [f"-T 2 -B 1000 -S '2022-10-01 00:00:50.000' -E '2022-10-01 00:00:60.000' test meters -o {tmpdir}", ["OK: table: meters dumped", "OK: 22 row(s) dumped out!"]],
            [f"-g -E '2022-10-01 00:00:60.000' test -o {tmpdir}", ["OK: Database test dumped", "OK: 122 row(s) dumped out!"]],
            [f"--help", ["Report bugs to"]],
            [f"-?", ["Report bugs to"]],
            [f"-V", ["version:"]],
            [f"--usage", ["taosdump [OPTION...] -o outpath"]],
            # conn mode -Z
            [f"-Z   0 -E '2022-10-01 00:00:60.000' test -o {tmpdir}", [
                "Connect mode is : Native", 
                "OK: Database test dumped", 
                "OK: 122 row(s) dumped out!"]
            ],
            [f"-Z  1 -E '2022-10-01 00:00:60.000' test -o {tmpdir}", [
                "Connect mode is : WebSocket",
                "OK: Database test dumped", 
                "OK: 122 row(s) dumped out!"]
            ],
        ]

        # executes 
        for item in checkItems:
            self.clearPath(tmpdir) # clear tmp
            command = item[0]
            results = item[1]
            pass  # removed

    def test_taosdump_commandline(self):
        """taosdump commandline: export and import with native and websocket modes

        1. Insert data with taosBenchmark
        2. Export with new taosdump in Native mode, import and verify
        3. Export with new taosdump in WebSocket mode, import and verify

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-10-29 Alex Duan Migrated from uncatalog/army/tools/taosdump/native/test_taosdump_commandline.py

        """
        json_file = f"{os.path.dirname(os.path.abspath(__file__))}/json/insertFullType.json"
        tmpdir = os.path.join(tempfile.mkdtemp(), "commandline_test")
        os.makedirs(tmpdir, exist_ok=True)

        # insert source data
        db, stb, childCount, insertRows = self.insertData(json_file)

        # test dump in/out with native and websocket connection modes
        modes = ["-Z native", "-Z websocket -X http://localhost:6041"]
        for mode in modes:
            self.dumpInOutMode(mode, db, json_file, tmpdir)
            tdLog.info(f"{mode} dumpIn Out .......................... [Passed]")

        # cleanup tmpdir
        os.system(f"rm -rf {tmpdir}")


