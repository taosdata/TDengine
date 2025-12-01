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

from new_test_framework.utils import tdLog, tdSql, etool, tdCom


class TestSubqueryBugs:
    clientCfgDict = { "keepColumnName": 1 }
    updatecfgDict = { "clientCfg": clientCfgDict }

    def ts_30189(self):
        tdLog.info("create database ts_30189")
        tdSql.execute(f"create database ts_30189")
        tdSql.execute(f"use ts_30189")
        sqls = [
            "CREATE STABLE `demo` (`_ts` TIMESTAMP, `faev` DOUBLE) TAGS (`deviceid` VARCHAR(256))",
            "CREATE TABLE demo_201000008 USING demo (deviceid) TAGS ('201000008')",
            "CREATE TABLE demo_K201000258 USING demo (deviceid) TAGS ('K201000258')",
            "INSERT INTO demo_201000008 (_ts,faev) VALUES ('2023-11-30 23:59:27.255', 51412.900999999998021)",
            "INSERT INTO demo_201000008 (_ts,faev) VALUES ('2023-12-04 23:11:28.179', 51458.900999999998021)",
            "INSERT INTO demo_201000008 (_ts,faev) VALUES ('2023-12-04 23:12:28.180', 51458.800999999999476)",
            "INSERT INTO demo_201000008 (_ts,faev) VALUES ('2023-12-31 23:59:36.108', 52855.400999999998021)",
            "INSERT INTO demo_K201000258 (_ts,faev) VALUES ('2023-11-30 23:59:00.365', 258839.234375000000000)",
            "INSERT INTO demo_K201000258 (_ts,faev) VALUES ('2023-12-28 05:00:00.381', 272188.843750000000000)",
            "INSERT INTO demo_K201000258 (_ts,faev) VALUES ('2023-12-28 05:01:00.600', 13.909012794494629)",
            "INSERT INTO demo_K201000258 (_ts,faev) VALUES ('2023-12-31 23:59:00.366', 1886.711303710937500)",
        ]
        tdSql.executes(sqls)
        sql1 = '''
            SELECT ts, deviceid, faev FROM (
                (
                    SELECT deviceid, ts, faev FROM (
                        SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev 
                        FROM demo 
                        WHERE deviceid in ('201000008') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' 
                        PARTITION BY deviceid 
                    ) WHERE diff_faev < 0 
                ) UNION ALL 
                ( 
                    SELECT deviceid, ts, faev FROM ( 
                        SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev 
                        FROM ( SELECT deviceid, _ts as ts , faev FROM demo 
                        WHERE deviceid in ('201000008') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' 
                        ORDER BY ts desc ) PARTITION BY deviceid 
                    ) WHERE diff_faev > 0 
                ) 
                UNION ALL 
                ( 
                    SELECT deviceid, LAST(_ts) AS ts, LAST(faev) AS faev FROM demo
                    WHERE deviceid in ('201000008') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' 
                    PARTITION BY deviceid INTERVAL(1n) 
                )
            ) order by ts
                '''
        tdSql.query(sql1)
        tdSql.checkRows(4)

        row1 = ['2023-11-30 23:59:27.255', "201000008", 51412.900999999998021]
        row2 = ['2023-12-04 23:11:28.179', "201000008", 51458.900999999998021]
        row3 = ['2023-12-04 23:12:28.180', "201000008", 51458.800999999999476]
        row4 = ['2023-12-31 23:59:36.108', "201000008", 52855.400999999998021]

        rows = [row1, row2, row3, row4]
        tdSql.checkDataMem(sql1, rows)

    def ts_5443(self):
        tdLog.info("create database ts_5443")
        tdSql.execute("create database ts_5443")
        tdSql.execute("use ts_5443")
        sqls = [
            "CREATE STABLE demo (ts TIMESTAMP, site NCHAR(8), expected BIGINT) TAGS (group_id BIGINT UNSIGNED)",
            "CREATE TABLE demo_1 USING demo (group_id) TAGS (1)",
            "INSERT INTO demo_1 VALUES ('2022-10-25 16:05:00.000', 'MN-01', 1)",
            "CREATE TABLE demo_2 USING demo (group_id) TAGS (2)",
            "INSERT INTO demo_2 VALUES ('2022-10-25 16:10:00.000', 'MN-02', 2)",
            "CREATE TABLE demo_3 USING demo (group_id) TAGS (3)",
            "INSERT INTO demo_3 VALUES ('2022-10-25 16:15:00.000', 'MN-03', 3)",
        ]
        tdSql.executes(sqls)
        # test result of order by in plain query
        query = '''
            SELECT _wend, site, SUM(expected) AS check
            FROM ts_5443.demo
            PARTITION BY site INTERVAL(5m) SLIDING (5m)
            ORDER BY 1 DESC, 2, 3
                '''
        tdSql.query(query)
        tdSql.checkRows(3)
        rows = [
            ['2022-10-25 16:20:00.000', 'MN-03', 3],
            ['2022-10-25 16:15:00.000', 'MN-02', 2],
            ['2022-10-25 16:10:00.000', 'MN-01', 1],
        ]
        tdSql.checkDataMem(query, rows)
        # test order by position alias within subquery
        query = '''
            SELECT COUNT(*) FROM (
                SELECT _wend, site, SUM(expected) AS check
                FROM ts_5443.demo
                PARTITION BY site INTERVAL(5m) SLIDING (5m)
                ORDER BY 1 DESC, 2, 3
            ) WHERE check <> 0
                '''
        tdSql.query(query)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)
        # test order by target name within subquery
        query = '''
            SELECT COUNT(*) FROM (
                SELECT _wend, site, SUM(expected) AS check
                FROM ts_5443.demo
                PARTITION BY site INTERVAL(5m) SLIDING (5m)
                ORDER BY _wend DESC, site, check
            ) WHERE check <> 0
                '''
        tdSql.query(query)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)
        # test having clause within subquery
        query = '''
            SELECT COUNT(*) FROM (
                SELECT _wend, site, SUM(expected) AS check
                FROM ts_5443.demo
                PARTITION BY site INTERVAL(5m) SLIDING (5m)
                HAVING _wend > '2022-10-25 16:13:00.000'
            ) WHERE check <> 0
                '''
        tdSql.query(query)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

    def ts_5878(self):
        # prepare data
        tdLog.info("create database ts_5878")
        tdSql.execute("create database ts_5878")
        tdSql.execute("use ts_5878")
        sqls = [
            "CREATE STABLE meters (ts timestamp, c1 int) TAGS (gid int)",
            "CREATE TABLE d0 USING meters (gid) TAGS (0)",
            "CREATE TABLE d1 USING meters (gid) TAGS (1)",
            "CREATE TABLE d2 USING meters (gid) TAGS (2)",
            "INSERT INTO d0 VALUES ('2025-01-01 00:00:00', 0)",
            "INSERT INTO d1 VALUES ('2025-01-01 00:01:00', 1)",
            "INSERT INTO d2 VALUES ('2025-01-01 00:02:00', 2)"
        ]
        tdSql.executes(sqls)
        # check column name in query result
        sql1 = "SELECT * FROM (SELECT LAST_ROW(ts) FROM d1)"
        cols = ["ts"]
        rows = [["2025-01-01 00:01:00"]]
        colNames = tdSql.getColNameList(sql1)
        tdSql.checkColNameList(colNames, cols)
        tdSql.checkDataMem(sql1, rows)

        sql2 = "SELECT * FROM (SELECT LAST(ts) FROM meters PARTITION BY tbname) ORDER BY 1"
        cols = ["ts"]
        rows = [
            ["2025-01-01 00:00:00"],
            ["2025-01-01 00:01:00"],
            ["2025-01-01 00:02:00"],
        ]
        colNames = tdSql.getColNameList(sql2)
        tdSql.checkColNameList(colNames, cols)
        tdSql.checkDataMem(sql2, rows)

    # run
    def test_query_sub_bugs(self):
        """Subquery bugs

        1. Verify bug TS-30189
        2. Verify bug TS-5443
        3. Verify bug TS-5878
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-21 Alex Duan Migrated from uncatalog/army/query/subquery/test_subquery_bugs.py

        """

        # TS-30189
        self.ts_30189()
        # TS-5443
        self.ts_5443()
        # TS-5878
        self.ts_5878()

