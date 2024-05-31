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

import frame.etool

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *


class TDTestCase(TBase):
    
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

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # TS-30189
        self.ts_30189()


        tdLog.success(f"{__file__} successfully executed")

        

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
