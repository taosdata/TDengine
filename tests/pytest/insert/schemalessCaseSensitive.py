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

from util.log import *
from util.cases import *
from util.sql import *
from util.types import TDSmlProtocolType, TDSmlTimestampType
import json

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self._conn = conn

    def run(self):

        # influxDB Line Protocol
        self.influxDBLineProtocol()

        # OpenTSDB Line Protocol
        self.openTSDBLineProtocol()

        # OpenTSDB JSON Protocol
        self.openTSDBJSONProtocol()

    def influxDBLineProtocol(self):
        print("===== influxDB Line Protocol Case Sensitive Test =====\n")
        tdSql.execute("create database influxdb precision 'ns' ")
        tdSql.execute("use influxdb")
        lines = [   
                    "St,deviceId=1i voltage=1,phase=\"Test\" 1626006833639000000",
                    "St,DeviceId=3i voltage=2,phase=\"Test\" 1626006833639000000",
                    "St,deviceId=2i,DeviceId=3 Voltage=2,Phase=\"Test2\" 1626006833639000000",                    
                    "St,deviceId=4i,DeviceId=3 voltage=1,phase=\"Test\",Voltage=2,Phase=\"Test1\" 1626006833639000000",
                    "tbl,deviceId=\"sensor0\" Hello=3i 1646053743694400029",
                    "tbl,deviceId=\"sensor0\" n=3i,N=4i 1646053743694400030",
                    "tbl,deviceId=\"sensor0\" g=3i 1646053743694400031",
                    "tbl,deviceId=\"sensor0\" G=3i 1646053743694400032",
                    "tbl,deviceId=\"sensor0\" nice=2i,Nice=3i 1646053743694400033",
                    "tbl,deviceId=\"sensor0\" hello=3i 1646053743694400034",
                    "超级表,deviceId=\"sensor0\" 电压=3i 1646053743694400035",
                ]

        self._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        tdSql.query("show stables")
        tdSql.checkRows(3)

        tdSql.query("show tables")
        tdSql.checkRows(6)

        tdSql.query("describe `St`")
        tdSql.checkRows(7)

        tdSql.query("select * from `St`")
        tdSql.checkRows(4)

        tdSql.query("select * from tbl")
        tdSql.checkRows(6)

        tdSql.query("select * from `超级表`")
        tdSql.checkRows(1)

    def openTSDBLineProtocol(self):
        print("===== OpenTSDB Line Protocol Case Sensitive Test =====\n")
        tdSql.execute("create database opentsdbline")
        tdSql.execute("use opentsdbline")

        # format: <metric> <timestamp> <value> <tagk_1>=<tagv_1>[ <tagk_n>=<tagv_n>]
        lines = [
            "meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
            "meters.Current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
            "meters.Current 1648432611249 10.8 Location=California.LosAngeles groupid=3",
            "meters.Current 1648432611249 10.8 Location=California.LosAngeles location=California.SanFrancisco groupid=3",
            "Meters.current 1648432611250 11.3 location=California.LosAngeles Groupid=3",
            "电表 1648432611250 11.3 位置=California.LosAngeles Groupid=3"
        ]
        
        self._conn.schemaless_insert(lines, TDSmlProtocolType.TELNET.value, None)
        tdSql.query("show stables")
        tdSql.checkRows(4)

        tdSql.query("show tables")
        tdSql.checkRows(6)

        tdSql.query("describe `meters.Current`")
        tdSql.checkRows(5)        
        tdSql.checkData(2, 0, "groupid")
        tdSql.checkData(3, 0, "location")
        tdSql.checkData(4, 0, "Location")        

        tdSql.query("describe `Meters.current`")
        tdSql.checkRows(4)
        tdSql.checkData(2, 0, "Groupid")
        tdSql.checkData(3, 0, "location")

        tdSql.query("describe `电表`")
        tdSql.checkRows(4)
        tdSql.checkData(2, 0, "Groupid")
        tdSql.checkData(3, 0, "位置")        

    def openTSDBJSONProtocol(self):
        print("===== OpenTSDB JSON Protocol Case Sensitive Test =====\n")
        tdSql.execute("create database opentsdbjson")
        tdSql.execute("use opentsdbjson")

        lines = [
            {"metric": "meters.current", "timestamp": 1648432611249, "value": 10.3, "tags": {"location": "California.SanFrancisco", "groupid": 2}},
            {"metric": "meters.voltage", "timestamp": 1648432611249, "value": 219, "tags": {"Location": "California.LosAngeles", "groupid": 1}},
            {"metric": "meters.Current", "timestamp": 1648432611250, "value": 12.6, "tags": {"location": "California.SanFrancisco", "groupid": 2}},
            {"metric": "meters.voltage", "timestamp": 1648432611250, "value": 221, "tags": {"location": "California.LosAngeles", "groupid": 1}},
            {"metric": "meters.voltage", "timestamp": 1648432611250, "value": 221, "tags": {"location": "California.LosAngeles", "Location": "California.SanFrancisco", "groupid": 2}},
            {"metric": "电压", "timestamp": 1648432611250, "value": 221, "tags": {"位置": "California.LosAngeles", "groupid": 1}}
        ]

        self._conn.schemaless_insert([json.dumps(lines)], TDSmlProtocolType.JSON.value, None)
        tdSql.query("show stables")
        tdSql.checkRows(4)

        tdSql.query("show tables")
        tdSql.checkRows(6)

        tdSql.query("describe `meters.Current`")
        tdSql.checkRows(4)

        tdSql.query("describe `meters.voltage`")
        tdSql.checkRows(5)
        tdSql.checkData(3, 0, "Location")
        tdSql.checkData(4, 0, "location")

        tdSql.query("describe `电压`")
        tdSql.checkRows(4)
        tdSql.checkData(3, 0, "位置")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())