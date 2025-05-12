from enum import Enum

from util.log import *
from util.sql import *
from util.cases import *
from util.csv import *
import os
import taos
import json
from taos import SmlProtocol, SmlPrecision
from taos.error import SchemalessError

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), True)


    def run(self):  
        conn = taos.connect()

        conn.execute("drop database if exists reproduce")
        conn.execute("CREATE DATABASE reproduce")
        conn.execute("USE reproduce")

        # influxDB
        conn.execute("drop table if exists meters")
        lines1 = ["meters,location=California.LosAngeles groupid=2,current=11i32,voltage=221,phase=0.28 1648432611249000",]
        lines2 = ["meters,location=California.LosAngeles,groupid=2 groupid=2,current=11i32,voltage=221,phase=0.28 1648432611249001",]
        lines3 = ["meters,location=California.LosAngeles,groupid=2 current=11i32,voltage=221,phase=0.28 1648432611249002",]
        
        try:
            conn.schemaless_insert(lines1, SmlProtocol.LINE_PROTOCOL, SmlPrecision.MICRO_SECONDS)
            conn.schemaless_insert(lines2, SmlProtocol.LINE_PROTOCOL, SmlPrecision.MICRO_SECONDS)
            tdSql.checkEqual('expected error', 'no error occurred')
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg, 'Duplicated column names')
        
        try:
            conn.schemaless_insert(lines3, SmlProtocol.LINE_PROTOCOL, SmlPrecision.MICRO_SECONDS)
            tdSql.checkEqual('expected error', 'no error occurred')
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg, 'Duplicated column names')
        

        # OpenTSDB
        conn.execute("drop table if exists meters")
        lines1 = ["meters 1648432611249 10i32 location=California.SanFrancisco groupid=2 groupid=3",]
        lines2 = ["meters 1648432611250 10i32 groupid=2 location=California.SanFrancisco groupid=3",]
        
        try:
            conn.schemaless_insert(lines1, SmlProtocol.TELNET_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
            tdSql.checkEqual('expected error', 'no error occurred')
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg, 'Duplicated column names')

        try:
            conn.schemaless_insert(lines2, SmlProtocol.TELNET_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
            tdSql.checkEqual('expected error', 'no error occurred')
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg, 'Duplicated column names')

        # OpenTSDB Json
        conn.execute("drop table if exists meters")
        lines1 = [{"metric": "meters", "timestamp": 1648432611249, "value": "a32", "tags": {"location": "California.SanFrancisco", "groupid": 2, "groupid": 3}}]
        lines2 = [{"metric": "meters", "timestamp": 1648432611250, "value": "a32", "tags": {"groupid": 2, "location": "California.SanFrancisco", "groupid": 4}}]
        try:
            lines = json.dumps(lines1)
            conn.schemaless_insert([lines], SmlProtocol.JSON_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
            # tdSql.checkEqual('expected error', 'no error occurred')     TD-29850
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg, 'Duplicated column names')
        
        try:
            lines = json.dumps(lines2)
            conn.schemaless_insert([lines], SmlProtocol.JSON_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
            # tdSql.checkEqual('expected error', 'no error occurred')     TD-29850
        except SchemalessError as errMsg:
            tdSql.checkEqual(errMsg.msg, 'Duplicated column names')

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
