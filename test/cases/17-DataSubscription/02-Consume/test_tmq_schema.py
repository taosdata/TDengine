
# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import taos
import sys
import time
import socket
import os
import threading
from enum import Enum
from taos.tmq import Consumer

from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom

class actionType(Enum):
    CREATE_DATABASE = 0
    CREATE_STABLE   = 1
    CREATE_CTABLE   = 2
    INSERT_DATA     = 3

class TestCase:
    hostname = socket.gethostname()
    #rpcDebugFlagVal = '143'
    #clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    #clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal
    #updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    #updatecfgDict["rpcDebugFlag"] = rpcDebugFlagVal
    #print ("===================: ", updatecfgDict)

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")


    def newcur(self,cfg,host,port):
        user = "root"
        password = "taosdata"
        con=taos.connect(host=host, user=user, password=password, config=cfg ,port=port)
        cur=con.cursor()
        print(cur)
        return cur

    def create_database(self,tsql, dbName,dropFlag=1,vgroups=4,replica=1):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d wal_retention_period 3600"%(dbName, vgroups, replica))
        tdLog.debug("complete to create database %s"%(dbName))
        return

    def create_stable(self,tsql, dbName,stbName):
        tsql.execute("create table if not exists %s.%s (ts timestamp, c1 bigint, c2 binary(16)) tags(t1 int)"%(dbName, stbName))
        tdLog.debug("complete to create %s.%s" %(dbName, stbName))
        return

    def create_ctables(self,tsql, dbName,stbName,ctbPrefix,ctbNum):
        tsql.execute("use %s" %dbName)
        pre_create = "create table"
        sql = pre_create
        #tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
        for i in range(ctbNum):
            sql += " %s_%d using %s tags(%d)"%(ctbPrefix,i,stbName,i+1)
            if (i > 0) and (i%100 == 0):
                tsql.execute(sql)
                sql = pre_create
        if sql != pre_create:
            tsql.execute(sql)

        tdLog.debug("complete to create %d child tables in %s.%s" %(ctbNum, dbName, stbName))
        return

    def insert_data_interlaceByMultiTbl(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        ctbDict = {}
        for i in range(ctbNum):
            ctbDict[i] = 0

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsOfCtb = 0
        while rowsOfCtb < rowsPerTbl:
            for i in range(ctbNum):
                sql += " %s.%s_%d values "%(dbName,ctbPrefix,i)
                for k in range(batchNum):
                    sql += "(%d, %d, 'tmqrow_%d') "%(startTs + ctbDict[i], ctbDict[i], ctbDict[i])
                    ctbDict[i] += 1
                    if (0 == ctbDict[i]%batchNum) or (ctbDict[i] == rowsPerTbl):
                        tsql.execute(sql)
                        sql = "insert into "
                        break
            rowsOfCtb = ctbDict[0]

        tdLog.debug("insert data ............ [OK]")
        return

    def insert_data(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsOfSql = 0
        for i in range(ctbNum):
            sql += " %s_%d values "%(ctbPrefix,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'tmqrow_%d') "%(startTs + j, j, j)
                rowsOfSql += 1
                if (j > 0) and ((rowsOfSql == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsOfSql = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s_%d values " %(ctbPrefix,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def insert_data_with_autoCreateTbl(self,tsql,dbName,stbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0):
        tdLog.debug("start to insert data wiht auto create child table ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsOfSql = 0
        for i in range(ctbNum):
            sql += " %s.%s_%d using %s.%s tags (%d) values "%(dbName,ctbPrefix,i,dbName,stbName,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'autodata_%d') "%(startTs + j, j, j)
                rowsOfSql += 1
                if (j > 0) and ((rowsOfSql == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsOfSql = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s_%d using %s.%s tags (%d) values " %(dbName,ctbPrefix,i,dbName,stbName,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def consume_topic_rows(self, consumer, expected_rows, timeout_seconds=20):
        rows = []
        deadline = time.time() + timeout_seconds
        while len(rows) < expected_rows and time.time() < deadline:
            result = consumer.poll(1)
            if not result:
                continue
            error = result.error()
            if error is not None:
                tdLog.exit(f"TMQ poll failed: {error}")
            value = result.value()
            if value is None:
                continue
            for block in value:
                rows.extend(block.fetchall())
        if len(rows) != expected_rows:
            tdLog.exit(f"Expected {expected_rows} TMQ rows, got {len(rows)}")
        return rows

    def do_query_topic_column_expansion(self):
        database = "db_query_topic_expand"
        table = "ntb_query_topic_expand"
        topic = "topic_query_topic_expand"
        initial_value = "a" * 32
        expanded_value = "b" * 40

        consumer = None
        try:
            tdSql.execute(f"drop database if exists {database}")
            tdSql.execute(f"create database {database} vgroups 1 wal_retention_period 3600")
            tdSql.execute(f"create table {database}.{table} (ts timestamp, value binary(32))")
            tdSql.execute(f"create topic {topic} as select ts, value from {database}.{table}")

            consumer = Consumer({
                "group.id": "query_topic_expand_group",
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "enable.auto.commit": "false",
                "auto.offset.reset": "earliest",
            })
            consumer.subscribe([topic])
            tdLog.debug("Inserting initial query topic row with current timestamp")
            tdSql.execute(f"insert into {database}.{table} values(now, '{initial_value}')")
            initial_rows = self.consume_topic_rows(consumer, 1)
            if len(initial_rows[0][1]) != len(initial_value):
                tdLog.exit(f"Expected initial value length {len(initial_value)}, got {len(initial_rows[0][1])}")

            tdSql.execute(f"alter table {database}.{table} modify column value binary(40)")
            tdLog.debug("Inserting expanded query topic row one second after the initial row")
            tdSql.execute(f"insert into {database}.{table} values(now+1s, '{expanded_value}')")
            expanded_rows = self.consume_topic_rows(consumer, 1)
            if len(expanded_rows[0][1]) != len(expanded_value):
                tdLog.exit(f"Expected expanded value length {len(expanded_value)}, got {len(expanded_rows[0][1])}")
        finally:
            # consume_topic_rows exits via SystemExit on failure, so the topic and the database are
            # dropped here as well: they must be removed even when the consumer cannot be created
            # or closed, otherwise they leak into the shared test instance.
            try:
                if consumer is not None:
                    consumer.close()
            finally:
                tdSql.execute(f"drop topic if exists {topic}")
                tdSql.execute(f"drop database if exists {database}")

        print("query topic column expansion ........ [passed]")

    def prepareEnv(self, **parameterDict):
        # create new connector for my thread
        tsql=self.newcur(parameterDict['cfg'], 'localhost', 6030)

        if parameterDict["actionType"] == actionType.CREATE_DATABASE:
            self.create_database(tsql, parameterDict["dbName"])
        elif parameterDict["actionType"] == actionType.CREATE_STABLE:
            self.create_stable(tsql, parameterDict["dbName"], parameterDict["stbName"])
        elif parameterDict["actionType"] == actionType.CREATE_CTABLE:
            self.create_ctables(tsql, parameterDict["dbName"], parameterDict["stbName"], parameterDict["stbName"], parameterDict["ctbNum"])
        elif parameterDict["actionType"] == actionType.INSERT_DATA:
            self.insert_data(tsql, parameterDict["dbName"], parameterDict["stbName"], parameterDict["ctbNum"],\
                            parameterDict["rowsPerTbl"],parameterDict["batchNum"])
        else:
            tdLog.exit("not support's action: ", parameterDict["actionType"])

        return

    def tmqCase1(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 1: ")
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db1',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb1',   \
                         'ctbPrefix':  'stb1',   \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   23,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tdLog.info("create database, super table, child table, normal table")
        ntbName = 'ntb1'
        self.create_database(tdSql, parameterDict["dbName"])
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10)) tags (t1 int, t2 binary(32), t3  double, t4 binary(32), t5 nchar(10))"%(parameterDict["dbName"],parameterDict["stbName"]))
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10))"%(parameterDict["dbName"],ntbName))

        tdLog.info("create topics from super table and normal table")
        columnTopicFromStb = 'column_topic_from_stb1'
        columnTopicFromNtb = 'column_topic_from_ntb1'

        tdSql.execute("create topic %s as select ts, c1, c2, t1, t2 from %s.%s" %(columnTopicFromStb, parameterDict['dbName'], parameterDict['stbName']))
        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s" %(columnTopicFromNtb, parameterDict['dbName'], ntbName))

        tdLog.info("======== super table test:")
        # alter actions prohibited: drop column/tag, modify column/tag type, rename column/tag included in topic
        tdSql.execute("alter table %s.%s drop column c1"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.execute("alter table %s.%s drop tag t1"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.execute("alter table %s.%s modify column c2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.execute("alter table %s.%s modify tag t2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))

        # Renaming a column that IS part of the topic is allowed: the topic binds by colId, which the
        # rename preserves. This matches both the tag rename on the next line and the normal-table
        # path further down, which has always allowed the same statement.
        tdSql.execute("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.execute("alter table %s.%s rename tag t2 t2new"%(parameterDict['dbName'], parameterDict['stbName']))

        # alter actions allowed: drop column/tag, modify column/tag type, rename column/tag not included in topic
        tdSql.query("alter table %s.%s modify column c4 binary(60)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t4 binary(60)"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s rename tag t3 t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t4 t4new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s drop column c3"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c4"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t4new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s add column c3 int"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add column c4 float"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t3 int"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t4 float"%(parameterDict['dbName'], parameterDict['stbName']))

        tdLog.info("======== normal table test:")
        # alter actions prohibited: drop column/tag, modify column/tag type, rename column/tag included in topic
        tdSql.execute("alter table %s.%s drop column c1"%(parameterDict['dbName'], ntbName))

        tdSql.execute("alter table %s.%s modify column c2 binary(60)"%(parameterDict['dbName'], ntbName))

        tdSql.execute("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], ntbName))

        # alter actions allowed: drop column/tag, modify column/tag type, rename column/tag not included in topic
        tdSql.query("alter table %s.%s modify column c4 binary(60)"%(parameterDict['dbName'], ntbName))

        tdSql.query("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], ntbName))
        tdSql.query("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], ntbName))

        tdSql.query("alter table %s.%s drop column c3new"%(parameterDict['dbName'], ntbName))
        tdSql.query("alter table %s.%s drop column c4new"%(parameterDict['dbName'], ntbName))

        tdSql.query("alter table %s.%s add column c3 int"%(parameterDict['dbName'], ntbName))
        tdSql.query("alter table %s.%s add column c4 float"%(parameterDict['dbName'], ntbName))

        tdLog.info("======== child table test:")
        parameterDict['stbName'] = 'stb12'
        ctbName = 'stb12_0'
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10)) tags (t1 int, t2 binary(32), t3  double, t4 binary(32), t5 nchar(10))"%(parameterDict["dbName"],parameterDict['stbName']))
        tdSql.query("create table %s.%s using %s.%s tags (1, '2', 3, '4', '5')"%(parameterDict["dbName"],ctbName,parameterDict["dbName"],parameterDict['stbName']))

        tdLog.info("create topics from child table")
        columnTopicFromCtb = 'column_topic_from_ctb1'

        tdSql.execute("create topic %s as select ts, c1, c2, t1, t2 from %s.%s" %(columnTopicFromCtb,parameterDict['dbName'],ctbName))

        # alter actions prohibited: drop column/tag, modify column/tag type, rename column/tag included in topic
        tdSql.query("alter table %s.%s set tag t1=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t2='20'"%(parameterDict['dbName'], ctbName))

        tdSql.query("alter table %s.%s set tag t3=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t4='20'"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t5='20'"%(parameterDict['dbName'], ctbName))

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def tmqCase4(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 4: ")
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db4',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb4',   \
                         'ctbPrefix':  'stb4',   \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   23,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        ctbName = 'stb4_0'

        tdLog.info("create database, super table, child table, normal table")
        self.create_database(tdSql, parameterDict["dbName"])
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10)) tags (t1 int, t2 binary(32), t3  double, t4 binary(32), t5 nchar(10))"%(parameterDict["dbName"],parameterDict["stbName"]))
        tdSql.query("create table %s.%s using %s.%s tags (10, '10', 10, '10', '10')"%(parameterDict["dbName"],ctbName,parameterDict["dbName"],parameterDict['stbName']))

        tdLog.info("create topics from super table")
        columnTopicFromStb = 'star_topic_from_stb4'

        tdSql.execute("create topic %s as stable %s.%s" %(columnTopicFromStb, parameterDict['dbName'], parameterDict['stbName']))

        tdLog.info("======== child table test:")
        tdSql.query("alter table %s.%s set tag t1=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t2='20'"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t3=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t4='20'"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t5='20'"%(parameterDict['dbName'], ctbName))

        tdLog.info("======== super table test:")
        # all alter actions allow
        tdSql.query("alter table %s.%s add column c6 int"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t6 float"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s modify column c2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify column c4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify column c5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))

        # RENAME COLUMN on a super table is supported since 3.4.3.0, and a topic binds its columns
        # by colId (which a rename preserves), so these are allowed just like the tag renames below.
        tdSql.query("alter table %s.%s rename column c1 c1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename column c5 c5new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t1 t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t2 t2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t3 t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t4 t4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t5 t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        # the columns were renamed just above, so drop them under their new names
        tdSql.query("alter table %s.%s drop column c1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c5new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdLog.printNoPrefix("======== test case 4 end ...... ")

    def tmqCase5(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 5: ")
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db5',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb5',   \
                         'ctbPrefix':  'stb5',   \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   23,       \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        ctbName = 'stb5_0'

        tdLog.info("create database, super table, child table, normal table")
        self.create_database(tdSql, parameterDict["dbName"])
        tdSql.query("create table %s.%s (ts timestamp, c1 int, c2 binary(32), c3 double, c4 binary(32), c5 nchar(10)) tags (t1 int, t2 binary(32), t3  double, t4 binary(32), t5 nchar(10))"%(parameterDict["dbName"],parameterDict["stbName"]))
        tdSql.query("create table %s.%s using %s.%s tags (10, '10', 10, '10', '10')"%(parameterDict["dbName"],ctbName,parameterDict["dbName"],parameterDict['stbName']))

        tdLog.info("create topics from super table")
        columnTopicFromStb = 'star_topic_from_db5'

        tdSql.execute("create topic %s as database %s" %(columnTopicFromStb, parameterDict['dbName']))

        tdLog.info("======== child table test:")
        tdSql.query("alter table %s.%s set tag t1=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t2='20'"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t3=20"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t4='20'"%(parameterDict['dbName'], ctbName))
        tdSql.query("alter table %s.%s set tag t5='20'"%(parameterDict['dbName'], ctbName))

        tdLog.info("======== super table test:")
        # all alter actions allow
        tdSql.query("alter table %s.%s add column c6 int"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s add tag t6 float"%(parameterDict['dbName'], parameterDict['stbName']))

        tdSql.query("alter table %s.%s modify column c2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify column c4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify column c5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t2 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t4 binary(40)"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s modify tag t5 nchar(40)"%(parameterDict['dbName'], parameterDict['stbName']))

        # RENAME COLUMN on a super table is supported since 3.4.3.0, and a topic binds its columns
        # by colId (which a rename preserves), so these are allowed just like the tag renames below.
        tdSql.query("alter table %s.%s rename column c1 c1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename column c2 c2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename column c3 c3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename column c4 c4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename column c5 c5new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t1 t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t2 t2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t3 t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t4 t4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s rename tag t5 t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        # the columns were renamed just above, so drop them under their new names
        tdSql.query("alter table %s.%s drop column c1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop column c5new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t1new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t2new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t3new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t4new"%(parameterDict['dbName'], parameterDict['stbName']))
        tdSql.query("alter table %s.%s drop tag t5new"%(parameterDict['dbName'], parameterDict['stbName']))

        tdLog.printNoPrefix("======== test case 5 end ...... ")

    def test_schema(self):
        """Advanced: schema changes
        
        1. Create tables and start consumption
        2. Add column during consumption
        3. Drop column during consumption
        4. Modify column type
        5. Consume values written after a queried column is expanded
        6. Verify schema evolution handled
        7. Check backward compatibility
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_schema.py
            - 2026-09-20 Mingming Wang Added query topic column expansion regression coverage.

        """
        tdSql.prepare()

        buildPath = tdCom.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        cfgPath = os.path.join(tdDnodes.sim.path,"psim","cfg")
        tdLog.info("cfgPath: %s" % cfgPath)

        self.do_query_topic_column_expansion()
        self.tmqCase1(cfgPath, buildPath)
        self.tmqCase4(cfgPath, buildPath)
        self.tmqCase5(cfgPath, buildPath)



event = threading.Event()
