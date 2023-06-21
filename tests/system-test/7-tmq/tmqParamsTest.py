
import taos
import sys
import time
import socket
import os
import threading
import math
from taos.tmq import Consumer
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.commit_value_list = ["true", "false"]
        self.reset_value_list = ["", "earliest", "latest", "none"]
        self.tbname_value_list = ["true", "false"]

        self.commit_value_list = ["true"]
        self.reset_value_list = ["earliest"]
        self.tbname_value_list = ["true"]

    def tmqParamsTest(self):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'db1',
                    'dropFlag':   1,
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbNum':     1,
                    'rowsPerTbl': 10000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'auto_commit_interval': "5000"}

        topic_name = 'topic1'
        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=4,replica=1)
        tdLog.info("create stb")
        tdCom.create_stable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"], column_elm_list=paraDict['colSchema'], tag_elm_list=paraDict['tagSchema'])
        tdLog.info("create ctb")
        tdCom.create_ctable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"],tag_elm_list=paraDict['tagSchema'],count=paraDict["ctbNum"], default_ctbname_prefix=paraDict['ctbPrefix'])
        tdLog.info("insert data")
        tmqCom.insert_data(tdSql,paraDict["dbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"])
        tdSql.execute("alter database %s wal_retention_period 3600" % (paraDict['dbName']))
        tdLog.info("create topics from stb with filter")
        queryString = "select ts, log(c1), ceil(pow(c1,3)) from %s.%s where c1 %% 7 == 0" %(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topic_name, queryString)
        start_group_id = 1
        for commit_value in self.commit_value_list:
            for reset_value in self.reset_value_list:
                for tbname_value in self.tbname_value_list:
                    tdLog.info("create topic sql: %s"%sqlString)
                    tdSql.execute(sqlString)
                    tdSql.query(queryString)
                    group_id = "csm_" + str(start_group_id)
                    consumer_dict = {
                        "group.id": group_id,
                        "td.connect.user": "root",
                        "td.connect.pass": "taosdata",
                        "auto.commit.interval.ms": paraDict["auto_commit_interval"],
                        "enable.auto.commit": commit_value,
                        "auto.offset.reset": reset_value,
                        "msg.with.table.name": tbname_value
                    }
                    consumer_commit = 1 if consumer_dict["enable.auto.commit"] == "true" else 0
                    consumer_tbname = 1 if consumer_dict["msg.with.table.name"] == "true" else 0
                    consumer_ret = "earliest" if reset_value == "" else reset_value
                    expected_parameters=f'tbname:{consumer_tbname},commit:{consumer_commit},interval:{paraDict["auto_commit_interval"]},reset:{consumer_ret}'
                    if len(reset_value) == 0:
                        del consumer_dict["auto.offset.reset"]
                    print(consumer_dict)
                    consumer = Consumer(consumer_dict)
                    consumer.subscribe([topic_name])
                    try:
                        while True:
                            res = consumer.poll(1)
                            tdSql.query('show consumers;')
                            consumer_info = tdSql.queryResult[0][-1]
                            if not res:
                                break
                            err = res.error()
                            if err is not None:
                                raise err
                            val = res.value()

                            # for block in val:
                            #     print(block.fetchall())
                    finally:
                        consumer.unsubscribe()
                        consumer.close()
                    tdSql.checkEqual(consumer_info, expected_parameters)
                    start_group_id += 1
                    tdSql.query('show subscriptions;')
                    subscription_info = tdSql.queryResult
                    offset_value_list = list(map(lambda x: int(x[-2].replace("log:", "")), subscription_info))
                    print(offset_value_list)
                    rows_value_list  = list(map(lambda x: int(x[-1]), subscription_info))
                    print(rows_value_list)
                    tdSql.execute(f"drop topic if exists {topic_name}")
        return

    def run(self):
        self.tmqParamsTest()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
