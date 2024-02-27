import sys
import time
import threading
from taos.tmq import Consumer
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {'debugFlag': 135, 'clientCfg':clientCfgDict}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.wal_retention_period1 = 3600
        self.wal_retention_period2 = 1
        self.commit_value_list = ["true", "false"]
        self.offset_value_list = ["earliest", "latest", "none"]
        self.tbname_value_list = ["true", "false"]
        self.snapshot_value_list = ["false"]

        # self.commit_value_list = ["true"]
        # self.offset_value_list = [""]
        # self.tbname_value_list = ["true"]
        # self.snapshot_value_list = ["false"]

    def tmqParamsTest(self):
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
                    'auto_commit_interval': "100"}


        start_group_id = 1
        for snapshot_value in self.snapshot_value_list:
            for commit_value in self.commit_value_list:
                for offset_value in self.offset_value_list:
                    for tbname_value in self.tbname_value_list:
                        topic_name = 'topic1'
                        tmqCom.initConsumerTable()
                        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=4,replica=1)
                        tdLog.info("create stb")
                        tdCom.create_stable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"], column_elm_list=paraDict['colSchema'], tag_elm_list=paraDict['tagSchema'])
                        tdLog.info("create ctb")
                        tdCom.create_ctable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"],tag_elm_list=paraDict['tagSchema'],count=paraDict["ctbNum"], default_ctbname_prefix=paraDict['ctbPrefix'])
                        tdLog.info("insert data")
                        tmqCom.insert_data(tdSql,paraDict["dbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"])


                        tdLog.info("create topics from stb with filter")
                        queryString = "select ts, log(c1), ceil(pow(c1,3)) from %s.%s where c1 %% 7 == 0" %(paraDict['dbName'], paraDict['stbName'])
                        sqlString = "create topic %s as %s" %(topic_name, queryString)
                        tdSql.query(f'select * from information_schema.ins_databases')
                        db_wal_retention_period_list = list(map(lambda x:x[-6] if x[0] == paraDict['dbName'] else None, tdSql.queryResult))
                        for i in range(len(db_wal_retention_period_list)):
                            if db_wal_retention_period_list[0] is None or db_wal_retention_period_list[-1] is None:
                                db_wal_retention_period_list.remove(None)
                        if snapshot_value =="true":
                            if db_wal_retention_period_list[0] != self.wal_retention_period2:
                                tdSql.execute(f"alter database {paraDict['dbName']} wal_retention_period {self.wal_retention_period2}")
                                time.sleep(self.wal_retention_period2+1)
                                tdSql.execute(f'flush database {paraDict["dbName"]}')
                        else:
                            if db_wal_retention_period_list[0] != self.wal_retention_period1:
                                tdSql.execute(f"alter database {paraDict['dbName']} wal_retention_period {self.wal_retention_period1}")
                        tdLog.info("create topic sql: %s"%sqlString)
                        tdSql.execute(sqlString)
                        tdSql.query(queryString)
                        expected_res = tdSql.queryRows
                        group_id = "csm_" + str(start_group_id)
                        consumer_dict = {
                            "group.id": group_id,
                            "td.connect.user": "root",
                            "td.connect.pass": "taosdata",
                            "auto.commit.interval.ms": paraDict["auto_commit_interval"],
                            "enable.auto.commit": commit_value,
                            "auto.offset.reset": offset_value,
                            "experimental.snapshot.enable": snapshot_value,
                            "msg.with.table.name": tbname_value
                        }
                        consumer_commit = 1 if consumer_dict["enable.auto.commit"] == "true" else 0
                        consumer_tbname = 1 if consumer_dict["msg.with.table.name"] == "true" else 0
                        consumer_ret = "latest" if offset_value == "" else offset_value
                        expected_parameters=f'tbname:{consumer_tbname},commit:{consumer_commit},interval:{paraDict["auto_commit_interval"]}ms,reset:{consumer_ret}'
                        if len(offset_value) == 0:
                            del consumer_dict["auto.offset.reset"]
                        consumer = Consumer(consumer_dict)
                        consumer.subscribe([topic_name])
                        tdLog.info(f"enable.auto.commit: {commit_value}, auto.offset.reset: {offset_value}, experimental.snapshot.enable: {snapshot_value}, msg.with.table.name: {tbname_value}")
                        stop_flag = 0
                        try:
                            while True:
                                res = consumer.poll(1)
                                tdSql.query('show consumers;')
                                consumer_info = tdSql.queryResult[0][-1]
                                if offset_value == "latest":
                                    if not res and stop_flag == 1:
                                        break
                                else:
                                    if not res:
                                        break
                                # err = res.error()
                                # if err is not None:
                                #     raise err
                                # val = res.value()
                                # for block in val:
                                #     print(block.fetchall())
                                if offset_value == "latest" and stop_flag == 0:
                                    tmqCom.insert_data(tdSql,paraDict["dbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],int(round(time.time()*1000)))
                                    stop_flag = 1
                        finally:
                            consumer.unsubscribe()
                            consumer.close()
                        tdSql.checkEqual(consumer_info, expected_parameters)
                        start_group_id += 1
                        tdSql.query('show subscriptions;')
                        subscription_info = tdSql.queryResult
                        tdLog.info(f"---------- subscription_info: {subscription_info}")
                        if snapshot_value == "true":
                            if offset_value != "earliest" and offset_value != "":
                                if offset_value == "latest":
                                    offset_value_list = list(map(lambda x: (x[-2].replace("wal:", "").replace("earliest", "0").replace("latest", "0").replace(offset_value, "0")), subscription_info))
                                    offset_value_list1 = list(map(lambda x: int(x.split("/")[0]), offset_value_list))
                                    offset_value_list2 = list(map(lambda x: int(x.split("/")[1]), offset_value_list))
                                    tdSql.checkEqual(offset_value_list1 == offset_value_list2, True)
                                    tdSql.checkEqual(sum(offset_value_list1) >= 0, True)
                                    rows_value_list  = list(map(lambda x: int(x[-1]), subscription_info))
                                    tdSql.checkEqual(sum(rows_value_list), expected_res)
                                elif offset_value == "none":
                                    offset_value_list = list(map(lambda x: x[-2], subscription_info))
                                    offset_value_list1 = list(map(lambda x: (x.split("/")[0]), offset_value_list))
                                    tdSql.checkEqual(offset_value_list1, ['none']*len(subscription_info))
                                    rows_value_list  = list(map(lambda x: x[-1], subscription_info))
                                    tdSql.checkEqual(rows_value_list, [0]*len(subscription_info))
                            else:
                                if offset_value != "none":
                                    offset_value_str = ",".join(list(map(lambda x: x[-2], subscription_info)))
                                    tdLog.info("checking tsdb in offset_value_str")
                                    # tdSql.checkEqual("tsdb" in offset_value_str, True)
                                    # rows_value_list  = list(map(lambda x: int(x[-1]), subscription_info))
                                    # tdSql.checkEqual(sum(rows_value_list), expected_res)
                                else:
                                    offset_value_list = list(map(lambda x: x[-2], subscription_info))
                                    offset_value_list1 = list(map(lambda x: (x.split("/")[0]), offset_value_list))
                                    tdSql.checkEqual(offset_value_list1, [None]*len(subscription_info))
                                    rows_value_list  = list(map(lambda x: x[-1], subscription_info))
                                    tdSql.checkEqual(rows_value_list, [None]*len(subscription_info))
                        else:
                            if offset_value != "none":
                                offset_value_list = list(map(lambda x: (x[-2].replace("wal:", "").replace("earliest", "0").replace("latest", "0").replace(offset_value, "0")), subscription_info))
                                offset_value_list1 = list(map(lambda x: int(x.split("/")[0]), offset_value_list))
                                offset_value_list2 = list(map(lambda x: int(x.split("/")[1]), offset_value_list))
                                tdSql.checkEqual(offset_value_list1 == offset_value_list2, True)
                                tdSql.checkEqual(sum(offset_value_list1) >= 0, True)
                                rows_value_list  = list(map(lambda x: int(x[-1]), subscription_info))
                                tdSql.checkEqual(sum(rows_value_list), expected_res)
                            else:
                                offset_value_list = list(map(lambda x: x[-2], subscription_info))
                                offset_value_list1 = list(map(lambda x: (x.split("/")[0]), offset_value_list))
                                tdSql.checkEqual(offset_value_list1, ['none']*len(subscription_info))
                                rows_value_list  = list(map(lambda x: x[-1], subscription_info))
                                tdSql.checkEqual(rows_value_list, [0]*len(subscription_info))
                        tdSql.execute(f"drop topic if exists {topic_name}")
                        tdSql.execute(f'drop database if exists {paraDict["dbName"]}')

    def run(self):
        self.tmqParamsTest()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())