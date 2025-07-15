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

import subprocess
import random
import time
import os
import platform
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from frame.srvCtl import *
from frame import *

class TDTestCase:
    """This test case is used to veirfy hot refresh configurations
    """

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

        self.configration_dic = {
            "cli": [
            ],
            "svr": [
                {
                    "name": "audit",
                    "value": 0,
                    "category": "global"
                },
                {
                    "name": "asyncLog",
                    "value": 0,
                    "category": "local"
                },
                {
                    "name": "disableStream",
                    "value": 1,
                    "category": "global"
                },
                {
                    "name": "enableWhiteList",
                    "value": 1,
                    "category": "global"
                },
                {
                    "name": "statusInterval",
                    "value": 3,
                    "category": "global"
                },
                {
                    "name": "telemetryReporting",
                    "value" : 1,
                    "category" : "global"
                },
                {
                    "name" : "monitor",
                    "value" : 0,
                    "category" : "global"
                },
                {
                    "name" : "monitorInterval",
                    "value" : 3,
                    "category" : "global"
                },
                {
                    "name" : "monitorComp",
                    "value" : 1,
                    "category" : "global"
                },
                {
                    "name" : "monitorForceV2",
                    "value" : 0,
                    "category" : "global"
                },
                {
                    "name" : "monitorLogProtocol",
                    "value" : 1,
                    "category" : "global"
                },
                {
                    "name": "monitorMaxLogs",
                    "value": 1000,
                    "category": "global"
                },
                {
                    "name": "auditCreateTable",
                    "value": 0,
                    "category": "global"
                },
                {
                    "name": "auditInterval",
                    "value": 4000,
                    "category": "global"
                },
                {
                    "name": "slowLogThreshold",
                    "value": 20,
                    "category": "global"
                },
                {
                    "name": "compressMsgSize",
                    "value": 0,
                    "category": "global"
                },
                {
                    "name": "compressor",
                    "value": "GZIP_COMPRESSOR",
                    "category": "global"
                },
                {
                    "name": "curRange",
                    "value": 200,
                    "category": "global"
                },
                {
                    "name": "fPrecision",
                    "value": "1000.000000",
                    "category": "global"
                },
                {
                    "name": "dPrecision",
                    "value": "1000.000000",
                    "category": "global"
                },
                {
                    "name": "ifAdtFse",
                    "value": 1,
                    "category": "global"
                },
                {
                    "name": "maxRange",
                    "value": 1000,
                    "category": "global"
                },
                {
                    "name": "maxTsmaNum",
                    "value": 2,
                    "category": "global"
                },
                {
                    "name": "queryRsmaTolerance",
                    "value": 2000,
                    "category": "global"
                },
                {
                    "name": "uptimeInterval",
                    "value": 600,
                    "category": "global"
                },
                {
                    "name": "slowLogMaxLen",
                    "value": 8192,
                    "category": "global"
                },
                {
                    "name": "slowLogScope",
                    "value": "insert",
                    "category": "global"
                },
                {
                    "name": "slowLogExceptDb",
                    "value": "db1",
                    "category": "global"
                },
                {
                    "name": "mndSdbWriteDelta",
                    "value": 1000,
                    "category": "global"
                },
                {
                    "name": "minDiskFreeSize",
                    "value": 100*1024*1024,
                    "category": "global"
                },
                {
                    "name": "randErrorChance",
                    "value": 5,
                    "category": "global"
                },
                {
                    "name": "randErrorDivisor",
                    "value": 20001,
                    "category": "global"
                },
                {
                    "name": "randErrorScope",
                    "value": 8,
                    "category": "global"
                },
                {
                    "name": "syncLogBufferMemoryAllowed",
                    "value": 1024 * 1024 * 20 * 10,
                    "category": "global"
                },
                {
                    "name": "syncElectInterval",
                    "value": 50000,
                    "category": "global"
                },
                {
                    "name": "syncHeartbeatInterval",
                    "value": 3000,
                    "category": "global"
                },
                {
                    "name": "syncHeartbeatTimeout",
                    "value": 40000,
                    "category": "global"
                },
                {
                    "name": "syncSnapReplMaxWaitN",
                    "value": 32,
                    "category": "global"
                },
                {
                    "name": "walFsyncDataSizeLimit",
                    "value": 200 * 1024 * 1024,
                    "category": "global"
                },
                {
                    "name": "numOfCores",
                    "value": "30.000000",
                    "category": "global"
                },
                {
                    "name": "enableCoreFile",
                    "value": 0,
                    "category": "global"
                },
                {
                    "name": "telemetryInterval",
                    "value": 6000,
                    "category": "global"
                },
                {
                    "name": "cacheLazyLoadThreshold",
                    "value": 1000,
                    "category": "global"
                },
                {
                    "name": "checkpointInterval",
                    "value": 120,
                    "category": "global"
                },
                {
                    "name": "concurrentCheckpoint",
                    "value": 3,
                    "category": "global"
                },
                {
                    "name": "retentionSpeedLimitMB",
                    "value": 24,
                    "category": "global"
                },
                {
                    "name": "ttlChangeOnWrite",
                    "value": 1,
                    "category": "global"
                },
                {
                    "name": "logKeepDays",
                    "value": 30,
                    "category": "local"
                },
                {
                    "name": "maxStreamBackendCache",
                    "value": 256,
                    "category": "global"
                },
                {
                    "name": "mqRebalanceInterval",
                    "value": 30,
                    "category": "global"
                },
                {
                    "name": "numOfLogLines",
                    "value": 20000000,
                    "category": "local"
                },
                {
                    "name": "queryRspPolicy",
                    "value": 1,
                    "category": "global"
                },
                {
                    "name": "timeseriesThreshold",
                    "value": 100,
                    "category": "global"
                },
                {
                    "name": "tmqMaxTopicNum",
                    "value": 30,
                    "category": "global"
                },
                {
                    "name": "tmqRowSize",
                    "value": 8192,
                    "category": "global"
                },
                {
                    "name": "transPullupInterval",
                    "value": 4,
                    "category": "global"
                },
                {
                    "name": "compactPullupInterval",
                    "value": 20,
                    "category": "global"
                },
                {
                    "name": "trimVDbIntervalSec",
                    "value": 7200,
                    "category": "global"
                },
                {
                    "name": "ttlBatchDropNum",
                    "value": 20000,
                    "category": "global"
                },
                {
                    "name": "ttlFlushThreshold",
                    "value": 200,
                    "category": "global"
                },
                {
                    "name": "ttlPushInterval",
                    "value": 20,
                    "category": "global"
                },
                {
                    "name": "ttlUnit",
                    "value": 86500,
                    "category": "global"
                },
                {
                    "name": "udf",
                    "value": 0,
                    "category": "global"
                },
                # {
                #     "name": "udfdLdLibPath",
                #     "value": 1000,
                #     "category": "global"
                # },
                # {
                #     "name": "udfdResFuncs",
                #     "value": 1000,
                #     "category": "global"
                # },
                {
                    "name": "ssAutoMigrateIntervalSec",
                    "value": 1800,
                    "category": "global"
                },
                # {
                #     "name": "ssBlockCacheSize",
                #     "value": 32,
                #     "category": "global"
                # },
                {
                    "name": "ssPageCacheSize",
                    "value": 8192,
                    "category": "global"
                },
                {
                    "name": "ssUploadDelaySec",
                    "value": 30,
                    "category": "global"
                },
                {
                    "name": "mndLogRetention",
                    "value": 1000,
                    "category": "global"
                },
                {
                    "name": "supportVnodes",
                    "value": 128,
                    "category": "global"
                },
                {
                    "name": "experimental",
                    "value": 0,
                    "category": "global"
                },
                {
                    "name": "maxTsmaNum",
                    "value": 2,
                    "category": "global"
                },
                {
                    "name": "maxShellConns",
                    "value": 25000,
                    "category": "global"
                },
                {
                    "name": "numOfRpcSessions",
                    "value": 15000,
                    "category": "global"
                },
                {
                    "name": "numOfRpcThreads",
                    "value": 2,
                    "category": "global"
                },
                {
                    "name": "rpcQueueMemoryAllowed",
                    "value": 1024*1024*20*10,
                    "category": "global"
                },
                {
                    "name": "shellActivityTimer",
                    "value": 2,
                    "category": "global"
                },
                {
                    "name": "timeToGetAvailableConn",
                    "value": 200000,
                    "category": "global"
                },
                {
                    "name": "readTimeout",
                    "value": 800,
                    "category": "global"
                }, 
                {
                    "name": "safetyCheckLevel",
                    "value": 2,
                    "category": "global"
                },
                {
                    "name": "bypassFlag",
                    "value": 4,
                    "category": "global"
                }
            ]
        }
    def cli_get_param_value(self, config_name):
        tdSql.query("show local variables;")
        for row in tdSql.queryResult:
            if config_name == row[0]:
                return row[1]

    def svr_get_param_value(self, config_name):
        tdSql.query("show dnode 1 variables;")
        for row in tdSql.queryResult:
            if config_name == row[1]:
                return row[2]
            
    def configuration_alter(self):
        for key in self.configration_dic:
            if "svr" == key:
                for item in self.configration_dic[key]:
                    name = item["name"]
                    value = item["value"]
                    category = item["category"]
                    if category == "global":
                        tdSql.execute(f'alter all dnodes "{name} {value}";')
                    else:
                        tdSql.execute(f'alter dnode 1 "{name} {value}";')
            elif "cli" == key:
                for item in self.configration_dic[key]:
                    name = item["name"]
                    value = item["value"]
                    category = item["category"]
                    if category == "global":
                        tdSql.execute(f'alter all dnodes "{name} {value}";')
                    else:
                        tdSql.execute(f'alter local "{name} {value}";')
            else:
                raise Exception(f"unknown key: {key}")

    def run(self):
        self.configuration_alter()
        for key in self.configration_dic:
            if "cli" == key:
                for item in self.configration_dic[key]:
                    actVal = self.cli_get_param_value(item["name"])
                    assert str(actVal) == str(item["value"]), f"tem name: {item['name']}, Expected value: {item['value']}, actual value: {actVal}"
            elif "svr" == key:
                for item in self.configration_dic[key]:
                    actVal = self.svr_get_param_value(item["name"])
                    assert str(actVal) == str(item["value"]), f"tem name: {item['name']}, Expected value: {item['value']}, actual value: {actVal}"
            else:
                raise Exception(f"unknown key: {key}")

        tdLog.info("stop and restart taosd")
        tdDnodes.stop(1)
        tdDnodes.start(1)

        for key in self.configration_dic:
            if "cli" == key:
                for item in self.configration_dic[key]:
                    actVal = self.cli_get_param_value(item["name"])
                    assert str(actVal) == str(item["oldVal"]), f"item name: {item['name']}, Expected value: {item['value']}, actual value: {actVal}"
            elif "svr" == key:
                for item in self.configration_dic[key]:
                    actVal = self.svr_get_param_value(item["name"])
                    assert str(actVal) == str(item["value"]), f"item name: {item['name']}, Expected value: {item['value']}, actual value: {actVal}"
            else:
                raise Exception(f"unknown key: {key}")

    def stop(self):
        tdSql.close()
        # tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
