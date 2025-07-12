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

import sys
import time

import taos
import frame
import frame.etool

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame.epath import *
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
                {
                    "name": "asyncLog",
                    "value": 0,
                    "category": "local",
                },
                {
                    "name": "enableQueryHb",
                    "value": 0,
                    "category": "local",
                },
                {
                    "name": "keepColumnName",
                    "value": 1,
                    "category": "local",
                },
                {
                    "name": "logKeepDays",
                    "value": 30,
                    "category": "local",
                },
                {
                    "name": "maxInsertBatchRows",
                    "value": 2000000,
                    "category": "local",
                },
                {
                    "name": "minSlidingTime",
                    "value": 10000,
                    "category": "local",
                },
                {
                    "name": "minIntervalTime",
                    "value": 2,
                    "category": "local",
                },
                {
                    "name": "numOfLogLines",
                    "value": 20000000,
                    "category": "local",

                },
                {
                    "name": "querySmaOptimize",
                    "value": 1,
                    "category": "local",
                },
                {
                    "name": "queryPolicy",
                    "value": 3,
                    "category": "local",
                },
                {
                    "name": "queryTableNotExistAsEmpty",
                    "value": 1,
                    "category": "local",
                },
                {
                    "name": "queryPlannerTrace",
                    "value": 1,
                    "category": "local",
                },
                {
                    "name": "queryNodeChunkSize",
                    "value": 16 * 1024,
                    "category": "local",
                },
                {
                    "name": "queryUseNodeAllocator",
                    "value": 0,
                    "category": "local",
                },
                {
                    "name": "smlDot2Underline",
                    "value": 0,
                    "category": "local",
                },
                {
                    "name": "useAdapter",
                    "value": 1,
                    "category": "local",
                },
                # {
                #     "name": "multiResultFunctionStarReturnTags",
                #     "value": 1,
                #     "category": "local",
                # },
                {
                    "name": "maxTsmaCalcDelay",
                    "value": 1200,
                    "category": "local",
                },
                {
                    "name": "tsmaDataDeleteMark",
                    "value": 1000 * 60 * 60 * 12,
                    "category": "local",
                },
                {
                    "name": "bypassFlag",
                    "value": 4,
                    "category": "local",
                }
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
                    "name": "streamBufferSize",
                    "value": 1024,
                    "category": "local"
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
                # {
                #     "name": "s3Accesskey",
                #     "value": 1000,
                #     "category": "global"
                # },
                # {
                #     "name": "s3BucketName",
                #     "value": 1000,
                #     "category": "global"
                # },
                # {
                #     "name": "s3Endpoint",
                #     "value": 1000,
                #     "category": "global"
                # },
                {
                    "name": "s3MigrateIntervalSec",
                    "value": 1800,
                    "category": "global"
                },
                {
                    "name": "s3MigrateEnabled",
                    "value": 1,
                    "category": "global"
                },
                # {
                #     "name": "s3BlockCacheSize",
                #     "value": 32,
                #     "category": "global"
                # },
                {
                    "name": "s3PageCacheSize",
                    "value": 8192,
                    "category": "global"
                },
                {
                    "name": "s3UploadDelaySec",
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
        res = tdSql.getResult("show local variables")
        for row in res:
            if config_name == row[0]:
                return row[1]

    def svr_get_param_value(self, config_name):
        res = tdSql.getResult("show dnode 1 variables")
        for row in res:
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
            
        tdLog.info("success to alter all configurations")

        tdLog.info("stop and restart taosd")

        sc.dnodeStopAll()
        sc.dnodeStart(1)
        sc.dnodeStart(2)
        sc.dnodeStart(3)
        sleep(10)

        for key in self.configration_dic:
            if "cli" == key:
                for item in self.configration_dic[key]:
                    actVal = self.cli_get_param_value(item["name"])
                    assert str(actVal) == str(item["value"]), f"item name: {item['name']}, Expected value: {item['value']}, actual value: {actVal}"
            elif "svr" == key:
                for item in self.configration_dic[key]:
                    actVal = self.svr_get_param_value(item["name"])
                    assert str(actVal) == str(item["value"]), f"item name: {item['name']}, Expected value: {item['value']}, actual value: {actVal}"
            else:
                raise Exception(f"unknown key: {key}")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
