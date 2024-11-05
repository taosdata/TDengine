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


class TDTestCase:
    """This test case is used to veirfy hot refresh configurations
    """

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

        self.configration_dic = {
            "cli": [
                # {
                #     "name": "keepAliveIdle",
                #     "values": [1, 100, 7200000],
                #     "except_values": [0, 7200001]
                # },
                {
                    "name": "queryPolicy",
                    "values": [1, 2, 4],
                    "except_values": [0, 5]
                },
                {
                    "name": "numOfLogLines",
                    "values": [1000, 2000, 2000000000],
                    "except_values": [999, 2000000001]
                },
                {
                    "name": "logKeepDays",
                    "values": [-365000, 2000, 365000],
                    "except_values": [-365001, 365001]
                }
            ],
            "svr": [
                # {
                #     "name": "keepAliveIdle",
                #     "alias": "tsKeepAliveIdle",
                #     "values": [1, 100, 7200000],
                #     "except_values": [0, 7200001]
                # },
                {
                    "name": "mndSdbWriteDelta",
                    "alias": "tsMndSdbWriteDelta",
                    "values": [20, 1000, 10000],
                    "except_values": [19, 10001]
                },
                {
                    "name": "enableWhiteList",
                    "alias": "tsEnableWhiteList",
                    "values": [0, 1],
                    "except_values": [-1, 2]
                },
                {
                    "name": "audit",
                    "alias": "tsEnableAudit",
                    "values": [0, 1],
                    "except_values": [-1, 2]
                },
                {
                    "name": "telemetryReporting",
                    "alias": "tsEnableTelem",
                    "values": [0, 1],
                    "except_values": [-1, 2]
                },
                {
                    "name": "cacheLazyLoadThreshold",
                    "alias": "tsCacheLazyLoadThreshold",
                    "values": [0, 200, 100000],
                    "except_values": [-1, 100001]
                },
                {
                    "name": "queryRspPolicy",
                    "alias": "tsQueryRspPolicy",
                    "values": [0, 1],
                    "except_values": [-1, 2]
                },
                {
                    "name": "ttlFlushThreshold",
                    "alias": "tsTtlFlushThreshold",
                    "values": [-1, 200, 1000000],
                    "except_values": [-2, 1000001]
                },
                {
                    "name": "timeseriesThreshold",
                    "alias": "tsTimeSeriesThreshold",
                    "values": [0, 200, 2000],
                    "except_values": [-2, 2001]
                },
                {
                    "name": "minDiskFreeSize",
                    "alias": "tsMinDiskFreeSize",
                    "values": ["51200K", "100M", "1G"],
                    "check_values": ["52428800", "104857600", "1073741824"],
                    "except_values": ["1024K", "1.1G", "1T"]
                },
                {
                    "name": "tmqMaxTopicNum",
                    "alias": "tmqMaxTopicNum",
                    "values": [1, 1000, 10000],
                    "except_values": [0, 10001]
                },
                {
                    "name": "transPullupInterval",
                    "alias": "tsTransPullupInterval",
                    "values": [1, 1000, 10000],
                    "except_values": [0, 10001]
                },
                {
                    "name": "mqRebalanceInterval",
                    "alias": "tsMqRebalanceInterval",
                    "values": [1, 1000, 10000],
                    "except_values": [0, 10001]
                },
                {
                    "name": "checkpointInterval",
                    "alias": "tsStreamCheckpointInterval",
                    "values": [60, 1000, 1800],
                    "except_values": [59, 1801]
                },
                {
                    "name": "trimVDbIntervalSec",
                    "alias": "tsTrimVDbIntervalSec",
                    "values": [1, 1000, 100000],
                    "except_values": [0, 100001]
                },
                {
                    "name": "disableStream",
                    "alias": "tsDisableStream",
                    "values": [0, 1],
                    "except_values": [-1]
                },
                {
                    "name": "maxStreamBackendCache",
                    "alias": "tsMaxStreamBackendCache",
                    "values": [16, 512, 1024],
                    "except_values": [15, 1025]
                },
                {
                    "name": "numOfLogLines",
                    "alias": "tsNumOfLogLines",
                    "values": [1000, 50000, 2000000000],
                    "except_values": [999, 2000000001]
                },
                {
                    "name": "logKeepDays",
                    "alias": "tsLogKeepDays",
                    "values": [-365000, 10, 365000],
                    "except_values": [-365001, 365001]
                },
                {
                    "name": "syncLogBufferMemoryAllowed",
                    "alias": "syncLogBufferMemoryAllowed",
                    "values": [104857600, 1048576000, 9223372036854775807],
                    "except_values": [-104857600, 104857599, 9223372036854775808]
                }
            ]
        }

    def cli_get_param_value(self, config_name):
        tdSql.query("show local variables;")
        for row in tdSql.queryResult:
            if config_name == row[0]:
                tdLog.debug("Found variable '{}'".format(row[0]))
                return row[1]

    def svr_get_param_value(self, config_name):
        tdSql.query("show dnode 1 variables;")
        for row in tdSql.queryResult:
            if config_name == row[1]:
                tdLog.debug("Found variable '{}'".format(row[1]))
                return row[2]

    def cli_check(self, item, except_values=False):
        name = item["name"]
        if except_values:
            values = item["except_values"]
        else:
            values = item["values"]
        check_values = item.get("check_values", [])
        if not except_values:
            for i in range(len(values)):
                v = values[i]
                tdLog.debug("Set {} to {}".format(name, v))
                tdSql.execute(f'alter local "{name} {v}";')
                value = self.cli_get_param_value(name)
                tdLog.debug("Get {} value: {}".format(name, value))
                if check_values:
                    tdLog.debug(f"assert {check_values[i]} == {str(value)}")
                    assert str(check_values[i]) == str(value)
                else:
                    tdLog.debug(f"assert {v} == {str(value)}")
                    assert str(v) == str(value)
        else:
            for v in values:
                tdLog.debug("Set client {} to {}".format(name, v))
                tdSql.error(f'alter local "{name} {v}";')

    def svr_check(self, item, except_values=False):
        name = item["name"]
        if except_values:
            values = item["except_values"]
        else:
            values = item["values"]
        check_values = item.get("check_values", [])
        p_list = ["dnode 1", "all dnodes"]
        # check bool param value
        if len(values) == 2 and [0, 1] == values and name != "queryRspPolicy":
            is_bool = True
        else:
            is_bool = False
        tdLog.debug(f"{name} is_bool: {is_bool}")
        if not except_values:
            for i in range(len(values)):
                v = values[i]
                dnode = random.choice(p_list)
                tdSql.execute(f'alter {dnode} "{name} {v}";')
                value = self.svr_get_param_value(name)
                tdLog.debug(f"value: {value}")
                if check_values:
                    tdLog.debug(f"assert {check_values[i]} == {str(value)}")
                    assert str(check_values[i]) == str(value)
                else:
                    tdLog.debug(f"assert {v} == {str(value)}")
                    assert str(v) == str(value)
        else:
            for v in values:
                dnode = random.choice(p_list)
                tdSql.error(f'alter {dnode} "{name} {v}";')

    def run(self):

        # reset log
        taosdLogPath = tdCom.getTaosdPath() + "/log/*"
        tdSql.execute("alter all dnodes 'resetlog';")
        time.sleep(1)
        cmd = "egrep 'reset log file' {}".format(taosdLogPath)
        tdLog.debug("run cmd: {}".format(cmd))
        r = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = r.communicate()
        tdLog.debug("stdout: {}".format(stdout.decode()))
        tdLog.debug("stderr: {}".format(stderr.decode()))
        assert ('reset log file' in stdout.decode())

        for key in self.configration_dic:
            if "cli" == key:
                for item in self.configration_dic[key]:
                    self.cli_check(item)
                    if "except_values" in item:
                        self.cli_check(item, True)
            elif "svr" == key:
                for item in self.configration_dic[key]:
                    self.svr_check(item)
                    if "except_values" in item:
                        self.svr_check(item, True)
            else:
                raise Exception(f"unknown key: {key}")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
