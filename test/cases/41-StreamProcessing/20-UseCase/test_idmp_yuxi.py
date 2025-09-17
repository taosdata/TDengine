import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, etool
from datetime import datetime
from datetime import date


class Test_IDMP_Meters:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_usecase_em(self):
        """IDMP: YuXi scenario

        Refer: https://taosdata.feishu.cn/wiki/G8mSwPK20iLpPrk9MmOc9g95nLe

        Catalog:
            - Streams:UseCases

        Since: v3.3.7.6

        Labels: common,ci

        Jira: TS-7152

        History:
            - 2025-8-28 MINGMING WANG Created

        """

        #
        #  main test
        #

        # env
        tdStream.createSnode()

        # create vtables
        self.createVtables()

        # create streams
        self.createStreams()

        # check stream status
        self.checkStreamStatus()

        # insert trigger data
        self.writeTriggerData()

        # verify results
        self.verifyResults()


    #
    # ---------------------   main flow frame    ----------------------
    #

    # 
    # 1. create vtables
    #
    def createVtables(self):
        sqls = [
            "create database test;",
            "use test;",
            "CREATE STABLE `electronic_belt_scale` (`ts` TIMESTAMP, `fault_alarm` BOOL, `control_setpoint` FLOAT, `equipment_parameters` VARCHAR(512), `equipment_status` BOOL, `production_information` VARCHAR(512), `measured_value` FLOAT, `weight_ignal` FLOAT, `belt_speed` FLOAT) TAGS (`tobacco_asset` NCHAR(256), `process_stage` NCHAR(32), `process_step` NCHAR(32), `equipment_type` NCHAR(32)) SMA(`ts`,`fault_alarm`,`control_setpoint`,`equipment_parameters`,`equipment_status`,`production_information`,`measured_value`);",
            '''CREATE TABLE `f1w1a_belt_scale_07` USING `electronic_belt_scale` (`tobacco_asset`, `process_stage`, `process_step`, `equipment_type`) TAGS ("卷烟一厂.制丝车间.A线.烘丝段.叶丝回潮.电子皮带秤", "烘丝段", "叶丝回潮", "电子皮带秤");''',
            "CREATE STABLE `vst_电子皮带秤_636756` (`ts` TIMESTAMP, `故障报警` BOOL, `控制设定值` FLOAT, `设备参数` VARCHAR(512), `设备状态` BOOL, `生产信息` VARCHAR(512), `测量值` FLOAT, `皮带速度` FLOAT, `重量信号` FLOAT, `皮带速度_FORECASTS_Low` DOUBLE, `皮带速度_FORECASTS_Mean` DOUBLE, `皮带速度_FORECASTS_High` DOUBLE) TAGS (`element` VARCHAR(256), `卷烟制丝场景` NCHAR(256), `工艺段` NCHAR(32), `工序` NCHAR(32), `设备类型` NCHAR(32), `path1` VARCHAR(512)) SMA(`ts`,`故障报警`) VIRTUAL 1;",
            '''CREATE VTABLE `vt_电子皮带秤_07_564885` (
                `故障报警` FROM `f1w1a_belt_scale_07`.`fault_alarm`,
                `控制设定值` FROM `f1w1a_belt_scale_07`.`control_setpoint`,
                `设备参数` FROM `f1w1a_belt_scale_07`.`equipment_parameters`,
                `设备状态` FROM `f1w1a_belt_scale_07`.`equipment_status`,
                `生产信息` FROM `f1w1a_belt_scale_07`.`production_information`,
                `测量值` FROM `f1w1a_belt_scale_07`.`measured_value`,
                `皮带速度` FROM `f1w1a_belt_scale_07`.`belt_speed`,
                `重量信号` FROM `f1w1a_belt_scale_07`.`weight_ignal`)
            USING `vst_电子皮带秤_636756` (`element`, `卷烟制丝场景`, `工艺段`, `工序`, `设备类型`, `path1`)
            TAGS ("电子皮带秤_07", "卷烟一厂.制丝车间.A线.烘丝段.叶丝回潮.电子皮带秤", "烘丝段", "叶丝回潮", "电子皮带秤", "卷烟制丝场景.卷烟一厂.制丝车间.A线.烘丝段.叶丝回潮.电子皮带秤");'''
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create vtable successfully.")
        

    # 
    # 2. create streams
    #
    def createStreams(self):

        sqls = [
            "create stream if not exists `ana_1914678932738560_1`  event_window( start with `皮带速度`> 0.4  end with `皮带速度`< 0.4 ) true_for(1m) from `vt_电子皮带秤_07_564885` stream_options(ignore_disorder)  notify('ws://idmp:6042/eventReceive') on(window_open|window_close) into `ana_vt_电子皮带秤_07_564885_anaEnd_ana_1914678932738560_1` as select _twstart+0s as output_timestamp, max(`皮带速度`) as `最大皮带速度`  from `vt_电子皮带秤_07_564885`;",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} streams successfully.")

    # 
    # 3. wait stream ready
    #
    def checkStreamStatus(self):
        print("wait stream ready ...")
        tdStream.checkStreamStatus()
        tdLog.info(f"check stream status successfully.")

    # 
    # 4. write trigger data
    #
    def writeTriggerData(self):
        sqls = [
            "insert into f1w1a_belt_scale_07 values ('2025-08-01 00:00:00.000', false, 1200, NULL, true, 'BATCH-202504-0128-A', 1200.66, 1.02178, 0.73044);",
            "insert into f1w1a_belt_scale_07 values ('2025-08-27 00:04:12.878', NULL, NULL, NULL, NULL, NULL, NULL, 0.999213, 0.344908);"
        ]

        tdSql.executes(sqls)
        tdLog.info(f"inset {len(sqls)} rows successfully.")
        


    # 
    # 5. verify results
    #
    def verifyResults(self):
        # sleep
        time.sleep(5)

        # result_stream8
        result_sql = f"select * from test.`ana_vt_电子皮带秤_07_564885_anaEnd_ana_1914678932738560_1` "

        tdSql.query(result_sql)
        tdSql.checkRows(1)

        tdLog.info(f"verify stream ................................. successfully.")
