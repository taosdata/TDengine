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
        """Nevados

        Refer: https://taosdata.feishu.cn/wiki/Zkb2wNkHDihARVkGHYEcbNhmnxb

        Catalog:
            - Streams:UseCases

        Since: v3.3.7.0

        Labels: common,ci,skip

        Jira: https://jira.taosdata.com:18080/browse/TD-36363

        History:
            - 2025-7-10 Alex Duan Created

        """

        #
        #  main test
        #

        # env
        tdStream.createSnode()

        # prepare data
        self.prepare()

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
    # prepare data
    #
    def prepare(self):
        # name
        self.db = "assert01"
        self.vdb = "tdasset"
        self.stb = "electricity_meters"
        self.start = 1752563000000
        self.start_current = 10
        self.start_voltage = 260

        self.start2 = 1752574200000

        # import data
        etool.taosdump(f"-i cases/41-StreamProcessing/20-UseCase/meters_data/data/")

        tdLog.info(f"import data to db={self.db} successfully.")


    # 
    # 1. create vtables
    #
    def createVtables(self):
        sqls = [
            "create database tdasset;",
            "use tdasset;",
            "CREATE STABLE `vst_智能电表_1` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `电流` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `电压` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `功率` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `相位` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium') TAGS (`_ignore_path` VARCHAR(20), `地址` VARCHAR(50), `单元` TINYINT, `楼层` TINYINT, `设备ID` VARCHAR(20), `path1` VARCHAR(512)) SMA(`ts`,`电流`) VIRTUAL 1;",
            "CREATE STABLE `vst_智能水表_1` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `流量` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `水压` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`_ignore_path` VARCHAR(20), `地址` VARCHAR(50), `path1` VARCHAR(512)) SMA(`ts`,`流量`) VIRTUAL 1;",
            "CREATE VTABLE `vt_em-1` (`电流` FROM `asset01`.`em-1`.`current`, `电压` FROM `asset01`.`em-1`.`voltage`, `功率` FROM `asset01`.`em-1`.`power`, `相位` FROM `asset01`.`em-1`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 1, 2, 'em202502200010001', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-2` (`电流` FROM `asset01`.`em-2`.`current`, `电压` FROM `asset01`.`em-2`.`voltage`, `功率` FROM `asset01`.`em-2`.`power`, `相位` FROM `asset01`.`em-2`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 1, 2, 'em202502200010002', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-3` (`电流` FROM `asset01`.`em-3`.`current`, `电压` FROM `asset01`.`em-3`.`voltage`, `功率` FROM `asset01`.`em-3`.`power`, `相位` FROM `asset01`.`em-3`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 1, 2, 'em202502200010003', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-4` (`电流` FROM `asset01`.`em-4`.`current`, `电压` FROM `asset01`.`em-4`.`voltage`, `功率` FROM `asset01`.`em-4`.`power`, `相位` FROM `asset01`.`em-4`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 2, 2, 'em202502200010004', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-5` (`电流` FROM `asset01`.`em-5`.`current`, `电压` FROM `asset01`.`em-5`.`voltage`, `功率` FROM `asset01`.`em-5`.`power`, `相位` FROM `asset01`.`em-5`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.海淀.西三旗街道', 2, 2, 'em202502200010005', '公共事业.北京.海淀.西三旗街道');",
            "CREATE VTABLE `vt_em-6` (`电流` FROM `asset01`.`em-6`.`current`, `电压` FROM `asset01`.`em-6`.`voltage`, `功率` FROM `asset01`.`em-6`.`power`, `相位` FROM `asset01`.`em-6`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道', 1, 2, 'em20250220001006', '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-7` (`电流` FROM `asset01`.`em-7`.`current`, `电压` FROM `asset01`.`em-7`.`voltage`, `功率` FROM `asset01`.`em-7`.`power`, `相位` FROM `asset01`.`em-7`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道', 1, 2, 'em20250220001007', '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-8` (`电流` FROM `asset01`.`em-8`.`current`, `电压` FROM `asset01`.`em-8`.`voltage`, `功率` FROM `asset01`.`em-8`.`power`, `相位` FROM `asset01`.`em-8`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道', 1, 2, 'em20250220001008', '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-9` (`电流` FROM `asset01`.`em-9`.`current`, `电压` FROM `asset01`.`em-9`.`voltage`, `功率` FROM `asset01`.`em-9`.`power`, `相位` FROM `asset01`.`em-9`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.国贸街道', 1, 2, 'em20250220001009', '公共事业.北京.朝阳.国贸街道');",
            "CREATE VTABLE `vt_em-10` (`电流` FROM `asset01`.`em-10`.`current`, `电压` FROM `asset01`.`em-10`.`voltage`, `功率` FROM `asset01`.`em-10`.`power`, `相位` FROM `asset01`.`em-10`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.三元桥街道', 1, 2, 'em202502200010010', '公共事业.北京.朝阳.三元桥街道');",
            "CREATE VTABLE `vt_em-11` (`电流` FROM `asset01`.`em-11`.`current`, `电压` FROM `asset01`.`em-11`.`voltage`, `功率` FROM `asset01`.`em-11`.`power`, `相位` FROM `asset01`.`em-11`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道', 11, 11, 'em202502200010011', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_em-12` (`电流` FROM `asset01`.`em-12`.`current`, `电压` FROM `asset01`.`em-12`.`voltage`, `功率` FROM `asset01`.`em-12`.`power`, `相位` FROM `asset01`.`em-12`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道', 11, 12, 'em202502200010012', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_em-13` (`电流` FROM `asset01`.`em-13`.`current`, `电压` FROM `asset01`.`em-13`.`voltage`, `功率` FROM `asset01`.`em-13`.`power`, `相位` FROM `asset01`.`em-13`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道', 11, 13, 'em202502200010013', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_em-14` (`电流` FROM `asset01`.`em-14`.`current`, `电压` FROM `asset01`.`em-14`.`voltage`, `功率` FROM `asset01`.`em-14`.`power`, `相位` FROM `asset01`.`em-14`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道', 11, 14, 'em202502200010014', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_em-15` (`电流` FROM `asset01`.`em-15`.`current`, `电压` FROM `asset01`.`em-15`.`voltage`, `功率` FROM `asset01`.`em-15`.`power`, `相位` FROM `asset01`.`em-15`.`phase`) USING `vst_智能电表_1` (`_ignore_path`, `地址`, `单元`, `楼层`, `设备ID`, `path1`) TAGS (NULL, '北京.朝阳.望京街道', 1, 15, 'em202502200010015', '公共事业.北京.朝阳.望京街道');",
            "CREATE VTABLE `vt_wm-1` (`流量` FROM `asset01`.`wm-1`.`rate`, `水压` FROM `asset01`.`wm-1`.`pressure`) USING `vst_智能水表_1` (`_ignore_path`, `地址`, `path1`) TAGS (NULL, '北京.朝阳.三元桥街道', '公共事业.北京.朝阳.三元桥街道');"
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} vtable successfully.")
        

    # 
    # 2. create streams
    #
    def createStreams(self):

        sqls = [
            # stream8
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream8`      PERIOD(1s, 0s) FROM `tdasset`.`vt_em-8`                                       NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8`      AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream8_sub1` PERIOD(1s, 0s) FROM `tdasset`.`vt_em-8` STREAM_OPTIONS(IGNORE_NODATA_TRIGGER) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream8_sub1` AS SELECT CAST(_tlocaltime/1000000 as timestamp) AS ts, COUNT(*) AS cnt, AVG(`电压`) AS `平均电压`, SUM(`功率`) AS `功率和` FROM %%trows",
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
        # stream8
        self.trigger_stream8()


    # 
    # 5. verify results
    #
    def verifyResults(self):
        self.verify_stream8()


    # ---------------------   stream trigger    ----------------------


    #
    #  stream8 trigger
    #
    def trigger_stream8(self):
        ts    = self.start2
        table = "asset01.`em-8`"
        cols  = "ts,current,voltage,power"
        sleepS = 0.2  # 0.2 seconds

        # write order
        count = 20
        fixedVals = "100, 200, 300"
        tdSql.insertNow(table, sleepS, count, cols, fixedVals)

        # write disorder data
        count = 10
        step  = 200
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, fixedVals)




    #
    # ---------------------   verify    ----------------------
    #


    #
    # verify stream8
    #
    def verify_stream8(self):
        # sleep
        time.sleep(5)

        # result_stream8
        result_sql = f"select * from {self.vdb}.`result_stream8` "

        tdSql.query(result_sql)
        count = tdSql.getRows()
        sum   = 0

        for i in range(count):
            # row
            cnt = tdSql.getData(i, 1)
            if cnt > 5:
                tdLog.exit(f"stream8 expected cnt <= 5, actual cnt={cnt}")
            elif cnt > 0:
                tdSql.checkData(i, 2, 200)  # avg(voltage)
                tdSql.checkData(i, 3, cnt * 300) # sum(power)
            sum += cnt
        
        if sum != 30:
            tdLog.exit(f"stream8 not found expected data. expected sum(cnt) == 30, actual: {sum}")

        tdLog.info(f"verify stream8 ................................. successfully.")

        # sub
        self.verify_stream8_sub1()

    # verify stream8_sub1    
    def verify_stream8_sub1(self):
        # check sum
        tdSql.checkResultsByFunc (
            sql  = f"select sum(cnt) from {self.vdb}.`result_stream8_sub1`",
            func = lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 30)
        )

        # check no data windows is zero
        tdSql.checkResultsByFunc (
            sql  = f"select * from {self.vdb}.`result_stream8_sub1` where cnt = 0 ",
            func = lambda: tdSql.getRows() == 0
        )

        tdLog.info("verify stream8_sub1 ............................. successfully.")