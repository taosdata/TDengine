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
            # stream5 sub3
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream5_sub3` SESSION(ts, 10m) FROM `tdasset`.`vt_em-5` STREAM_OPTIONS(PRE_FILTER(`电压`>300)) NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream5_sub3` AS SELECT _twstart+0s AS ts, COUNT(*) AS cnt, LAST(`电流`) AS `最后电流` FROM %%trows",
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
        # stream5
        self.trigger_stream5()


    # 
    # 5. verify results
    #
    def verifyResults(self):
        self.verify_stream5_sub3()


    # ---------------------   stream trigger    ----------------------


    #
    #  stream5 trigger 
    #
    def trigger_stream5(self):
        ts = self.start2
        table = "asset01.`em-5`"
        step  = 1 * 60 * 1000 # 1 minute
        
        # first window have 3 + 5 = 10 rows
        count = 3
        cols = "ts,current,voltage,power"
        vals = "30,400,200"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # blank 9
        ts += 9 * step

        # boundary of first window
        count = 1
        vals = "31,300,200"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)
        
        # last
        count = 2
        vals = "32,401,201"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # save span ts
        spanTs = ts

        # blank 30 triggered      
        ts += 30 * step

        # trigger first windows close
        count = 2
        vals = "40,500,300"
        ts = tdSql.insertFixedVal(table, ts, step, count, cols, vals)

        # disorder write 2 rows from spanTs
        count = 2
        disTs = spanTs + 3 * step
        orderVals = [36, 406, 206]
        disTs = tdSql.insertOrderVal(table, disTs, step, count, cols, orderVals)

        # blank 12
        disTs +=  12 * step

        # disorder write 1 rows
        count = 2
        vals = "39,200,200"
        disTs = tdSql.insertFixedVal(table, disTs, step, count, cols, vals)


    #
    # ---------------------   verify    ----------------------
    #


    #
    # verify stream5
    #
    def verify_stream5_sub3(self):
        # check
        result_sql = f"select * from tdasset.`result_stream5_sub3` "
        tdSql.checkResultsByFunc (
            sql  = result_sql, 
            func = lambda: tdSql.getRows() == 2
            # row1
            and tdSql.compareData(0, 0, self.start2) # ts
            and tdSql.compareData(0, 1, 3)           # cnt
            and tdSql.compareData(0, 2, 30)          # last current
            # row2
            and tdSql.compareData(1, 0, 1752574980000) # ts
            and tdSql.compareData(1, 1, 2 + 2)         # cnt
            and tdSql.compareData(1, 2, 37)            # last current
        )

        tdLog.info(f"verify stream5 sub3 ............................ successfully.")
