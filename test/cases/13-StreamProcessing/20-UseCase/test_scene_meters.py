import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, etool
from datetime import datetime
from datetime import date


class Test_Scene_Asset01:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_usecase_em(self):
        """Nevados

        Refer: https://taosdata.feishu.cn/wiki/Zkb2wNkHDihARVkGHYEcbNhmnxb

        Catalog:
            - Streams:UseCases

        Since: v3.3.7.0

        Labels: common,ci

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

        # wait stream processing
        self.waitStreamProcessing()

        # verify results
        self.verifyResults()


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

        # import data
        etool.taosdump(f"-i cases/13-StreamProcessing/20-UseCase/asset01/data/")

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
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream1`  event_window( start with `电压` > 250 end with `电压` <= 250 ) TRUE_FOR(10m) FROM `tdasset`.`vt_em-1`  NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream1` AS SELECT _twstart+0s AS output_timestamp, avg(`电压`) AS `平均电压`  FROM tdasset.`vt_em-1`  WHERE ts >= _twstart AND ts <=_twend;",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream2`  interval(1h)  sliding(5m) FROM `tdasset`.`vt_em-2`  notify('ws://idmp:6042/eventReceive') ON(window_open|window_close) INTO `tdasset`.`result_stream2` AS SELECT _twstart+0s AS output_timestamp, max(`电流`) AS `最大电流` FROM tdasset.`vt_em-2`  WHERE ts >=_twstart AND ts <=_twend;",
            "CREATE STREAM IF NOT EXISTS `tdasset`.`ana_stream3`  event_window( start with `电流` > 100 end with `电流` <= 100 ) TRUE_FOR(5m) FROM `tdasset`.`vt_em-3` NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `tdasset`.`result_stream3` AS SELECT _twstart+0s AS output_timestamp, AVG(`电流`) AS `平均电流` FROM tdasset.`vt_em-3`  WHERE ts >= _twstart AND ts <=_twend"
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create {len(sqls)} streams successfully.")

    # 
    # 3. wait stream ready
    #
    def checkStreamStatus(self):
        print("check status")
        tdStream.checkStreamStatus()
        tdLog.info(f"check stream status successfully.")

    # 
    # 4. insert trigger data
    #
    def writeTriggerData(self):
        # strem1
        self.trigger_stream1()
        # stream2
        self.trigger_stream2()
        # stream3
        self.trigger_stream3()


    # 
    # 5. wait stream processing
    #
    def waitStreamProcessing(self):
        tdLog.info("wait for check result sleep 5s ...")
        time.sleep(5)

    # 
    # 5. verify results
    #
    def verifyResults(self):
        self.verify_stream1()
        self.verify_stream2()
        self.verify_stream3()


    # em1-stream1 trigger voltage > 250 start and voltage <= 250 end
    def trigger_stream1(self):

        # 1~20 minutes no trigger
        ts = self.start
        for i in range(20):
            ts += 1*60*1000
            sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 260);"
            tdSql.execute(sql, show=True)

        # 20~40 minutes trigger
        voltage = 251
        for i in range(20):
            ts += 1*60*1000
            voltage += 1
            sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, {voltage});"
            tdSql.execute(sql, show=True)


        # 40~60 minutes no triiger with high lower data 
        for i in range(20):
            ts += 1*60*1000
            if i % 2 == 0:
                voltage = 250 - i
            else:
                voltage = 260 + i 
            sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, {voltage});"
            tdSql.execute(sql, show=True)

        # 60~71 minutes trigger
        for i in range(11):
            ts += 1*60*1000
            sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 260);"
            tdSql.execute(sql, show=True)

        # end trigger
        for i in range(2):
            ts += 1*60*1000
            sql = f"insert into asset01.`em-1`(ts,voltage) values({ts}, 230);"
            tdSql.execute(sql, show=True)


    # em2-stream1 trigger
    def trigger_stream2(self):
        ts = self.start
        current = self.start_current
        voltage = self.start_voltage
        power   = 200
        phase   = 0

        cnt     = 11 # 
        for i in range(cnt):
            ts += 1 * 60 * 1000
            current += 1
            voltage += 1
            power   += 1
            phase   += 1
            sql = f"insert into asset01.`em-2` values({ts}, {current}, {voltage}, {power}, {phase});"
            tdSql.execute(sql, show=True)

    # stream3 trigger
    def trigger_stream3(self):
        ts = self.start
        current = 100
        voltage = 220
        power   = 200
        phase   = 0

        # enter condiction
        cnt     = 10
        for i in range(cnt):
            ts += 1 * 60 * 1000
            current += 1
            voltage += 1
            power   += 1
            phase   += 1
            sql = f"insert into asset01.`em-3` values({ts}, {current}, {voltage}, {power}, {phase});"
            tdSql.execute(sql, show=True)

        # leave condiction
        cnt     = 10 #
        current = 100
        for i in range(cnt):
            ts += 1 * 60 * 1000
            current -= 1
            voltage += 1
            power   += 1
            phase   += 1
            sql = f"insert into asset01.`em-3` values({ts}, {current}, {voltage}, {power}, {phase});"
            tdSql.execute(sql, show=True)

        cnt     = 20
        current = 200
        for i in range(cnt):
            ts += 1 * 60 * 1000
            current += 1
            voltage += 1
            power   += 1
            phase   += 1
            sql = f"insert into asset01.`em-3` values({ts}, {current}, {voltage}, {power}, {phase});"
            tdSql.execute(sql, show=True)


    # verify stream1
    def verify_stream1(self):
        sql = f"select * from {self.vdb}.`result_stream1` "
        tdSql.waitedQuery(sql, 2, 100)
        tdLog.info("verify stream1 successfully.")

    # verify stream2
    def verify_stream2(self):
        sql = f"select * from {self.vdb}.`result_stream2` "
        tdSql.query(sql, show=True)
        cnt = tdSql.getRows()
        if cnt == 0:
            tdLog.exit("stream2 rows is zero.")
        else:
            tdLog.info("verify stream2 successfully.")

    # verify stream3
    def verify_stream3(self):
        sql = f"select * from {self.vdb}.`result_stream3` "
        tdSql.query(sql, show=True)
        cnt = tdSql.getRows()
        if cnt == 0:
            tdLog.exit("stream3 rows is zero.")
        else:
            tdLog.info("verify stream3 successfully.")            