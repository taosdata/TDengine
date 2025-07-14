from new_test_framework.utils import tdLog, tdSql, tdStream, etool
import time
import os


class TestSceneTobacco:

    def test_tobacco(self):
        """
        Refer: https://taosdata.feishu.cn/wiki/XaqbweV96iZVRnkgHLJcx2ZCnQf
        Catalog:
            - Streams:UseCases
        Since: v3.3.6.14
        Labels: common,ci
        Jira:
            - https://jira.taosdata.com:18080/browse/TD-36514
        History:
            - 2025-7-11 zyyang90 Created
        """
        # prepare data
        self.prepare()

        # create vtables
        self.createVirTables()

        # streams can be specified by environment variable
        # if not specified, all streams in the stream.sql will be created
        self.stream_ids = [8]

        # create streams
        self.createStreams(self.stream_ids)

        # insert trigger data
        self.insertTriggerData(self.stream_ids)

        # wait stream processing
        time.sleep(5)

        # verify results
        self.verifyResults(self.stream_ids)

        tdLog.info("test IDMP tobacco scene done")

    def prepare(self):
        # create snode if not exists
        snodes = tdSql.getResult("SHOW SNODES;")
        if snodes is None or len(snodes) == 0:
            tdStream.createSnode()

        # name
        self.db = "idmp_sample_tobacco"
        self.vdb = "idmp"

        # drop database if exists
        tdSql.executes(
            [
                f"DROP DATABASE IF EXISTS {self.vdb};",
                f"DROP DATABASE IF EXISTS {self.db};",
            ]
        )
        # import tobacco scene data
        etool.taosdump(
            "-i cases/13-StreamProcessing/20-UseCase/tobacco_data/idmp_sample_tobacco/"
        )

        # delete existed data
        res = tdSql.getResult(f"show `{self.db}`.stables")
        for s in res:
            stable = s[0]
            tdSql.execute(f"DELETE FROM `{self.db}`.`{stable}`;")
            tdSql.checkResultsByFunc(
                sql=f"select count(*) from `{self.db}`.`{stable}`;",
                func=lambda: tdSql.compareData(0, 0, 0),
            )

        tdLog.info(f"import data to db: {self.db} done")

    def createVirTables(self):
        # create database which stroe virtual tables
        tdSql.executes(
            [
                f"DROP DATABASE IF EXISTS {self.vdb};",
                f"CREATE DATABASE IF NOT EXISTS {self.vdb};",
                f"USE {self.vdb};",
            ]
        )

        # create virtual stables
        with open(
            "cases/13-StreamProcessing/20-UseCase/tobacco_data/idmp/vstb.sql",
            "r",
            encoding="utf-8",
        ) as f:
            for line in f:
                sql = line.strip()
                if sql:
                    tdLog.debug(f"virtual stable SQL: {sql}")
                    tdSql.execute(sql)

        # self.stable_map = {
        #     "drum_air_flow_dryer": "vst_滚筒气流烘丝机_608157",
        #     "cutting_public_point": "vst_切丝公共点_864610",
        #     "fragrance_machine": "vst_加香机_101420",
        #     "metering_pipe": "vst_计量管_958712",
        #     "electronic_belt_scale": "vst_电子皮带秤_527456",
        #     "belt_conveyor": "vst_带式输送机_703474",
        #     "drying_public_point": "vst_烘丝公共点_915945",
        #     "thin_plate_dryer": "vst_薄板烘丝机_884743",
        #     "flip_box_feeder": "vst_翻箱喂料机构_553580",
        #     "drum_type_stem_rehumidifier": "vst_滚筒式烟梗回潮机_514261",
        #     "jet_vacuum_rehumidifier": "vst_喷射式真空回潮机_348599",
        #     "wind_fiber_feed_public_point": "vst_风力送丝公共点_734506",
        #     "public_point": "vst_公共点_725959",
        #     "fragrance_public_point": "vst_加香公共点_667004",
        #     "leaf_storage_cabinet": "vst_贮叶柜_597655",
        #     "feeding_machine2": "vst_喂料机_369036",
        #     "vibrating_conveyor": "vst_振动输送机_701259",
        #     "feeding_machine": "vst_加料机_130700",
        #     "chain_conveyor": "vst_链式输送机_452298",
        # }

        # create virtable tables
        vtb_count = 0
        with open(
            "cases/13-StreamProcessing/20-UseCase/tobacco_data/idmp/vtb.sql",
            "r",
            encoding="utf-8",
        ) as f:
            for line in f:
                sql = line.strip()
                if sql:
                    tdLog.debug(f"virtual table SQL: {sql}")
                    vtb_count += 1
                    tdSql.execute(sql)

        # check vtables created
        tdSql.checkResultsByFunc(
            sql=f"show `{self.vdb}`.VTABLES",
            func=lambda: tdSql.getRows() == vtb_count,
        )
        tdLog.info(f"create {vtb_count} vtables in {self.vdb}")

    def createStreams(self, stream_ids):
        self.stream_name_map = {
            1: "ana_振动输送机_平均值",
            2: "ana_振动输送机_超过10分钟没有上报数据",
            3: "ana_振动输送机_电机信号最大值",
            4: "ana_振动输送机_振动幅度总和",
            5: "ana_振动输送机_最后一条电机信号",
            6: "ana_振动输送机_振动幅度标准差",
            7: "ana_振动输送机_电机信号极差",
            8: "ana_振动输送机_超过15分钟没有上报电机信号数据",
            9: "ana_振动输送机_过去15分钟的振动幅度变化率",
        }

        stream_count = 0
        with open(
            "cases/13-StreamProcessing/20-UseCase/tobacco_data/idmp/stream.sql",
            "r",
            encoding="utf-8",
        ) as f:
            for idx, line in enumerate(f, start=1):
                stream_name = self.stream_name_map.get(idx, "")
                sql = line.strip().replace("%STREAM_NAME", stream_name)
                if sql and (stream_ids is None or idx in stream_ids):
                    tdLog.debug(f"stream SQL: {sql}")
                    stream_count += 1
                    tdSql.execute(sql)
                    # check streams created
                    tdStream.checkStreamStatus(stream_name)

        tdLog.info(f"create {stream_count} streams in {self.vdb}")

    def insertTriggerData(self, stream_ids):
        if stream_ids is None or 1 in stream_ids:
            self.insertTriggerDataForStream1()

        tdLog.info("insert trigger data done")

    def insertTriggerDataForStream1(self):
        """
        系统中振动输送机的振动幅度,每5分钟计算一次过去半小时的平均值，常规告警
        """
        # 以当前时间戳为准，取整到 1 min
        ts = (int(time.time()) // 60) * 60 * 1000
        rows = 35

        res = tdSql.getResult(
            f"select distinct tbname from `{self.db}`.`vibrating_conveyor`"
        )
        for row in res:
            tbname = row[0]
            for i in range(rows):
                sql = f"INSERT INTO `{self.db}`.`{tbname}` VALUES ({ts - (rows - i) * 60 * 1000}, {i % 5}.0, {i % 5}.0);"
                # tdLog.info(f"sql: {sql}")
                tdSql.execute(sql)

        tdLog.debug("insert trigger data for stream 1")

    def verifyResults(self, stream_ids):
        # if stream_ids is None or 1 in stream_ids:
        #     self.verifyResultsForStream1()
        for id in stream_ids:
            stream_name = self.stream_name_map.get(id, "")
            res = tdSql.getResult(f"SHOW `{self.vdb}`.stables like '{stream_name}';")
            if res is None or len(res) == 0:
                raise RuntimeError(
                    f"查询结果为空: SHOW `{self.vdb}`.stables like '{stream_name}';"
                )

        tdLog.info("verify results done")
