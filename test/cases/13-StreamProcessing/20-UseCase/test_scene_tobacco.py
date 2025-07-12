from new_test_framework.utils import tdLog, tdSql, tdStream, etool
import time


class TestSceneTobacco:
    def test_tobacco(self):
        """
        Refer: https://taosdata.feishu.cn/wiki/XaqbweV96iZVRnkgHLJcx2ZCnQf
        Catalog:
            - Streams:UseCases
        Since: v3.3.3.7
        Labels: common,ci
        Jira: https://jira.taosdata.com:18080/browse/TD-36368
        History:
            - 2025-7-11 zyyang90 Created
        """
        # env
        tdStream.createSnode()

        # prepare data
        self.prepare()

        # create vtables
        self.createVirTables()

        # create streams
        self.createStreams()

        # insert trigger data
        self.insertTriggerData()

        # wait stream processing
        time.sleep(5)

        # verify results
        self.verifyResults()

    def prepare(self):
        # name
        self.db = "tdasset_demo_tobacco"
        self.stb = "vibrating_conveyor"
        self.vdb = "tdasset"
        self.vstb = "vst_振动输送机"
        self.stream = "ana_振动输送机"
        self.ts = int(time.time() * 1000)
        # self.ts = 1752165000000

        # drop database if exists
        tdSql.execute(f"DROP DATABASE IF EXISTS {self.db};")
        # import tobacco scene data
        etool.taosdump("-i cases/13-StreamProcessing/20-UseCase/tobacco_data/")
        # schema:
        # CREATE STABLE `vibrating_conveyor` (
        #   `ts` TIMESTAMP,
        #   `motor_signal` FLOAT,
        #   `vibration_amplitude` FLOAT
        # ) TAGS (
        #   `tobacco_asset` NCHAR(256),
        #   `process_stage` NCHAR(32),
        #   `process_step` NCHAR(32),
        #   `equipment_type` NCHAR(32)
        # )

        tdSql.execute(f"DELETE FROM `{self.db}`.`{self.stb}`;")
        tdSql.checkResultsByFunc(
            sql=f"select count(*) from `{self.db}`.`{self.stb}`;",
            func=lambda: tdSql.compareData(0, 0, 0),
        )
        tdLog.info(f"import data to db: {self.db} done")

    def createVirTables(self):
        # create virtual stable
        sqls = [
            f"DROP DATABASE IF EXISTS {self.vdb};",
            f"CREATE DATABASE IF NOT EXISTS {self.vdb};",
            f"CREATE STABLE `{self.vdb}`.`{self.vstb}` (`ts` TIMESTAMP, `电机信号` FLOAT, `振动幅度` FLOAT) TAGS (`卷烟制丝场景` VARCHAR(256), `工艺段` VARCHAR(32), `工序` VARCHAR(32), `设备类型` VARCHAR(32)) SMA(`ts`,`电机信号`) VIRTUAL 1;",
        ]
        tdSql.executes(sqls)

        # create virtable sub-tables
        res = tdSql.getResult(
            f"SELECT DISTINCT tbname,tobacco_asset,process_stage,process_step,equipment_type from `{self.db}`.`{self.stb}`"
        )
        table_count = len(res)
        for row in res:
            tbname = row[0]
            tobacco_asset = row[1]
            process_stage = row[2]
            process_step = row[3]
            equipment_type = row[4]
            # tdLog.info(
            #     f"tobacco_asset={tobacco_asset}, process_stage={process_stage}, process_step={process_step}, equipment_type={equipment_type}"
            # )
            sql = f"CREATE VTABLE `{self.vdb}`.`{self.vstb}_{tbname}`(`电机信号` FROM `{self.db}`.`{tbname}`.`motor_signal`, `振动幅度` FROM `{self.db}`.`{tbname}`.`vibration_amplitude`) USING `{self.vdb}`.`{self.vstb}`(`卷烟制丝场景`,`工艺段`,`工序`,`设备类型`) TAGS('{tobacco_asset}','{process_stage}','{process_step}','{equipment_type}');"
            # tdLog.info(f"create vtable sql: {sql}")
            tdSql.execute(sql)

        # check vtables created
        tdSql.checkResultsByFunc(
            sql=f"show `{self.vdb}`.VTABLES",
            func=lambda: tdSql.getRows() == table_count,
        )
        tdLog.info(f"create {table_count} vtables in db: {self.vdb}")

    def createStreams(self):
        # create stream
        sql = f"CREATE STREAM IF NOT EXISTS `{self.vdb}`.`{self.stream}` INTERVAL(30m) SLIDING(5m) From `{self.vdb}`.`{self.vstb}` NOTIFY('ws://idmp:6042/eventReceive') ON(WINDOW_OPEN|WINDOW_CLOSE) INTO `{self.vdb}`.`{self.stream}` AS SELECT _twstart as output_timestamp, AVG(电机信号) AS `电机信号平均值` From `{self.vdb}`.`{self.vstb}` WHERE ts >=_twstart and ts <=_twend"
        tdLog.info(f"create stream sql: {sql}")
        tdSql.execute(sql)

        # check stream status
        tdStream.checkStreamStatus()

    def insertTriggerData(self):
        res = tdSql.getResult(f"select distinct tbname from `{self.db}`.`{self.stb}`")
        table_count = len(res)
        for row in res:
            tbname = row[0]
            for i in range(30):
                sql = f"INSERT INTO `{self.db}`.`{tbname}` VALUES ({self.ts - (30-i) * 60 * 1000}, {(i % 5)}.0, {i % 5}.0);"
                tdLog.info(f"sql: {sql}")
                tdSql.execute(sql)
        tdSql.checkResultsByFunc(
            sql=f"select * from `{self.db}`.`{self.stb}`;",
            func=lambda: tdSql.getRows() == 30 * table_count,
        )
        tdLog.info(f"insert trigger data done, total {30 * table_count} rows")

    def verifyResults(self):
        # select * from `tdasset_demo_tobacco`.`vibrating_conveyor`;
        # select * from `tdasset_demo_tobacco`.`f1w1a_vibrating_conveyor_06` order by ts asc;
        # select * from `tdasset`.`vst_振动输送机`;
        # select * from `tdasset`.`ana_振动输送机`;
        tdSql.checkResultsByFunc(
            sql=f"select * from `{self.vdb}`.`{self.stream}`;",
            func=lambda: tdSql.getRows() == 6 and tdSql.getCols() == 2,
        )
        # TODO: Add more specific checks for the results
