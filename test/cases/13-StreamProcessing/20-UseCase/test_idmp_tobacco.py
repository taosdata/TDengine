from new_test_framework.utils import tdLog, tdSql, tdStream, etool
import time
import os
import json


class TestIdmpTobacco:
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
        TestIdmpTobaccoImpl().run()


class TestIdmpTobaccoImpl:
    def init(self):
        self.stream_ids = []

    def run(self):

        # prepare data
        self.prepare()

        # create vtables
        self.createVirTables()

        # create streams
        self.createStreams()

        # insert trigger data
        self.insertTriggerData()

        # # wait stream processing
        time.sleep(5)

        # # verify results
        self.verifyResults()

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
                    tdSql.execute(sql, queryTimes=1)

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
                    tdSql.execute(sql, queryTimes=1)

        # check vtables created
        tdSql.checkResultsByFunc(
            sql=f"show `{self.vdb}`.VTABLES",
            func=lambda: tdSql.getRows() == vtb_count,
        )
        tdLog.info(f"create {vtb_count} vtables in db: {self.vdb}")

    def createStreams(self):
        with open(
            "cases/13-StreamProcessing/20-UseCase/tobacco_data/idmp/stream.json",
            "r",
            encoding="utf-8",
        ) as f:
            arr = json.load(f)
            self.stream_objs = [StreamObj.from_dict(obj) for obj in arr]

        # streams can be specified by environment variable
        # if not specified, all streams in the stream.sql will be created
        if hasattr(self, "stream_ids") and len(self.stream_ids) > 0:
            tdLog.info(f"USE specified stream ids: {self.stream_ids}")
        elif "IDMP_TOBACCO_STREAM_IDS" in os.environ:
            ids = os.environ.get("IDMP_TOBACCO_STREAM_IDS")
            if ids:
                self.stream_ids = [
                    int(x) for x in ids.split(",") if x.strip().isdigit()
                ]
                tdLog.info(f"use IDMP_TOBACCO_STREAM_IDS from env: {self.stream_ids}")
        else:
            self.stream_ids = [obj.id for obj in self.stream_objs]
            tdLog.info(f"use all stream ids: {self.stream_ids}")

        # 遍历 self.stream_objs，如果 id 在 self.stream_ids 中，则创建 Stream
        stream_count = 0
        for obj in self.stream_objs:
            if obj.id in self.stream_ids and obj.create:
                # 生成 stream 名称，可根据实际需求生成
                stream_name = obj.name if obj.name else f"stream_{obj.id}"
                create_sql = obj.create.replace("%STREAM_NAME", stream_name)
                tdLog.info(f"create stream SQL: {create_sql}")
                tdSql.execute(create_sql, queryTimes=1)
                stream_count += 1
                # check streams created
                # tdStream.checkStreamStatus(stream_name)

            # check streams created
            tdStream.checkStreamStatus()

        tdLog.info(f"create {stream_count} streams in {self.vdb}")

    def insertTriggerData(self):
        tdSql.execute(f"USE `{self.db}`;")

        for obj in self.stream_objs:
            # skip if id not in self.stream_ids
            if obj.id not in self.stream_ids:
                continue
            # skip if no data or data is not a list
            if not obj.data or not isinstance(obj.data, list):
                continue

            # 以当前时间戳为准，取整到 1 min
            ts = (int(time.time()) // 60) * 60 * 1000
            # 默认间隔为 1 分钟
            interval = obj.interval if obj.interval else 60 * 1000
            rows = len(obj.data)

            for i, sql in enumerate(obj.data):
                if "%TIMESTAMP" in sql:
                    current_ts = ts - (rows - i) * interval
                    exec_sql = sql.replace("%TIMESTAMP", str(current_ts))
                else:
                    exec_sql = sql
                tdLog.info(f"SQL: {exec_sql}")
                tdSql.execute(exec_sql, queryTimes=1)

        tdLog.info("insert trigger data done")

    def verifyResults(self):
        for id in self.stream_ids:
            # 从 self.stream_objs 中找到 id 相同的 stream_obj
            obj = next((o for o in self.stream_objs if o.id == id), None)
            # skip if obj.assert_list is None or empty
            if obj.assert_list is None or len(obj.assert_list) == 0:
                tdLog.info(f"no assert for stream id: {id}, skip verify")
                continue

            name = obj.name if obj and obj.name else f"stream_{id}"

            # check the output table
            res = tdSql.getResult(f"SHOW `{self.vdb}`.stables like '{name}';")
            if res is None or len(res) == 0:
                res = tdSql.getResult(f"SHOW `{self.vdb}`.tables like '{name}';")
                if res is None or len(res) == 0:
                    raise RuntimeError(
                        f"assert failed: output table '{name}' not found"
                    )

            # check the output
            output = tdSql.getResult(f"SELECT * FROM `{self.vdb}`.`{name}`;")
            for a in obj.assert_list:
                if a.row >= len(output) or a.col >= len(output[a.row]):
                    raise AssertionError(
                        f"assert failed: out of boundary, row: {a.row}, col: {a.col}"
                    )
                actual = output[a.row][a.col]
                if str(actual) != str(a.data):
                    raise AssertionError(
                        f"assert failed: not equal, row: {a.row}, col: {a.col}, expect: {a.data}, actual: {actual}"
                    )

        tdLog.info("verify results done")


class StreamObj:
    def __init__(self, id, name, create, data=None, interval=None, assert_list=None):
        self.id = id
        self.name = name
        self.create = create
        self.data = data
        self.interval = interval
        self.assert_list = assert_list if assert_list else []

    @staticmethod
    def from_dict(d):
        assert_list = [AssertObj.from_dict(a) for a in d.get("assert", [])]
        return StreamObj(
            id=d.get("id"),
            name=d.get("name"),
            create=d.get("create"),
            data=d.get("data"),
            interval=d.get("interval"),
            assert_list=assert_list,
        )


class AssertObj:
    def __init__(self, row, col, data):
        self.row = row
        self.col = col
        self.data = data

    @staticmethod
    def from_dict(d):
        return AssertObj(
            row=d.get("row"),
            col=d.get("col"),
            data=d.get("data"),
        )
