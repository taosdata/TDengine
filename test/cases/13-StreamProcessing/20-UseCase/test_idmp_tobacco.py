from new_test_framework.utils import tdLog, tdSql, tdStream, etool
import time
import os
import json
import math


class TestIdmpTobacco:

    def test_idmp_tobacco(self):
        """IDMP 烟草场景测试

        Refer: https://taosdata.feishu.cn/wiki/Zkb2wNkHDihARVkGHYEcbNhmnxb#share-I9GwdF26PoWk6uxx2zJcxZYrn1d
        1. 测试 AI 推荐生成的分析，创建 Stream,验证流的正确性
        2. 测试手动创建的分析，验证流的正确性
            2.1. 触发类型：
                - 定时窗口：指定不同的窗口大小、窗口偏移
                - 状态窗口：指定状态的字段
                - 会话窗口：指定会话的时间间隔
            2.2. 时间窗口聚合：
                - 窗口开始时间: _tprev_localtime/ _twstart/ _tprev_ts
                - 窗口结束时间: _tlocaltime/ _twend/ _tcurrent_ts
            2.3. 输出属性：
                - AVG: 平均值
                - LAST:最新值
                - SUM: 求和
                - MAX: 最大值
                - STDDEV: 标准差
                - SPREAD: 极差
                - SPREAD/FIRST: 变化率

        Catalog:
            - Streams:UseCases

        Since: v3.3.6.14

        Labels: common,ci

        Jira:
            - https://jira.taosdata.com:18080/browse/TD-36514

        History:
            - 2025-7-11 zyyang90 Created
        """
        tobac = IdmpScene()
        tobac.init(
            "tobacco",
            "idmp_sample_tobacco",
            "idmp",
            "cases/13-StreamProcessing/20-UseCase/tobacco_data/idmp_sample_tobacco",
            "cases/13-StreamProcessing/20-UseCase/tobacco_data/idmp/vstb.sql",
            "cases/13-StreamProcessing/20-UseCase/tobacco_data/idmp/vtb.sql",
            "cases/13-StreamProcessing/20-UseCase/tobacco_data/idmp/stream.json",
        )
        # 这里可以指定需要创建的 stream_ids
        tobac.stream_ids = [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
        tobac.run()


class IdmpScene:
    def init(self, scene, db, vdb, db_dump_dir, vstb_sql, vtb_sql, stream_json):
        # scene name
        self.scene = scene
        # sample database
        self.db = db
        # analysis database
        self.vdb = vdb
        # sample database dump file
        self.db_dump_dir = db_dump_dir
        # virtual stables
        self.vstb_sql = vstb_sql
        # virtual tables
        self.vtb_sql = vtb_sql
        # stream json
        self.stream_json = stream_json
        # stream id filters
        self.stream_ids = []
        # golbal stream.assert.retry
        self.assert_retry = -1

    def run(self):

        # prepare data
        self.prepare()

        # create vtables
        self.createVirTables()

        # create streams
        self.createStreams()

        # insert trigger data
        self.insertTriggerData()

        # verify results
        self.verifyResults()

        tdLog.info(f"test IDMP {self.scene} scene done")

    def prepare(self):
        # create snode if not exists
        snodes = tdSql.getResult("SHOW SNODES;")
        if snodes is None or len(snodes) == 0:
            tdStream.createSnode()

        # drop database if exists
        tdSql.executes(
            [
                f"DROP DATABASE IF EXISTS {self.vdb};",
                f"DROP DATABASE IF EXISTS {self.db};",
            ]
        )
        # import tobacco scene data
        etool.taosdump(f"-i {self.db_dump_dir}")

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
        vstb_count = 0
        with open(f"{self.vstb_sql}", "r", encoding="utf-8") as f:
            for line in f:
                sql = line.strip()
                if sql:
                    tdLog.debug(f"virtual stable SQL: {sql}")
                    tdSql.execute(sql, queryTimes=1)
                    vstb_count += 1

        # check virtual stables
        tdSql.checkResultsByFunc(
            sql=f"show `{self.vdb}`.STABLES",
            func=lambda: tdSql.getRows() == vstb_count,
        )
        tdLog.info(f"create {vstb_count} virtual stables in {self.vdb}")

        # create virtable tables
        vtb_count = 0
        with open(f"{self.vtb_sql}", "r", encoding="utf-8") as f:
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
        tdLog.info(f"create {vtb_count} vtables in {self.vdb}")

    def createStreams(self):
        with open(f"{self.stream_json}", "r", encoding="utf-8") as f:
            arr = json.load(f)
            self.stream_objs = [StreamObj.from_dict(obj) for obj in arr]

        # streams can be specified by environment variable
        # if not specified, all streams in the stream.sql will be created
        if hasattr(self, "stream_ids") and len(self.stream_ids) > 0:
            tdLog.info(f"USE specified stream ids: {self.stream_ids}")
        elif "IDMP_STREAM_IDS" in os.environ:
            ids = os.environ.get("IDMP_STREAM_IDS")
            if ids:
                self.stream_ids = [
                    int(x) for x in ids.split(",") if x.strip().isdigit()
                ]
                tdLog.info(f"use IDMP_STREAM_IDS from env: {self.stream_ids}")
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
            sql = f"select stable_name as name from information_schema.ins_stables where stable_name = '{name}' UNION select table_name as name from information_schema.ins_tables where table_name = '{name}';"
            tdLog.info(f"check output table SQL: {sql}")
            tdSql.checkResultsByFunc(
                sql,
                func=lambda: tdSql.getRows() > 0,
            )

            # check the output
            sql = f"SELECT * FROM `{self.vdb}`.`{name}`;"
            tdLog.info(f"check output SQL: {sql}")

            def assert_func():
                output = tdSql.getResult(sql)
                tdLog.info(f"output: {output}")
                for a in obj.assert_list:
                    if a.row >= len(output):
                        tdLog.error(
                            f"assert failed: out of boundary, row: {a.row}, col: {a.col}, output rows: {len(output)}"
                        )
                        return False
                    if a.col >= len(output[a.row]):
                        tdLog.error(
                            f"assert failed: out of boundary, row: {a.row}, col: {a.col}, output cols: {len(output[a.row])}"
                        )
                        return False
                    actual = output[a.row][a.col]
                    if not values_equal(a.data, actual):
                        tdLog.error(
                            f"assert failed: not equal, row: {a.row}, col: {a.col}, expect: {a.data}, actual: {actual}"
                        )
                        return False
                return True

            def values_equal(expected, actual, rel_tol=1e-6, abs_tol=1e-8):
                # compare NULL if actual is None
                if actual is None:
                    return str(expected) == "NULL"
                # use math.isclose whene actual is float
                if isinstance(actual, float):
                    try:
                        return math.isclose(
                            float(expected), actual, rel_tol=rel_tol, abs_tol=abs_tol
                        )
                    except Exception:
                        return False
                # use string comparison otherwise
                return str(expected) == str(actual)

            retry_count = (
                self.assert_retry
                if hasattr(self, "assert_retry") and self.assert_retry > 0
                else obj.retry
            )
            tdLog.info(f"retry count: {retry_count}")
            tdSql.checkResultsByFunc(sql, func=assert_func, retry=retry_count)

        tdLog.info("verify results done")


class StreamObj:
    def __init__(
        self,
        id,
        name,
        create,
        data=None,
        interval=None,
        assert_list=None,
        assert_retry=60,
    ):
        self.id = id
        self.name = name
        self.create = create
        self.data = data
        self.interval = interval
        self.assert_list = assert_list if assert_list else []
        self.retry = assert_retry

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
            assert_retry=d.get("assert_retry", 60),
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
