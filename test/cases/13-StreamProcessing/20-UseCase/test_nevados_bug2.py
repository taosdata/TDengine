import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream
from datetime import datetime
from datetime import date


class Test_Nevados:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_usecase_nevados(self):
        """Nevados

        Refer: https://taosdata.feishu.cn/wiki/XaqbweV96iZVRnkgHLJcx2ZCnQf

        Catalog:
            - Streams:UseCases

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-6-16 Simon Guan Created

        """

        tdStream.createSnode()
        self.prepare()
        self.windspeeds_hourly()

    def prepare(self):
        db = "dev"
        stb = "windspeeds"
        precision = "ms"
        start = "2025-06-01 00:00:00"
        interval = 30
        tbBatch = 1
        tbPerBatch = 10
        rowBatch = 1
        rowsPerBatch = 1000

        # start = (
        #     datetime.now()
        #     .replace(hour=0, minute=0, second=0, microsecond=0)
        #     .strftime("%Y-%m-%d %H:%M:%S")
        # )
        dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")

        if precision == "us":
            prec = 1000 * 1000 * 1000
        elif precision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsInterval = interval * prec
        tdLog.info(f"start={start} tsStart={tsStart}")

        tdLog.info(f"create database {db}")
        tdSql.prepare(dbname=db)

        tdLog.info(f"create super table f{stb}")
        tdSql.execute(
            f"create table {stb}("
            "    _ts TIMESTAMP,"
            "    speed DOUBLE,"
            "    direction DOUBLE"
            ") tags("
            "    id NCHAR(16),"
            "    site NCHAR(16),"
            "    tracker NCHAR(16),"
            "    zone NCHAR(16)"
            ")"
        )

        totalTables = tbBatch * tbPerBatch
        tdLog.info(f"create total {totalTables} child tables")
        for batch in range(tbBatch):
            sql = "create table "
            for tb in range(tbPerBatch):
                table = batch * tbPerBatch + tb
                id = f"id_{table}"
                site = f"site_{table}"
                tracker = f"tracker_{table}"
                zone = f"zone_{table}"
                sql += f"{db}.t{table} using {db}.{stb} tags('{id}', '{site}', '{tracker}', '{zone}')"
            tdSql.execute(sql)

        totalRows = rowsPerBatch * rowBatch
        tdLog.info(f"write {totalRows} rows per table")
        for table in range(totalTables):
            for batch in range(rowBatch):
                sql = f"insert into {db}.t{table} values "
                for row in range(rowsPerBatch):
                    if row >= 100 and row < 400:
                        continue
                    rows = batch * rowsPerBatch + row
                    ts = tsStart + rows * tsInterval
                    speed = rows
                    direction = rows * 2.0 if rows % 100 < 60 else rows * 0.5
                    sql += f"({ts}, {speed}, {direction}) "
                tdSql.execute(sql)

    def windspeeds_hourly(self):
        tdLog.info(f"create stream windspeeds_hourly")
        tdSql.execute(
            "create stream `windspeeds_hourly`"
            "  interval(1h) sliding(1h)"
            "  from windspeeds"
            "  partition by site, id"
            "  options(fill_history('2025-06-01 00:00:00') | pre_filter(_ts >= '2025-05-07'))"
            "  into `windspeeds_hourly`"
            "  tags("
            "    group_id bigint as _tgrpid"
            "  )"
            "  as select _twstart, _twend as window_hourly, %%1 as site, %%2 as id, max(speed) as windspeed_hourly_maximum from %%trows"
        )

        tdSql.checkTableSchema(
            dbname="dev",
            tbname="windspeeds_hourly",
            schema=[
                ["_twstart", "TIMESTAMP", 8, ""],
                ["window_hourly", "TIMESTAMP", 8, ""],
                ["site", "NCHAR", 16, ""],
                ["id", "NCHAR", 16, ""],
                ["windspeed_hourly_maximum", "DOUBLE", 8, ""],
                ["group_id", "BIGINT", 8, "TAG"],
            ],
            retry=10,
        )

        tdLog.info(f"create stream windspeeds_hourly_2")
        tdSql.execute(
            "create stream `windspeeds_hourly_2`"
            "  interval(1h) sliding(1h)"
            "  from windspeeds"
            "  partition by site, id"
            "  options(fill_history('2025-06-01 00:00:00') | pre_filter(_ts >= '2025-05-07'))"
            "  into `windspeeds_hourly_2`"
            "  as select _twstart, _twend as window_hourly, max(speed) as windspeed_hourly_maximum from %%trows"
        )

        tdSql.checkTableSchema(
            dbname="dev",
            tbname="windspeeds_hourly_2",
            schema=[
                ["_twstart", "TIMESTAMP", 8, ""],
                ["window_hourly", "TIMESTAMP", 8, ""],
                ["windspeed_hourly_maximum", "DOUBLE", 8, ""],
                ["site", "NCHAR", 16, "TAG"],
                ["id", "NCHAR", 16, "TAG"],
            ],
            retry=10,
        )
