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
        self.windspeeds_daily()

    def prepare(self):
        db = "dev"
        stb = "windspeeds"
        precision = "ms"
        start = "2025-06-01 00:00:00"
        interval = 150
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
        tdLog.info("windspeeds_hourly")
        # create stream windspeeds_hourly fill_history 1 into windspeeds_hourly as select _wend as window_hourly, site, id, max(speed) as windspeed_hourly_maximum from windspeeds where _ts >= '2025-05-07' partition by site, id interval(1h);
        tdSql.execute(
            "create stream `windspeeds_hourly`"
            "  interval(1h) sliding(1h)"
            "  from windspeeds"
            "  partition by site, id"
            "  options(fill_history('2025-06-01 00:00:00') | pre_filter(_ts >= '2025-05-07'))"
            "  into `windspeeds_hourly`"
            "  as select _twstart window_start, _twend as window_hourly, max(speed) as windspeed_hourly_maximum from %%trows"
        )

        tdSql.checkTableSchema(
            dbname="dev",
            tbname="windspeeds_hourly",
            schema=[
                ["window_start", "TIMESTAMP", 8, ""],
                ["window_hourly", "TIMESTAMP", 8, ""],
                ["windspeed_hourly_maximum", "DOUBLE", 8, ""],
                ["site", "NCHAR", 16, "TAG"],
                ["id", "NCHAR", 16, "TAG"],
            ],
        )

        tdSql.checkResultsByFunc(
            "select count(*) from information_schema.ins_tables where db_name='dev' and stable_name='windspeeds_hourly';",
            lambda: tdSql.compareData(0, 0, 10),
        )

        sql = "select window_start, window_hourly, site, id, windspeed_hourly_maximum from dev.windspeeds_hourly where id='id_1';"
        exp_sql = "select _wstart, _wend, site, id, max(speed) from t1 interval(1h);"
        tdSql.checkResultsBySql(sql=sql, exp_sql=exp_sql)

        sql = "select count(*) from dev.windspeeds_hourly;"
        exp_sql = "select count(*) from (select _wstart, _wend, site, id, max(speed) from windspeeds partition by tbname interval(1h));"
        tdSql.checkResultsBySql(sql=sql, exp_sql=exp_sql)

    def windspeeds_daily(self):
        tdLog.info("windspeeds_daily")
        # create stream windspeeds_daily fill_history 1 into windspeeds_daily as select _wend as window_daily, site, id, max(windspeed_hourly_maximum) as windspeed_daily_maximum from windspeeds_hourly partition by site, id interval(1d, 5h);
        tdSql.execute(
            "create stream `windspeeds_daily`"
            "  interval(1d, 5h) sliding(1d)"
            "  from windspeeds_hourly"
            "  partition by site, id"
            "  options(fill_history('2025-06-01 00:00:00'))"
            "  into `windspeeds_daily`"
            "  tags("
            "    group_id bigint as _tgrpid"
            "  )"
            "  as select _twstart window_start, _twend as window_hourly, max(windspeed_hourly_maximum) as windspeed_daily_maximum, %%1 as site, %%2 as id from %%trows"
        )
        
        tdSql.checkTableSchema(
            dbname="dev",
            tbname="windspeeds_daily",
            schema=[
                ["window_start", "TIMESTAMP", 8, ""],
                ["window_hourly", "TIMESTAMP", 8, ""],
                ["windspeed_hourly_maximum", "DOUBLE", 8, ""],
                ["site", "NCHAR", 16, ""],
                ["id", "NCHAR", 16, ""],
                ["group_id", "BIGINT", 8, "TAG"],
            ],
        )

        tdSql.checkResultsByFunc(
            "select count(*) from information_schema.ins_tables where db_name='dev' and stable_name='windspeeds_daily';",
            lambda: tdSql.compareData(0, 0, 10),
        )
