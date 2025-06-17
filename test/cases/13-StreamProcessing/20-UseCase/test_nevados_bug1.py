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
        # self.windspeeds_hourly()
        self.kpi_db_test()

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
            "    id NCHAR(8),"
            "    site NCHAR(8)"
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
                sql += f"{db}.t{table} using {db}.{stb} tags('{id}', '{site}')"
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
                    direction = rows / 10.0 if rows % 100 < 60 else rows / 5.0
                    sql += f"({ts}, {speed}, {direction}) "
                tdSql.execute(sql)

    def windspeeds_hourly(self):
        # create stream windspeeds_hourly fill_history 1 into windspeeds_hourly as select _wend as window_hourly, site, id, max(speed) as windspeed_hourly_maximum from windspeeds where _ts >= '2025-05-07' partition by site, id interval(1h);
        tdSql.execute(
            "create stream `windspeeds_hourly`"
            "  interval(1h) sliding(1h)"
            "  from windspeeds"
            "  partition by site, id"
            "  options(fill_history('2025-06-01 00:00:00') | pre_filter(_ts >= '2025-05-07'))"
            "  into `windspeeds_hourly`"
            # "  tags("
            # "    group_id bigint as _tgrpid"
            # "  )"
            "  as select _twstart, _twend as window_hourly, max(speed) as windspeed_hourly_maximum from %%trows"
            # "  as select _twstart, _twend as window_hourly, %%1 as site, %%2 as id, max(speed) as windspeed_hourly_maximum from %%trows"
        )

    def kpi_db_test(self):
        # create stream if not exists kpi_db_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_db_test as select _wend as window_end, case when last(_ts) is not null then 1 else 0 end as db_online from trackers where _ts >= '2024-10-04T00:00:00.000Z' interval(1h) sliding(1h);
        tdSql.execute(
            "create stream `kpi_db_test`"
            "  interval(1h) sliding(1h)"
            "  from windspeeds"
            "  options(fill_history('2025-06-01 00:00:00') | watermark(10m) | ignore_disorder | pre_filter(_ts >= '2024-10-04T00:00:00.000Z'))"
            "  into `kpi_db_test`"
            "  as select _twstart, _twend as window_end, case when last(_ts) is not null then 1 else 0 end as db_online, count(*) from windspeeds where _ts >= _twstart and _ts <_twend"
        )

        sql = "select * from dev.kpi_db_test;"
        exp_sql = "select tw, te, case when tl is not null then 1 else 0 end as db_online, tc from(select _wstart tw, _wend te, last(_ts) tl, count(*) tc  from windspeeds where _ts >= '2025-06-01 00:00:00.000' and _ts < '2025-06-01 09:00:00.000' interval(1h) fill(null));"
        tdSql.checkResultsBySql(sql=sql, exp_sql=exp_sql)
