import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream
from datetime import datetime
from datetime import date


class Test_Three_Gorges_Phase1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_dev_basic(self):
        """Three Gorges Info Dept Phase 1

        1. System-Level Alarm
        2. Converter-Level Alarm
        3. Battery Cluster  Alarm
        4. Centralized Control Alarm
        5. Predicted Data

        Catalog:
            - Streams:UseCases

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-6-12 Simon Guan Created

        """

        db = "ctg_tsdb"
        db2 = "ctg_test"
        stb = "stb_sxny_cn"
        precision = ("ms",)
        start = "2025-01-01 00:00:00"
        interval = 30
        tbBatch = 1
        tbPerBatch = 100
        rowBatch = 1
        rowsPerBatch = 1000

        start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        
        if precision == "us":
            prec = 1000 * 1000 * 1000
        elif precision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsNext = tsStart + 86400 * prec
        tsInterval = interval * prec
        tdLog.info(f"start={start} tsStart={tsStart}")

        tdLog.info(f"create database f{db} f{db2}")
        tdSql.prepare(dbname=db2)
        tdSql.prepare(dbname=db)

        tdLog.info(f"create super table f{stb}")
        tdSql.execute(
            f"create table {stb}("
            "    dt TIMESTAMP,"
            "    val DOUBLE,"
            "    rows int"
            ") tags("
            "    point VARCHAR(50), "
            "    point_name VARCHAR(64), "
            "    point_path VARCHAR(64), "
            "    index_name VARCHAR(64), "
            "    country_equipment_code VARCHAR(64), "
            "    index_code VARCHAR(64), "
            "    ps_code VARCHAR(50), "
            "    cnstationno VARCHAR(255), "
            "    cz_flag VARCHAR(255), "
            "    blq_flag VARCHAR(255), "
            "    dcc_flag VARCHAR(255)"
            ")"
        )

        totalTables = tbBatch * tbPerBatch
        tdLog.info(f"create total {totalTables} child tables")
        for batch in range(tbBatch):
            sql = "create table "
            for tb in range(tbPerBatch):
                table = batch * tbPerBatch + tb
                point = f"point_{table}"
                point_name = f"point_name_{table}"
                point_path = f"point_path_{table}"
                index_name = f"index_name_{table}"
                country_equipment_code = f"country_equipment_code_{table}"
                index_code = "emstxyc" if table % 3 == 1 else "bmstxyc"
                ps_code = f"ps_code_{table}"
                cnstationno = f"cnstationno_{table}"
                cz_flag = 1 if table % 2 == 1 else 2
                blq_flag = f"blq_flag_{table}"
                dcc_flag = f"dcc_flag_{table}"
                sql += f"{db}.t{table} using {db}.{stb} tags('{point}', '{point_name}', '{point_path}', '{index_name}', '{country_equipment_code}', '{index_code}', '{ps_code}', '{cnstationno}', '{cz_flag}', '{blq_flag}', '{dcc_flag}') "
            tdSql.execute(sql)

        totalRows = rowsPerBatch * rowBatch
        tdLog.info(f"write total:{totalRows} rows, {rowsPerBatch} rows per table")
        for table in range(totalTables):
            for batch in range(rowBatch):
                sql = f"insert into {db}.t{table} values "
                for row in range(rowsPerBatch):
                    rows = batch * rowsPerBatch + row
                    dt = tsStart + rows * tsInterval
                    val = 0 if rows % 100 < 60 else 1
                    sql += f"({dt}, {val}, {rows}) "
                tdSql.execute(sql)

        # old stream

        # create stream `str_station_alarmmsg_systemalarm_test` trigger at_once ignore expired 0 fill_history 1 into `ctg_test`.`str_station_alarmmsg_systemalarm_test` tags(
        #     cnstationno varchar(255),
        #     gpstationno varchar(255),
        #     alarmmsg varchar(255),
        #     alarmid varchar(255),
        #     alarmtype varchar(255),
        #     alarmcontent varchar(255)
        # ) subtable(
        #     concat('station_alarmmsg_systemalarm_test_', alarmid)
        # ) as
        # select
        #     first(dt) alarmdate,
        #     val alarmstatus
        # from
        #     ctg_tsdb.stb_sxny_cn t
        # where
        #     index_code in ('emstxyc', 'bmstxyc')
        #     and dt >= today() - 1d
        #     and cz_flag = 1
        #     partition by tbname,
        #     cnstationno,
        #     ps_code as gpstationno,
        #     index_name as alarmmsg,
        #     concat_ws('_', cnstationno, country_equipment_code, point) as alarmid,
        #     (
        #         case
        #             when index_code = 'emstxyc' then '01'
        #             when index_code = 'bmstxyc' then '02'
        #         end
        #     ) as alarmtype,
        #     point_path as alarmcontent state_window(cast(val as int)) ;

        tdLog.info(f"create streams")
        tdSql.execute(
            "create stream `str_station_alarmmsg_systemalarm_test`"
            "  state_window(cast(val as int))"
            "  from ctg_tsdb.stb_sxny_cn"
            "  partition by tbname,"
            "    cnstationno,"
            "    ps_code as gpstationno,"
            "    index_name as alarmmsg,"
            "    concat_ws('_', cnstationno, country_equipment_code, point) as alarmid,"
            "    ("
            "      case"
            "        when index_code = 'emstxyc' then '01'"
            "        when index_code = 'bmstxyc' then '02'"
            "      end"
            "    ) as alarmtype,"
            "    point_path as alarmcontent"
            f" options( fill_history({start}) | pre_filter(index_code in ('emstxyc', 'bmstxyc') and dt >= today() - 1d and cz_flag = 1) )"
            f" into `ctg_test`.`str_station_alarmmsg_systemalarm_test` tags("
            "    cnstationno varchar(255) as %%2,"
            "    gpstationno varchar(255) as %%3,"
            "    alarmmsg varchar(255) as %%4,"
            "    alarmid varchar(255) as %%5,"
            "    alarmtype varchar(255) as %%6,"
            "    alarmcontent varchar(255) as %%7"
            ") output_subtable("
            "    concat('station_alarmmsg_systemalarm_test_', alarmid)"
            ") as "
            "  select "
            "    _twstart alarmdate,"
            "    first(val) alarmstatus"
            "  from"
            "    %%trows;",
            show=True,
        )
