import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream
from datetime import datetime
from datetime import date


class Test_Three_Gorges_Phase1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_usecase_3gorges_1(self):
        """Three Gorges Info Dept Phase

        Refer: https://taosdata.feishu.cn/wiki/ATWQwcOAviZfWikU69WcNAbTndc
        1. System-level Alarms
        2. Converter-level Alarms
        3. Battery Cluster Alarms
        4. Centralized Control Data
        5. Predictive Data

        Since: v3.3.3.7

        Labels: common,ci,skip

        Jira: None

        History:
            - 2025-6-12 Simon Guan Created

        """

        tdStream.createSnode()

        db = "ctg_tsdb"
        db2 = "ctg_test"
        stb = "stb_sxny_cn"
        precision = ("ms",)
        start = "2025-01-01 00:00:00"
        interval = 30
        tbBatch = 1
        tbPerBatch = 10
        rowBatch = 1
        rowsPerBatch = 1000

        start = (
            datetime.now()
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .strftime("%Y-%m-%d %H:%M:%S")
        )
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

        tdLog.info(f"create database {db} {db2}")
        tdSql.prepare(dbname=db2)
        tdSql.prepare(dbname=db)

        tdLog.info(f"create super table f{stb}")
        tdSql.execute(
            f"create table {stb}("
            "    dt TIMESTAMP,"
            "    val DOUBLE,"
            "    rows INT"
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

        tdLog.info(f"基础计算(crash) ")
        tdSql.execute(
            "create stream `basic_stream`"
            "  interval(1d) sliding(1d)"
            "  from ctg_tsdb.stb_sxny_cn"
            "  partition by tbname"
            "  stream_options(fill_history('2025-06-01 00:00:00'))"
            "  into `ctg_test`.`basic_stream`"
            "as select _twstart tw, first(dt) alarmdate, first(val) alarmstatus from %%trows;"
        )

        tdSql.checkTableSchema(
            dbname="ctg_test",
            tbname="basic_stream",
            schema=[
                ["tw", "TIMESTAMP", 8, ""],
                ["alarmdate", "TIMESTAMP", 8, ""],
                ["alarmstatus", "DOUBLE", 8, ""],
                ["tag_tbname", "VARCHAR", 270, "TAG"],
            ],
        )

        tdSql.checkResultsByFunc(
            "select count(*) from information_schema.ins_tables where db_name='ctg_test' and stable_name='basic_stream';",
            func=lambda: tdSql.compareData(0, 0, 10),
        )

        tdSql.checkResultsBySql(
            sql="select * from ctg_test.basic_stream where tag_tbname='t1';",
            exp_sql="select dt, dt, val, tbname from ctg_tsdb.t1 order by dt asc limit 1;",
            retry=3
        )

        return

        tdLog.info(f"系统级报警 ")
        # tdSql.execute(
        #     "create stream `str_station_alarmmsg_systemalarm_test`"
        #     "  state_window(cast(val as int))"
        #     "  from ctg_tsdb.stb_sxny_cn"
        #     "  partition by tbname, cnstationno, ps_code, index_name, country_equipment_code, point, index_code, point_path"
        #     " stream_options(fill_history('2025-06-01 00:00:00') | pre_filter(index_code in ('emstxyc', 'bmstxyc') and dt >= today() - 1d and cz_flag = 1))"
        #     " into `ctg_test`.`str_station_alarmmsg_systemalarm_test`"
        #     "  output_subtable(concat_ws('_', 'station_alarmmsg_systemalarm_test', %%2, %%5, %%6))"
        #     "  tags(cnstationno varchar(255) as %%2,"
        #     "    gpstationno varchar(255) as %%3,"
        #     "    alarmmsg varchar(255) as %%4,"
        #     "    alarmid varchar(255) as concat_ws('_', %%2, %%5, %%6),"
        #     "    alarmtype varchar(255) as (case when %%7 = 'emstxyc' then '01' when %%7 = 'bmstxyc' then '02' end),"
        #     "    alarmcontent varchar(255) as %%8"
        #     "  )"
        #     "  as select _twstart alarmdate, first(val) alarmstatus from %%trows;"
        # )

        # tdSql.checkTableSchema(
        #     dbname="ctg_test",
        #     tbname="str_station_alarmmsg_systemalarm_test",
        #     schema=[
        #         ["alarmdate", "TIMESTAMP", 8, ""],
        #         ["alarmstatus", "DOUBLE", 8, ""],
        #         ["cnstationno", "VARCHAR", 255, "TAG"],
        #         ["gpstationno", "VARCHAR", 255, "TAG"],
        #         ["alarmmsg", "VARCHAR", 255, "TAG"],
        #         ["alarmid", "VARCHAR", 255, "TAG"],
        #         ["alarmtype", "VARCHAR", 255, "TAG"],
        #         ["alarmcontent", "VARCHAR", 255, "TAG"],
        #     ],
        # )

        # tdSql.checkResultsByFunc(
        #     sql="select * from information_schema.ins_streams where db_name='ctg_tsdb' and stream_name='str_station_alarmmsg_systemalarm_test';",
        #     func=lambda: tdSql.getRows() == 1,
        # )

        # tdSql.checkResultsByFunc(
        #     sql="select alarmdate, alarmstatus, cnstationno, gpstationno, alarmmsg, alarmid, alarmtype, alarmcontent from ctg_test.str_station_alarmmsg_systemalarm_test;",
        #     func=lambda: tdSql.getRows() > 0,
        # )

        # tdSql.checkResultsBySql(
        #     sql="select alarmdate, alarmstatus, cnstationno, gpstationno, alarmmsg, alarmid, alarmtype, alarmcontent from ctg_test.str_station_alarmmsg_systemalarm_test;",
        #     exp_sql="select "
        #     "  _wstart as alarmdate, "
        #     "  first(val) as alarmstatus, "
        #     "  cnstationno,  "
        #     "  ps_code as gpstationno,  "
        #     "  index_name as alarmmsg,  "
        #     "  concat_ws('_', cnstationno, country_equipment_code, point) as alarmid,  "
        #     "  (case when index_code = 'emstxyc' then '01' when index_code = 'bmstxyc' then '02' end) as alarmtype,  "
        #     "  point_path as alarmcontent "
        #     "from ctg_tsdb.stb_sxny_cn  "
        #     f"where index_code in ('emstxyc', 'bmstxyc') and dt >= today() - 1d and cz_flag = 1 and dt >= '{start}'  "
        #     "partition by tbname, cnstationno, ps_code, index_name, country_equipment_code, point, index_code, point_path "
        #     "state_window(cast(val as int)); ",
        # )

        return

        tdLog.info("变流器级报警")

        tdSql.execute(
            "create stream `str_station_alarmmsg_inverteralarm_test`"
            "  state_window(cast(val as int))"
            "  from ctg_tsdb.stb_sxny_cn"
            "  partition by tbname, cnstationno, ps_code, country_equipment_code, index_name, point, index_code, point_path"
            f" stream_options(fill_history('{start}') | pre_filter(1=1 and index_code in ('gzztj', 'dygz', 'igbtzjgz', 'jldlglyjbj','zldlglyjbj', 'flgz', 'gwtj') and dt >= today() - 1d and blq_flag = 1) )"
            f" into `ctg_test`.`str_station_alarmmsg_inverteralarm_test` "
            "  output_subtable(concat_ws('_', 'station_alarmmsg_inverteralarm_test_', %%2, %%4, %%4, %%6)) "
            "  tags("
            "    cnstationno varchar(255) as %%2,"
            "    gpstationno varchar(255) as %%3,"
            "    cninverterno varchar(255) as %%4,"
            "    gpinverterno varchar(255) as %%4,"
            "    alarmmsg varchar(255) as %%5,"
            "    alarmid varchar(255) as concat_ws('_', %%2, %%4, %%4, %%6),"
            "    alarmtype varchar(255) as (case"
            "        when %%7 = 'emstxyc' then '01'"
            "        when %%7 = 'bmstxyc' then '02'"
            "        when %%7 = 'igbtzjgz' then '03'"
            "        when %%7 = 'jldlglyjbj' then '04'"
            "        when %%7 = 'zldlglyjbj' then '05'"
            "        when %%7 = 'flgz1' then '06'"
            "        when %%7 = 'flgz2' then '06'"
            "        when %%7 = 'flgz' then '06'"
            "        when %%7 = 'gwtj' then '07'"
            "      end),"
            "    alarmcontent varchar(255) as %%8"
            "  ) "
            "  as select _twstart tw, to_char(first(dt), 'yyyy-mm-dd hh24:mi:ss') alarmdate, first(val) alarmstatus from %%trows;",
            show=True,
        )

        tdLog.info("电池簇报警")
        tdSql.execute(
            "create stream `str_station_alarmmsg_batteryclusteralarm_test`"
            "  state_window(cast(val as int))"
            "  from ctg_tsdb.stb_sxny_cn"
            "  partition by tbname, cnstationno, ps_code, country_equipment_code, index_name, point, index_code, point_path"
            f" stream_options(fill_history('{start}') | pre_filter(1=1 and index_code in ('dyyjyxbj', 'cd_dljcyjyxbj', 'fd_dljcyjyxbj', 'dcccfdhly')  and ps_code <> '2149' and dt >= today() - 1d and val > 0) )"
            f" into `ctg_test`.`str_station_alarmmsg_batteryclusteralarm_test` "
            "  output_subtable(concat_ws('_', 'station_alarmmsg_batteryclusteralarm_test_', %%2, %%4, %%4, %%6)) "
            "  tags("
            "    cnstationno varchar(255) as %%2,"
            "    gpstationno varchar(255) as %%3,"
            "    cninverterno varchar(255) as %%4,"
            "    gpinverterno varchar(255) as %%4,"
            "    alarmmsg varchar(255) as %%5,"
            "    alarmid varchar(255) as concat_ws('_', %%2, %%4, %%4, %%6),"
            "    alarmtype varchar(255) as (case"
            "        when %%7 = 'dyyjyxbj' then '01'"
            "        when %%7 = 'cd_dljcyjyxbj' then '02'"
            "        when %%7 = 'fd_dljcyjyxbj' then '03'"
            "        when %%7 = 'fd_dljcyjyxbj' then '06'"
            "      end),"
            "    alarmcontent varchar(255) as %%8"
            "  ) "
            "  as select _twstart tw, first(dt) alarmdate, val alarmstatus from %%trows;",
            show=True,
        )

        tdSql.execute(
            "CREATE STABLE stb_cjdl_point_data (ts TIMESTAMP, st DOUBLE, val DOUBLE) TAGS (id VARCHAR(20), senid VARCHAR(255), senid_name VARCHAR(255), tag_temp VARCHAR(255))"
        )

        tdLog.info("集控数据")
        tdSql.execute(
            "create stream `str_cjdl_point_data_szls_jk_test`"
            "  interval(1m) sliding(1m)"
            "  from ctg_tsdb.stb_cjdl_point_data"
            "  partition by tbname, senid, senid_name"
            f" stream_options(expired_time(0s) | fill_history('{start}') | pre_filter(tag_temp = 'A001') | max_delay(3s))"
            f" into `ctg_test`.`stb_cjdl_point_data_szls_jk_test`"
            "  output_subtable( concat('cjdl_point_data_szls_jk_test_', %%2))"
            "  tags("
            "    tname varchar(255) as %%1, "
            "    senid varchar(255) as %%2,"
            "    senid_name varchar(255) as %%3"
            "  )"
            " as select last(ts) as ts, last(val) as val from %%trows;",
            show=True,
        )

        tdLog.info("预测数据")
        tdSql.execute(
            "create stream `str_cjdl_point_data_szls_yc_test`"
            "  interval(1a) sliding(1a)"
            "  from ctg_tsdb.stb_cjdl_point_data"
            "  partition by tbname, senid, senid_name"
            f" stream_options(expired_time(0s) | fill_history('{start}') | pre_filter(tag_temp = 'TEMP03'))"
            f" into `ctg_test`.`stb_cjdl_point_data_szls_yc_test`"
            "  output_subtable(concat('cjdl_point_data_szls_yc_test_', %%2))"
            "  tags("
            "    senid varchar(255) as %%2,"
            "    senid_name varchar(255) as %%3"
            "  )"
            "  as select last(ts) as ts, last(val) as val from %%trows;",
            show=True,
        )
