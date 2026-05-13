import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream
from datetime import datetime
from datetime import date


class Test_Three_Gorges_Phase3:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_usecase_3gorges_3(self):
        """Three Gorges Info Dept Phase 3

        Refer: https://taosdata.feishu.cn/wiki/XaqbweV96iZVRnkgHLJcx2ZCnQf


        Since: v3.3.3.7

        Labels: common,ci,skip

        Jira: None

        History:
            - 2025-6-16 Simon Guan Created

        """

        tdStream.createSnode()
        self.prepare()
        # self.step1()
        # self.step2()
        # self.step3()
        # self.step4()
        # self.step5()
        # self.step6()
        # self.step7()
        # self.step8()
        # self.step9()
        # self.step10()
        # self.step11()
        # self.step12()
        # self.step13()
        # self.step14()
        # self.step15()
        # self.step16()
        # self.step17()

    def prepare(self):
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
            "    ps_name VARCHAR(50), "
            "    cnstationno VARCHAR(255), "
            "    cz_flag VARCHAR(255), "
            "    blq_flag VARCHAR(255), "
            "    dcc_flag VARCHAR(255)"
            ")"
        )

        # totalTables = tbBatch * tbPerBatch
        # tdLog.info(f"create total {totalTables} child tables")
        # for batch in range(tbBatch):
        #     sql = "create table "
        #     for tb in range(tbPerBatch):
        #         table = batch * tbPerBatch + tb
        #         point = f"point_{table}"
        #         point_name = f"point_name_{table}"
        #         point_path = f"point_path_{table}"
        #         index_name = f"index_name_{table}"
        #         country_equipment_code = f"country_equipment_code_{table}"
        #         index_code = "emstxyc" if table % 3 == 1 else "bmstxyc"
        #         ps_code = f"ps_code_{table}"
        #         ps_name = f"ps_name_{table}"
        #         cnstationno = f"cnstationno_{table}"
        #         cz_flag = 1 if table % 2 == 1 else 2
        #         blq_flag = f"blq_flag_{table}"
        #         dcc_flag = f"dcc_flag_{table}"
        #         sql += f"{db}.t{table} using {db}.{stb} tags('{point}', '{point_name}', '{point_path}', '{index_name}', '{country_equipment_code}', '{index_code}', '{ps_code}', '{ps_name}', '{cnstationno}', '{cz_flag}', '{blq_flag}', '{dcc_flag}') "  # type: ignore
        #     tdSql.execute(sql)

        # totalRows = rowsPerBatch * rowBatch
        # tdLog.info(f"write total:{totalRows} rows, {rowsPerBatch} rows per table")
        # for table in range(totalTables):
        #     for batch in range(rowBatch):
        #         sql = f"insert into {db}.t{table} values "
        #         for row in range(rowsPerBatch):
        #             rows = batch * rowsPerBatch + row
        #             dt = tsStart + rows * tsInterval
        #             val = 0 if rows % 100 < 60 else 1
        #             sql += f"({dt}, {val}, {rows}) "
        #         tdSql.execute(sql)

    def step1(self):
        tdSql.execute(
            "create stream `str_tb_station_power_info`"
            "  period(1d)"
            "  from ctg_tsdb.stb_sxny_cn"
            "  partition by tbname, company, ps_name, country_code, ps_code, rated_energy, rated_power_unit, data_unit, remark"
            "  stream_options((fill_history('2025-01-01 00:00:00'))"
            "  into `ctg_test`.`stb_station_power_info`"
            "  output_subtable(concat('station_power_info_', %%2))"
            "  tags(company varchar(255) as %%2,"
            "    ps_name varchar(255) as %%3,"
            "    country_code varchar(255) as %%4,"
            "    ps_code varchar(255) as %%5,"
            "    rated_energy varchar(255) as %%6,"
            "    rated_power_unit varchar(255) as %%7,"
            "    data_unit varchar(255) as %%8,"
            "    remark varchar(255) as %%9"
            "  )"
            "  as select today() ts, t.rated_power, t.minimum_power, t.data_rate from %%trows;"
        )

    def step2(self):
        tdSql.execute(
            "create stream `cjdl_cn_sbgjpt_stationmsg_cnstationstatus_bj_tp`"
            "  interval(15m) sliding(15m)"
            "  from ctg_tsdb.stb_cjdl_cn"
            "  partition by tbname, ps_code, cnstationno, index_code, point"
            "  stream_options((max_delay(14m) | pre_filter(1 = 1 and index_code in ('jldlglyjbj', 'zldlglyjbj') and ts >= to_char(today(), 'yyyy-mm-dd') and dcc_flag = 1))"
            "  into `ctg_test`.`stb_cjdl_cn_sbgjpt_stationmsg_cnstationstatus_bj_tp`"
            "  output_subtable(concat('cjdl_cn_sbgjpt_stationmsg_cnstationstatus_bj_tp_', %%2))"
            "  tags(point varchar(255) as %%5,"
            "    ps_code varchar(255) as %%2,"
            "    cnstationno varchar(255) as %%3,"
            "    index_code varchar(255) as %%4"
            "  )"
            "  as select _twstart ts, to_char(last(ts), 'yyyy-mm-dd hh24:mi:ss.ms') dt, to_char(last(ts), 'yyyy-mm-dd hh24:mi:00') stat_date, last(cast(val as DOUBLE)) val from %%trows;"
        )

    def step3(self):
        tdSql.execute(
            "create stream `stb_cjdl_cn_sbgjpt_stationmsg_cnstationstatus_bj`"
            "  interval(15m) sliding(15m)"
            "  from ctg_tsdb.stb_cjdl_cn_sbgjpt_stationmsg_cnstationstatus_bj_tp"
            "  partition by tbname, ps_code, cnstationno, index_code, pointcode"
            "  stream_options((max_delay(14m) | pre_filter(1 = 1 and length(cnstationno) <> 0))"
            "  into `ctg_test`.`stb_cjdl_cn_sbgjpt_stationmsg_cnstationstatus_bj`"
            "  output_subtable(concat_ws('_', 'bj', %%2, %%3))"
            "  tags(point varchar(255) as %%5,"
            "    ps_code varchar(255) as %%2,"
            "    cnstationno varchar(255) as %%3,"
            "    index_code varchar(255) as %%4"
            "  )"
            "  as select ts, avg(val) val from %%trows;"
        )

    def step4(self):
        tdSql.execute(
            "create stream `stb_cjdl_cn_all_cz_yggl_base_tp`"
            "  interval(15m) sliding(15m)"
            "  from ctg_tsdb.stb_cjdl_cn"
            "  partition by tbname, point, ps_code, cnstationno"
            "  stream_options((max_delay(14m) | pre_filter(cz_flag = '1' and index_code = 'yggl' and ts >= today() )"
            "  into `ctg_test`.`stb_cjdl_cn_all_cz_yggl_base_tp`"
            "  output_subtable(concat_ws('_', 'stb_cjdl_cn_all_cz_yggl_base_tp', %%2, %%3))"
            "  tags(pointcode varchar(255) as %%4,"
            "    ps_code varchar(255) as %%2,"
            "    cnstationno varchar(255) as %%3"
            "  )"
            "  as select _wtstart ts, avg(cast(val as DOUBLE)) val from %%trows;"
        )

    def step5(self):
        tdSql.execute(
            "create stream `str_cjdl_cn_all_cz_yggl_base`"
            "  interval(15m) sliding(15m)"
            "  from ctg_tsdb.stb_cjdl_cn_all_cz_yggl_base_tp"
            "  partition by ps_code, cnstationno, ts"
            "  stream_options((max_delay(14m))"
            "  into `ctg_test`.`stb_cjdl_cn_all_cz_yggl_base`"
            "  output_subtable(concat_ws('_', 'cjdl_cz_yggl_base', ps_code, cnstationno))"
            "  tags(ps_code varchar(50) as %%1,"
            "    cnstationno varchar(50) as %%2"
            "  )"
            "  as select _twstart dt, sum(val) val from %%trows;"
        )

    def step6(self):
        tdSql.execute(
            "create stream `str_cjdl_cn_all_cz_zt_base_tp`"
            "  interval(1d) sliding(1d)"
            "  from ctg_tsdb.stb_cjdl_cn"
            "  partition by pointcode, ps_code, cnstationno"
            "  stream_options((max_delay(14m) | pre_filter(cz_flag = '1' and index_code = 'zt' and val > 0 and ts >= today()))"
            "  into `ctg_test`.`stb_cjdl_cn_all_cz_zt_base_tp`"
            "  output_subtable(concat('stb_cjdl_cn_all_cz_zt_base_tp', '_', %%2))"
            "  tags(pointcode varchar(50) as %%1,"
            "    ps_code varchar(50) as %%2"
            "    cnstationno varchar(50) as %%2"
            "  )"
            "  as select _twstart ts, avg(cast(val as DOUBLE)) val from %%trows;"
        )

    def step7(self):
        tdSql.execute(
            "create stream `str_cjdl_cn_all_cz_zt_base`"
            "  interval(15m) sliding(15m)"
            "  from ctg_tsdb.stb_cjdl_cn_all_cz_zt_base_tp"
            "  partition by ps_code, cnstationno"
            "  stream_options(max_delay(14m) | pre_filter(cz_flag = '1' and index_code = 'zt' and val > 0 and ts >= today()))"
            "  into `ctg_test`.`stb_cjdl_cn_all_cz_zt_base`"
            "  output_subtable(concat('stb_cjdl_cn_all_cz_zt_base', '_', %%2))"
            "  tags(ps_code varchar(50) as %%2"
            "    cnstationno varchar(50) as %%2"
            "  )"
            "  as select _twstart ts, sum(val) val from %%trows;"
        )

    def step8(self):
        tdSql.execute(
            "create stream `str_cjdl_cn_all_cz_zt_base_tp`"
            "  interval(15m) sliding(15m)"
            "  from ctg_tsdb.stb_sxny_cn"
            "  partition by tbname, point, ps_code, cnstationno, index_code"
            "  stream_options(max_delay(14m) | pre_filter(1=1 and index_code in ('jldlglyjbj', 'zldlglyjbj') and dt >= today() and dcc_flag =1))"
            "  into `ctg_test`.`stb_cjdl_cn_all_cz_zt_base_tp`"
            "  output_subtable(concat('stb_cjdl_cn_all_cz_zt_base_tp', '_', %%2))"
            "  tags(point varchar(255) as %%1,"
            "    ps_code varchar(255) as %%2"
            "    cnstationno varchar(255) as %%2"
            "    index_code varchar(255) as %%2"
            "  )"
            "  as select _twstart ts, last(val) val from %%trows;"
        )

    def step9(self):
        tdSql.execute(
            "create stream `str_sxny_cn_all_cz_zt_base`"
            "  interval(15m) sliding(15m)"
            "  from ctg_tsdb.stb_cjdl_cn_all_cz_zt_base_tp"
            "  partition by ps_code, cnstationno"
            "  stream_options(max_delay(14m) | pre_filter(1 = 1 and and length(cnstationno) <> 0))"
            "  into `ctg_test`.`stb_sxny_cn_all_cz_zt_base`"
            "  output_subtable(concat('stb_sxny_cn_all_cz_zt_base', '_', %%2))"
            "  tags(ps_code varchar(50) as %%2"
            "    cnstationno varchar(50) as %%2"
            "  )"
            "  as select _twstart ts, avg(val) val from %%trows;"
        )

    def step10(self):
        tdSql.execute(
            "create stream `str_sxny_cn_all_cz_yggl_base_tp`"
            "  interval(15m) sliding(15m)"
            "  from ctg_tsdb.stb_sxny_cn"
            "  partition by point, ps_code, cnstationno, index_code"
            "  stream_options(max_delay(14m) | pre_filter(cz_flag = '1' and index_code = yggl and dt >= today()))"
            "  into `ctg_test`.`stb_sxny_cn_all_cz_yggl_base_tp`"
            "  output_subtable(concat('stb_sxny_cn_all_cz_yggl_base_tp', '_', %%2))"
            "  tags(point varchar(255) as %%1,"
            "    ps_code varchar(255) as %%2"
            "    cnstationno varchar(255) as %%2"
            "  )"
            "  as select _twstart ts, last(val) val from %%trows;"
        )

    def step11(self):
        tdSql.execute(
            "create stream `str_sxny_cn_all_cz_yggl_base`"
            "  state_window(cast(val as varchar(1)))"
            "  from ctg_tsdb.stb_sxny_cn_all_cz_yggl_base_tp"
            "  partition by tbname, point, index_code, ps_code, ps_name"
            "  stream_options(max_delay(14m) | pre_filter(1 = 1 and index_code = 'gzzt' and dt >= today()))"
            "  into `ctg_test`.`stb_sxny_cn_all_cz_yggl_base`"
            "  output_subtable(concat('stb_sxny_cn_all_cz_yggl_base', '_', %%2))"
            "  tags(ps_code varchar(50) as %%4"
            "    cnstationno varchar(50) as %%5"
            "  )"
            "  as select _twstart dt, sum(val) val from %%trows;"
        )

    def step12(self):
        tdSql.execute(
            "create stream `str_sxny_cn_sbgjpt_stationmsg_cnstationstatus_zt_fir`"
            "  interval(15m) sliding(15m)"
            "  from ctg_tsdb.stb_sxny_cn"
            "  partition by ps_code"
            "  stream_options(max_delay(14m) | pre_filter(1 = 1 and dt >= today()))"
            "  into `ctg_test`.`stb_sxny_cn_sbgjpt_stationmsg_cnstationstatus_zt_fir`"
            "  output_subtable(concat_ws('_', 'zt_fir', %%2, %%3))"
            "  tags(ps_code varchar(255) as %%4"
            "    cnstationno varchar(255) as %%5"
            "  )"
            "  as select _twstart ts, case when last(val) in (1, 2) then 3 when last(val) = 3 then 1 when last(val) = 4 then 2 else 4 end val from %%trows;"
        )

    def step13(self):
        tdSql.execute(
            "create stream `str_sxny_cn_sbgjpt_stationmsg_cnstationstatus_zt_thi_base`"
            "  interval(15m) sliding(15m)"
            "  from ctg_tsdb.stb_sxny_cn"
            "  partition by ps_code, cnstationno"
            "  stream_options(max_delay(14m) | pre_filter(cz_flag = '1' and index_code = 'zt' and ps_code in ('2137') and t.dt >= today() ))"
            "  into `ctg_test`.`stb_sxny_cn_sbgjpt_stationmsg_cnstationstatus_zt_thi_base`"
            "  output_subtable(concat_ws('_', 'zt_thi_base', %%2, %%3))"
            "  tags(ps_code varchar(255) as %%4"
            "    cnstationno varchar(255) as %%5"
            "  )"
            "  as select _twstart ts, last(t.val) val from %%trows;"
        )

    def step14(self):
        tdSql.execute(
            "create stream `str_sxny_cn_power_all_cz_yggl`"
            "  interval(15m) sliding(15m)"
            "  from ctg_tsdb.stb_sxny_cn"
            "  partition by tbname, point, ps_code, cnstationno"
            "  stream_options(max_delay(14m) | fill_history('2025-01-01') | pre_filter(cz_flag = '1' and index_code = 'yggl' ))"
            "  into `ctg_test`.`stb_sxny_cn_all_cz_yggl`"
            "  output_subtable(concat(sxny_cn_all_cz_yggl', '_', %%2))"
            "  tags(point varchar(50) as %%2,"
            "    ps_code varchar(50) as %%3,"
            "    cnstationno varchar(50) as %%4"
            "  )"
            "  as select _twstart ts, avg(val) val from %%trows;"
        )

    def step15(self):
        tdSql.execute(
            "create stream `str_stb_sxny_cn_all_cz_yggl_base1`"
            "  interval(15m) sliding(15m)"
            "  from ctg_tsdb.stb_sxny_cn"
            "  partition by tbname, point, ps_code, cnstationno"
            "  stream_options(max_delay(14m) | fill_history('2025-01-01') | pre_filter(cz_flag = '1' and index_code = 'yggl' and dt >= today() ))"
            "  into `ctg_test`.`stb_sxny_cn_all_cz_yggl_base1`"
            "  output_subtable(concat(yggl_base1_', %%2))"
            "  tags(point varchar(255) as %%2,"
            "    ps_code varchar(255) as %%3,"
            "    cnstationno varchar(255) as %%4"
            "  )"
            "  as select _twstart ts, last(val) val from %%trows;"
        )

    def step16(self):
        tdLog.info("repeat")

    def step17(self):
        tdSql.execute(
            "create stream `str_stb_sxny_cn_all_cz_yggl_base`"
            "  interval(15m) sliding(15m)"
            "  from ctg_tsdb.stb_sxny_cn"
            "  partition by ps_code, cnstationno"
            "  stream_options(max_delay(15m) | pre_filter(cz_flag = '1' and index_code = 'zt' and ps_code in ('2137') and t.dt >= today() ))"
            "  into `ctg_test`.`stb_sxny_cn_all_cz_yggl_base1`"
            "  output_subtable(concat('yggl_base_', %%2, %%3))"
            "  tags(ps_code varchar(255) as %%4"
            "    cnstationno varchar(255) as %%5"
            "  )"
            "  as select _twstart ts, sum(val) val from %%trows;"
        )
