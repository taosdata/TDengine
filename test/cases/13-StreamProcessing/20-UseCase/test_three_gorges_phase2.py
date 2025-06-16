import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream
from datetime import datetime
from datetime import date


class Test_Three_Gorges_Phase2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_usecase_3gorges_2(self):
        """Three Gorges Info Dept Phase 2

        Refer: https://taosdata.feishu.cn/wiki/R014wmTQyi1Omck6NZwcUT0Cn7c
        1. 当日总充电量
        2. 当日总放电量
        3. 电站soh
        4. 当日最大\最小soe
        5. 电站有功功率
        6. 当日最大充电功率首次时间
        7. 当日最大放电功率首次时间
        8. 当日上网电量
        9. 当日下网电量
        10. 站用电量
        11. 充放电时长
        12. 当日故障运行时常
        13. 庆云储能电站预警报警
        14. 庆云储能电站总充电量/放电量
        15. 电站实时运行数据-view
        16. 电站运行日统计数据-view
        17. 定时补全当天数据
        18. 计算每个场站5分钟内平局功率（三峡能源）
        19. 自动补全5分钟机组状态（长江流域）
        20. 计算每个场站5分钟内平局功率（三峡能源）
        21. 计算每个场站5分钟内平局功率（湖北能源）
        22. 计算每个场站5分钟内平局功率（长江电力）
        23. 计算每个机组5分钟内平局功率（湖北能源）
        24. 计算每个测点15分钟内平局功率
        25. 自动补全5分钟机组状态
        26. 计算昨天到当前时间的每个状态开始时间和结束时间


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
        # self.step1()
        # self.step2()
        # self.step3()
        # # self.step4()
        # # self.step5()
        # # self.step6()
        # # self.step7()
        # # self.step8()
        # # self.step9()
        # # self.step10()
        # # self.step11()
        # # self.step12()
        # # self.step13()
        # # self.step14()
        # # self.step15()
        # # self.step16()
        # # self.step17()
        # # self.step18()
        # # self.step19()
        # # self.step20()
        # # self.step21()
        # # self.step22()
        # # self.step33()
        # # self.step34()
        # # self.step35()
        # # self.step36()

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
            "create stream `str_sxny_cn_drzcfd_test01`"
            "  period(1d)"
            "  from ctg_tsdb.stb_sxny_cn"
            "  partition by tbname, point, index_code, ps_code, ps_name"
            "  options(fill_history('2025-01-01 00:00:00') | max_delay(1s) | pre_filter(1 = 1 and index_code = 'drzcdl' and dt >= today() - 1d))"
            "  into `ctg_test`.`stb_sxny_cn_drzcfd_test01`"
            "  output_subtable(concat_ws('_', 'sxny_cn_drzcfd_test01', %%2))"
            "  tags(point varchar(255) as %%2,"
            "    index_code varchar(255) as %%3,"
            "    ps_code varchar(255) as %%4,"
            "    ps_name varchar(255) as %%5"
            "  )"
            "  as select _tlocaltime dt, first(val) fir_val, last(val) sec_val from %%trows;"
        )

        # wait stream created
        # tdSql.execute(
        #     "create stream `str_sxny_cn_drzcfd_test02`"
        #     "  interval(1d) sliding(1d)"
        #     "  from ctg_tsdb.stb_sxny_cn_drzcfd_test01"
        #     "  partition by index_code, ps_code"
        #     "  options(fill_history('2025-01-01 00:00:00'))"
        #     "  into `ctg_test`.`stb_sxny_cn_drzcfd_test02`"
        #     "  output_subtable(concat('sxny_cn_drzcfd_test02', '_', %%2))"
        #     "  tags(index_code varchar(50) as %%1,"
        #     "    ps_code varchar(50) as %%2"
        #     "  )"
        #     "  as select _twstart tw, to_char(_twstart, 'yyyy-mm-dd hh24:mi:ss.ms') dt, sum(sec_val - fir_val) val from %%trows;"
        # )

    def step2(self):
        tdSql.execute(
            "create stream `str_sxny_cn_drzfdl_test01`"
            "  period(1d)"
            "  from ctg_tsdb.stb_sxny_cn"
            "  partition by tbname, point, index_code, ps_code, ps_name"
            "  options(fill_history('2025-01-01 00:00:00') | max_delay(1s) | pre_filter(1 = 1 and index_code = 'drzfdl' and dt >= today() - 1d))"
            "  into `ctg_test`.`stb_sxny_cn_drzfdl_test01`"
            "  output_subtable(concat('sxny_cn_drzfdl_test01_', %%2))"
            "  tags(point varchar(255) as %%2,"
            "    index_code varchar(255) as %%3,"
            "    ps_code varchar(255) as %%4,"
            "    ps_name varchar(255) as %%5"
            "  )"
            "  as select _tlocaltime dt, first(val) fir_val, last(val) sec_val from %%trows;"
        )

        # wait stream created
        # tdSql.execute(
        #     "create stream `str_sxny_cn_drzfdl_test02`"
        #     "  interval(1d) sliding(1d)"
        #     "  from ctg_tsdb.stb_sxny_cn_drzfdl_test01"
        #     "  partition by index_code, ps_code"
        #     "  options(fill_history('2025-01-01 00:00:00'))"
        #     "  into `ctg_test`.`stb_sxny_cn_drzfdl_test02`"
        #     "  output_subtable(concat('sxny_cn_drzcfd_test02', '_', %%2))"
        #     "  tags(index_code varchar(50) as %%1,"
        #     "    ps_code varchar(50) as %%2"
        #     "  )"
        #     "  as select _twstart tw, to_char(_twstart, 'yyyy-mm-dd hh24:mi:ss.ms') dt, sum(sec_val - fir_val) val from %%trows;"
        # )

    def step3(self):
        tdSql.execute("")

    def step4(self):
        tdSql.execute("")

    def step5(self):
        tdSql.execute("")

    def step6(self):
        tdSql.execute("")

    def step7(self):
        tdSql.execute("")

    def step8(self):
        tdSql.execute("")

    def step9(self):
        tdSql.execute("")

    def step10(self):
        tdSql.execute("")

    def step11(self):
        tdSql.execute("")

    def step12(self):
        tdSql.execute("")

    def step13(self):
        tdSql.execute("")

    def step14(self):
        tdSql.execute("")

    def step15(self):
        tdSql.execute("")

    def step16(self):
        tdSql.execute("")

    def step17(self):
        tdSql.execute("")

    def step18(self):
        tdSql.execute("")

    def step19(self):
        tdSql.execute("")

    def step20(self):
        tdSql.execute("")

    def step21(self):
        tdSql.execute("")

    def step22(self):
        tdSql.execute("")

    def step33(self):
        tdSql.execute("")

    def step34(self):
        tdSql.execute("")

    def step35(self):
        tdSql.execute("")

    def step36(self):
        tdSql.execute("")
