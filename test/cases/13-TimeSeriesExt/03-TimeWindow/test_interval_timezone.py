###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql, etool, tdCom


class TestIntervalDiffTz:
    clientCfgDict = {"timezone": "UTC"}
    updatecfgDict = {
        "timezone": "UTC-8",
        "clientCfg": clientCfgDict,
    }

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        tdLog.info("insert interval test data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "interval.json")
        etool.benchMark(json=json)

    def prepare_scan_timezone_data(self):
        tdLog.info("prepare interval scan timezone regression data.")
        tdSql.execute("drop database if exists db_interval_scan_timezone")
        tdSql.execute(
            "create database db_interval_scan_timezone "
            "vgroups 1 duration 100d stt_trigger 1"
        )
        tdSql.execute("use db_interval_scan_timezone")
        tdSql.execute(
            "create stable taosd_dnodes_info("
            "_ts timestamp,"
            "io_read_disk double,"
            "vnodes_num double,"
            "masters double,"
            "disk_total double,"
            "system_net_out double,"
            "io_write_disk double,"
            "has_mnode double,"
            "has_qnode double,"
            "has_snode double,"
            "mem_engine double,"
            "cpu_engine double,"
            "cpu_cores double,"
            "info_log_count double,"
            "mem_cache_buffer double,"
            "error_log_count double,"
            "debug_log_count double,"
            "trace_log_count double,"
            "disk_used double,"
            "mem_free double,"
            "system_net_in double,"
            "io_read double,"
            "disk_engine double,"
            "mem_total double,"
            "cpu_system double,"
            "io_write double,"
            "uptime double"
            ") tags(cluster_id nchar(32), dnode_id nchar(2), dnode_ep nchar(32))"
        )
        tdSql.execute(
            "create table dnode_0 using taosd_dnodes_info "
            "tags('1', '0', 'localhost:6030')"
        )

        start_ts = 1785316928479
        rows = 1703
        step = 30000
        batch = 500
        for offset in range(0, rows, batch):
            end = min(offset + batch, rows)
            values = ",".join(
                "("
                f"{start_ts + row * step},"
                f"{row % 10},"
                f"{row % 11},"
                f"{row % 12},"
                f"{row % 13},"
                f"{row % 14},"
                f"{row % 15},"
                f"{row % 16},"
                f"{row % 17},"
                f"{row % 18},"
                f"{row % 19},"
                f"{row % 20},"
                f"{row % 21},"
                f"{row % 22},"
                f"{row % 23},"
                f"{row % 24},"
                f"{row % 25},"
                f"{row % 26},"
                f"{row % 27},"
                f"{row % 28},"
                f"{row % 29},"
                f"{row % 30},"
                f"{row % 31},"
                f"{row % 32},"
                f"{row % 33},"
                f"{row % 34},"
                f"{row % 35}"
                ")"
                for row in range(offset, end)
            )
            tdSql.execute(f"insert into dnode_0 values {values}")
        tdSql.execute("flush database db_interval_scan_timezone")

    def check_timezone_config(self):
        tdSql.query("show variables like 'timezone'")
        server_timezone = tdSql.queryResult[0][1]
        tdLog.info(f"server timezone: {server_timezone}")
        assert "UTC-8" in server_timezone

        tdSql.query("show local variables like 'timezone'")
        local_timezone = tdSql.queryResult[0][1]
        tdLog.info(f"local timezone: {local_timezone}")
        assert "Asia/Shanghai" in local_timezone

    def check_interval_scan_timezone_query_ok(self):
        tdLog.info("check interval scan timezone query.")
        tdSql.execute("alter local 'timezone Asia/Shanghai'")
        self.check_timezone_config()
        self.prepare_scan_timezone_data()

        sql = (
            "select _wstart, count(*) "
            "from taosd_dnodes_info "
            "where _c0 >= '2026-07-29T00:00:00' "
            "and _c0 < '2026-07-31T00:00:00' "
            "interval(1d) "
            "order by _wstart"
        )

        tdSql.query("explain verbose true " + sql)
        for row in tdSql.queryResult:
            tdLog.info(row[0])

        plan = "\n".join(row[0] for row in tdSql.queryResult)
        assert "Interval on Column _ts" in plan
        assert "Table Scan on taosd_dnodes_info" in plan
        assert "data_load=no" in plan

        tdSql.query(sql)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2026-07-29 00:00:00.000")
        tdSql.checkData(0, 1, 796)
        tdSql.checkData(1, 0, "2026-07-30 00:00:00.000")
        tdSql.checkData(1, 1, 907)
        
        tdSql.execute("alter all dnodes 'timezone UTC'")
        tdSql.query(sql)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2026-07-29 00:00:00.000")
        tdSql.checkData(0, 1, 796)
        tdSql.checkData(1, 0, "2026-07-30 00:00:00.000")
        tdSql.checkData(1, 1, 907)       

    def test_interval_diff_tz(self):
        """Interval: timezone

        test interval with client and server using different timezone

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.3.0.0

        Labels: decimal,integration,functional
        History:
            - 2024-9-14 Feng Chao Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        # read sql from .sql file and execute
        tdLog.info("test normal query.")
        self.sqlFile = etool.curFile(__file__, f"in/interval.in")
        self.ansFile = etool.curFile(__file__, f"ans/interval_diff_tz.csv")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "interval_diff_tz")

    def test_interval_scan_timezone(self):
        """Interval: scan timezone

        Verify interval scan keeps timezone-aware day buckets on a block that
        crosses local midnight.

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None
        History:
            - 2026-08-07 Add scan timezone regression

        """
        try:
            self.check_interval_scan_timezone_query_ok()
        finally:
            tdSql.execute("alter local 'timezone UTC'")
