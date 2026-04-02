import pytest
from new_test_framework.utils import tdLog, tdSql, tdStream
import time


class TestStateWindow:

    @pytest.mark.ci
    def test_state_window(self):
        """OldPy: state window

        迁移自老用例: tests/system-test/8-stream/state_window_case.py

        Catalog:
            - Streams:OldPyCases
            
        Since: v3.3.6.0

        Labels: common,ci

        Jira: https://jira.taosdata.com:18080/browse/TD-36887

        History:
            - 2025-07-21: Created by zyyang
        """
        self.db = "test"
        self.stable = "st_variable_data"
        self.table = "aaa"

        self.stream1 = "stream_device_alarm"
        self.stream2 = "stream_device_alarm2"

        # create snode if not exists
        snodes = tdSql.getResult("SHOW SNODES;")
        if snodes is None or len(snodes) == 0:
            tdStream.createSnode()

        # create database and stable
        tdSql.executes(
            [
                f"DROP DATABASE IF EXISTS {self.db};",
                f"CREATE DATABASE IF NOT EXISTS {self.db};",
                f"USE {self.db};",
                f"CREATE STABLE `{self.stable}` (`load_time` TIMESTAMP, `collect_time` TIMESTAMP, `var_value` NCHAR(300)) TAGS (`factory_id` NCHAR(30), `device_code` NCHAR(80), `var_name` NCHAR(120), `var_type` NCHAR(30), `var_address` NCHAR(100), `var_attribute` NCHAR(30), `device_name` NCHAR(150), `var_desc` NCHAR(200), `trigger_value` NCHAR(50), `var_category` NCHAR(50), `var_category_desc` NCHAR(200));",
                f"CREATE TABLE `{self.table}` USING `{self.stable}` TAGS('a1','a2', 'a3','a4','a5','a6','a7','a8','a9','a10','a11')",
                f"CREATE STREAM {self.stream1} STATE_WINDOW(var_value) FROM {self.stable} PARTITION BY tbname, factory_id, device_code, var_name INTO {self.stream1} AS SELECT _twstart AS start_time, _twend AS end_time, first(var_value) AS var_value FROM %%tbname WHERE load_time >= _twstart and load_time <= _twend;",
                f"CREATE STREAM {self.stream2} STATE_WINDOW(var_value) FROM {self.stable} PARTITION BY tbname, factory_id, device_code, var_name INTO {self.stream2} AS SELECT _twstart AS start_time, _twend AS end_time, var_value FROM %%trows ORDER BY load_time ASC;",
                f"insert into {self.table} VALUES('2024-07-15 14:00:00', '2024-07-15 14:00:00', 'a8')",
                f"insert into {self.table} VALUES('2024-07-15 14:10:00', '2024-07-15 14:10:00', 'a9')",
            ],
            queryTimes=1,
        )

        # check stream status
        tdStream.checkStreamStatus()

        # insert data
        tdSql.executes(
            [
                f"insert into {self.table} VALUES('2024-07-15 14:00:00', '2024-07-15 14:00:00', 'val1')",
                f"insert into {self.table} VALUES('2024-07-15 14:10:00', '2024-07-15 14:10:00', 'val2')",
                f"insert into {self.table} VALUES('2024-07-15 14:20:00', '2024-07-15 14:20:00', 'val2')",
                f"insert into {self.table} VALUES('2024-07-15 14:30:00', '2024-07-15 14:30:00', 'val3')",
            ]
        )

        # check result
        tdSql.checkResultsByFunc(
            sql=f"select var_value from {self.stream1}",
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "val1")
            and tdSql.compareData(1, 0, "val2"),
        )
        tdSql.checkResultsByFunc(
            sql=f"select cast(end_time as timestamp) from {self.stream1}",
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, 1721023200000)
            and tdSql.compareData(1, 0, 1721024400000),
        )
        tdSql.checkResultsByFunc(
            sql=f"select var_value from {self.stream2}",
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "val1")
            and tdSql.compareData(1, 0, "val2"),
        )

        tdLog.debug("TestStateWindow done")
