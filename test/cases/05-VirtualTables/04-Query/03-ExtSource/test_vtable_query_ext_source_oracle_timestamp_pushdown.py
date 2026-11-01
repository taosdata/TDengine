# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY, without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
import datetime
import os
import sys
from zoneinfo import ZoneInfo

from new_test_framework.utils import tdLog, tdSql


_DDL_EXT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "01-DDL", "03-ExtSource")
)
if _DDL_EXT_DIR not in sys.path:
    sys.path.insert(0, _DDL_EXT_DIR)

from ext_source_helpers import (  # noqa: E402
    ExtSrcEnv,
    create_ext_source,
    create_oracle_table,
)


_SOURCE = "vt_oracle_timestamp_pushdown_src"
_SCHEMA = ExtSrcEnv.ORACLE_USER.upper()
_TABLE_NAIVE_PK = "VT_ORA_TS_NAIVE_PK"
_TABLE_NAIVE_PK_MS = "VT_ORA_TS_NAIVE_PK_MS"
_VSTABLE_NAIVE = "vst_naive"
_VCHILD_NAIVE = "vct_naive"
_LOCAL_DBS = {
    "ms": "vt_oracle_timestamp_pushdown_ms",
    "us": "vt_oracle_timestamp_pushdown_us",
    "ns": "vt_oracle_timestamp_pushdown_ns",
}

# Same values as the MySQL/PostgreSQL timestamp-pushdown cases: the first
# column is a naive timestamp read in the client timezone, the second an
# absolute instant (Oracle TIMESTAMP WITH TIME ZONE carries its own offset),
# then a naive timestamp, a DATE (second precision) and a value column.
# A TIMESTAMP WITH TIME ZONE column cannot be a primary key (ORA-02329) and an
# external table's key must be exactly one timestamp column, so the aware
# column rides along in the naive-keyed table instead of keying its own one.
_NAIVE_ROWS = (
    (
        "2024-01-01 08:00:00.123456",
        "2024-01-01 00:00:00.654321",
        "2024-01-01 08:00:00.789012",
        "2024-01-02",
        1,
    ),
    (
        "2024-07-01 08:00:00.223456",
        "2024-07-01 00:00:00.754321",
        "2024-07-01 08:00:00.889012",
        "2024-07-02",
        2,
    ),
)

_NAIVE_ROWS_MS = tuple(
    (ts[:23], aware[:23], naive[:23], day_value, value)
    for ts, aware, naive, day_value, value in _NAIVE_ROWS
)


class TestVtableQueryExtSourceOracleTimestampPushdown:
    updatecfgDict = {
        "federatedQueryEnable": 1,
        "clientCfg": {
            "federatedQueryEnable": 1,
        },
    }

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.ensure_oracle()
        ExtSrcEnv.ensure_qnode()
        cls.prepare_remote_tables()
        cfg = next(ExtSrcEnv.oracle_version_configs())
        create_ext_source(_SOURCE, "oracle", cfg.service)
        cls.prepare_vtables()

    @classmethod
    def teardown_class(cls):
        for database in _LOCAL_DBS.values():
            tdSql.execute(f"DROP DATABASE IF EXISTS {database}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_SOURCE}")
        cfg = next(ExtSrcEnv.oracle_version_configs())
        for table in (_TABLE_NAIVE_PK, _TABLE_NAIVE_PK_MS):
            ExtSrcEnv.oracle_drop_table_cfg(cfg, table)

    # --- util ---

    @staticmethod
    def ensure_oracle():
        if ExtSrcEnv._tcp_probe(ExtSrcEnv.ORACLE_HOST, ExtSrcEnv.ORACLE_PORT,
                                timeout=5):
            return
        ExtSrcEnv.start_oracle_instance(ExtSrcEnv.ORACLE_VERSIONS[0])

    @staticmethod
    def prepare_remote_tables():
        def ts_literal(value):
            return f"TIMESTAMP '{value}'"

        def tstz_literal(value):
            # TIMESTAMP WITH TIME ZONE literal: the value carries its offset,
            # so the instant is independent of the client timezone.
            return f"TIMESTAMP '{value} +00:00'"

        def date_literal(value):
            return f"DATE '{value}'"

        def naive_row(row):
            ts, aware, naive, day_value, value = row
            return ("(" + ", ".join((
                ts_literal(ts), tstz_literal(aware), ts_literal(naive),
                date_literal(day_value), str(value))) + ")")

        create_oracle_table(
            _TABLE_NAIVE_PK,
            "ts TIMESTAMP(6) PRIMARY KEY, "
            "aware TIMESTAMP(6) WITH TIME ZONE, "
            "naive_value TIMESTAMP(6), "
            "day_value DATE, "
            "v NUMBER(5,0)",
            [naive_row(row) for row in _NAIVE_ROWS],
        )
        create_oracle_table(
            _TABLE_NAIVE_PK_MS,
            "ts TIMESTAMP(3) PRIMARY KEY, "
            "aware TIMESTAMP(3) WITH TIME ZONE, "
            "naive_value TIMESTAMP(3), "
            "day_value DATE, "
            "v NUMBER(5,0)",
            [naive_row(row) for row in _NAIVE_ROWS_MS],
        )

    @staticmethod
    def prepare_vtables():
        for precision, database in _LOCAL_DBS.items():
            naive_table = _TABLE_NAIVE_PK_MS if precision == "ms" else _TABLE_NAIVE_PK
            naive_ref = f"{_SOURCE}.{_SCHEMA}.{naive_table}"
            tdSql.execute(f"DROP DATABASE IF EXISTS {database}")
            tdSql.execute(f"CREATE DATABASE {database} PRECISION '{precision}'")
            tdSql.execute(
                f"CREATE VTABLE {database}.v_naive ("
                "ts TIMESTAMP, "
                f"aware TIMESTAMP FROM {naive_ref}.aware, "
                f"naive_value TIMESTAMP FROM {naive_ref}.naive_value, "
                f"day_value TIMESTAMP FROM {naive_ref}.day_value, "
                f"v BIGINT FROM {naive_ref}.v)"
            )
            if precision == "us":
                tdSql.execute(
                    f"CREATE STABLE {database}.{_VSTABLE_NAIVE} ("
                    "ts TIMESTAMP, aware TIMESTAMP, naive_value TIMESTAMP, "
                    "day_value TIMESTAMP, v BIGINT) "
                    "TAGS (source_id INT) VIRTUAL 1"
                )
                tdSql.execute(
                    f"CREATE VTABLE {database}.{_VCHILD_NAIVE} ("
                    f"aware FROM {naive_ref}.aware, "
                    f"naive_value FROM {naive_ref}.naive_value, "
                    f"day_value FROM {naive_ref}.day_value, "
                    f"v FROM {naive_ref}.v) "
                    f"USING {database}.{_VSTABLE_NAIVE} TAGS (1)"
                )

    @staticmethod
    def epoch_us(value, timezone):
        value_format = "%Y-%m-%d" if len(value) == 10 else "%Y-%m-%d %H:%M:%S.%f"
        local_value = datetime.datetime.strptime(value, value_format).replace(
            tzinfo=ZoneInfo(timezone)
        )
        utc_value = local_value.astimezone(datetime.timezone.utc)
        epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        delta = utc_value - epoch
        return (
            delta.days * 86_400_000_000
            + delta.seconds * 1_000_000
            + delta.microseconds
        )

    @staticmethod
    def scale_epoch(epoch_us, precision):
        if precision == "ms":
            return epoch_us // 1_000
        if precision == "ns":
            return epoch_us * 1_000
        return epoch_us

    @classmethod
    def expected_naive_rows(cls, timezone, precision):
        rows = []
        source_rows = _NAIVE_ROWS_MS if precision == "ms" else _NAIVE_ROWS
        for ts, aware, naive, day_value, value in source_rows:
            rows.append(
                (
                    cls.scale_epoch(cls.epoch_us(ts, timezone), precision),
                    cls.scale_epoch(cls.epoch_us(aware, "UTC"), precision),
                    cls.scale_epoch(cls.epoch_us(naive, timezone), precision),
                    cls.scale_epoch(cls.epoch_us(day_value, timezone), precision),
                    value,
                )
            )
        return rows

    @staticmethod
    def check_rows(expected):
        tdSql.checkRows(len(expected))
        for row_index, row in enumerate(expected):
            for col_index, value in enumerate(row):
                tdSql.checkData(row_index, col_index, value)

    # --- impl ---

    def do_timestamp_pushdown(self):
        tdSql.query("SHOW LOCAL VARIABLES LIKE 'timezone'")
        original_timezone = str(tdSql.queryResult[0][1]).split(" ")[0]
        try:
            for timezone in ("Asia/Shanghai", "UTC", "America/New_York"):
                tdSql.connect("root", "taosdata")
                tdSql.execute('ALTER LOCAL "federatedQueryEnable" "1"')
                tdSql.execute(f'ALTER LOCAL "timezone" "{timezone}"')

                for precision, database in _LOCAL_DBS.items():
                    tdSql.query(
                        "SELECT CAST(ts AS BIGINT), CAST(aware AS BIGINT), "
                        "CAST(naive_value AS BIGINT), CAST(day_value AS BIGINT), v "
                        f"FROM {database}.v_naive ORDER BY ts"
                    )
                    self.check_rows(self.expected_naive_rows(timezone, precision))

                tdSql.query(
                    "SELECT CAST(ts AS BIGINT) "
                    f"FROM {_LOCAL_DBS['us']}.{_VSTABLE_NAIVE} ORDER BY ts"
                )
                self.check_rows(
                    [(row[0],) for row in self.expected_naive_rows(timezone, "us")]
                )
        finally:
            tdSql.execute(f'ALTER LOCAL "timezone" "{original_timezone}"')
            tdSql.connect("root", "taosdata")

        print("Oracle timestamp pushdown ................ [ passed ]")

    # --- main ---

    def test_oracle_timestamp_pushdown(self):
        """Push Oracle virtual-table timestamp conversion to the remote server

        1. Verify TIMESTAMP WITH TIME ZONE, TIMESTAMP, and DATE across client timezones.
        2. Verify destination database precision conversion for ms, us, and ns.
        3. Verify both static VTABLE and dynamic VSTABLE remote SQL paths.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-29 wpan Add Oracle timestamp pushdown coverage

        """
        self.do_timestamp_pushdown()
