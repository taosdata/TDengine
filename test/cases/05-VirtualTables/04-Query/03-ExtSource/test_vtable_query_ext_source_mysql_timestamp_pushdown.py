###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

# -*- coding: utf-8 -*-
import datetime
import os
import sys
from zoneinfo import ZoneInfo

from new_test_framework.utils import tdLog, tdSql, etool


_DDL_EXT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "01-DDL", "03-ExtSource")
)
if _DDL_EXT_DIR not in sys.path:
    sys.path.insert(0, _DDL_EXT_DIR)

from ext_source_helpers import ExtSrcEnv, create_ext_source  # noqa: E402


_REMOTE_DB = "vt_mysql_timestamp_pushdown_remote"
_SOURCE = "vt_mysql_timestamp_pushdown_src"
_TABLE_NAIVE_PK = "time_naive_pk"
_TABLE_AWARE_PK = "time_aware_pk"
_TABLE_NAIVE_PK_MS = "time_naive_pk_ms"
_TABLE_AWARE_PK_MS = "time_aware_pk_ms"
_VSTABLE_NAIVE = "vst_naive"
_VCHILD_NAIVE = "vct_naive"
_LOCAL_DBS = {
    "ms": "vt_mysql_timestamp_pushdown_ms",
    "us": "vt_mysql_timestamp_pushdown_us",
    "ns": "vt_mysql_timestamp_pushdown_ns",
}

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

_AWARE_ROWS = (
    (
        "2024-01-01 00:00:00.654321",
        "2024-01-01 08:00:00.789012",
        1,
    ),
    (
        "2024-07-01 00:00:00.754321",
        "2024-07-01 08:00:00.889012",
        2,
    ),
)

_NAIVE_ROWS_MS = tuple(
    (ts[:23], aware[:23], naive[:23], day_value, value)
    for ts, aware, naive, day_value, value in _NAIVE_ROWS
)
_AWARE_ROWS_MS = tuple(
    (ts[:23], naive[:23], value) for ts, naive, value in _AWARE_ROWS
)


class TestVtableQueryExtSourceMysqlTimestampPushdown:
    updatecfgDict = {
        "federatedQueryEnable": 1,
        "clientCfg": {
            "federatedQueryEnable": 1,
        },
    }

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.ensure_mysql()
        ExtSrcEnv.ensure_qnode()
        cls.prepare_remote_tables()
        create_ext_source(_SOURCE, "mysql", _REMOTE_DB)
        cls.prepare_vtables()

    @classmethod
    def teardown_class(cls):
        for database in _LOCAL_DBS.values():
            tdSql.execute(f"DROP DATABASE IF EXISTS {database}")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {_SOURCE}")
        ExtSrcEnv.mysql_drop_db(_REMOTE_DB)

    # --- util ---

    @staticmethod
    def ensure_mysql():
        if ExtSrcEnv._tcp_probe(ExtSrcEnv.MYSQL_HOST, ExtSrcEnv.MYSQL_PORT, timeout=5):
            return
        ExtSrcEnv.start_mysql_instance(ExtSrcEnv.MYSQL_VERSIONS[0])

    @staticmethod
    def prepare_remote_tables():
        ExtSrcEnv.mysql_drop_db(_REMOTE_DB)
        ExtSrcEnv.mysql_create_db(_REMOTE_DB)

        naive_values = ", ".join(
            "(" + ", ".join(
                (
                    f"'{ts}'",
                    f"'{aware}'",
                    f"'{naive}'",
                    f"'{day_value}'",
                    str(value),
                )
            ) + ")"
            for ts, aware, naive, day_value, value in _NAIVE_ROWS
        )
        aware_values = ", ".join(
            f"('{ts}', '{naive}', {value})"
            for ts, naive, value in _AWARE_ROWS
        )
        naive_ms_values = ", ".join(
            f"('{ts}', '{aware}', '{naive}', '{day_value}', {value})"
            for ts, aware, naive, day_value, value in _NAIVE_ROWS_MS
        )
        aware_ms_values = ", ".join(
            f"('{ts}', '{naive}', {value})"
            for ts, naive, value in _AWARE_ROWS_MS
        )
        ExtSrcEnv.mysql_exec(
            _REMOTE_DB,
            [
                "SET time_zone = '+00:00'",
                f"CREATE TABLE {_TABLE_NAIVE_PK} ("
                "ts DATETIME(6) PRIMARY KEY, "
                "aware TIMESTAMP(6), "
                "naive_value DATETIME(6), "
                "day_value DATE, "
                "v INT)",
                f"INSERT INTO {_TABLE_NAIVE_PK} VALUES {naive_values}",
                f"CREATE TABLE {_TABLE_AWARE_PK} ("
                "ts TIMESTAMP(6) PRIMARY KEY, "
                "naive_value DATETIME(6), "
                "v INT)",
                f"INSERT INTO {_TABLE_AWARE_PK} VALUES {aware_values}",
                f"CREATE TABLE {_TABLE_NAIVE_PK_MS} ("
                "ts DATETIME(3) PRIMARY KEY, "
                "aware TIMESTAMP(3), "
                "naive_value DATETIME(3), "
                "day_value DATE, "
                "v INT)",
                f"INSERT INTO {_TABLE_NAIVE_PK_MS} VALUES {naive_ms_values}",
                f"CREATE TABLE {_TABLE_AWARE_PK_MS} ("
                "ts TIMESTAMP(3) PRIMARY KEY, "
                "naive_value DATETIME(3), "
                "v INT)",
                f"INSERT INTO {_TABLE_AWARE_PK_MS} VALUES {aware_ms_values}",
            ],
        )

    @staticmethod
    def prepare_vtables():
        for precision, database in _LOCAL_DBS.items():
            naive_table = _TABLE_NAIVE_PK_MS if precision == "ms" else _TABLE_NAIVE_PK
            aware_table = _TABLE_AWARE_PK_MS if precision == "ms" else _TABLE_AWARE_PK
            naive_ref = f"{_SOURCE}.{_REMOTE_DB}.{naive_table}"
            aware_ref = f"{_SOURCE}.{_REMOTE_DB}.{aware_table}"
            tdSql.execute(f"DROP DATABASE IF EXISTS {database}")
            tdSql.execute(f"CREATE DATABASE {database} PRECISION '{precision}'")
            tdSql.execute(
                f"CREATE VTABLE {database}.v_naive ("
                "ts TIMESTAMP, "
                f"aware TIMESTAMP FROM {naive_ref}.aware, "
                f"naive_value TIMESTAMP FROM {naive_ref}.naive_value, "
                f"day_value TIMESTAMP FROM {naive_ref}.day_value, "
                f"v INT FROM {naive_ref}.v)"
            )
            tdSql.execute(
                f"CREATE VTABLE {database}.v_aware ("
                "ts TIMESTAMP, "
                f"naive_value TIMESTAMP FROM {aware_ref}.naive_value, "
                f"v INT FROM {aware_ref}.v)"
            )
            if precision == "us":
                tdSql.execute(
                    f"CREATE STABLE {database}.{_VSTABLE_NAIVE} ("
                    "ts TIMESTAMP, aware TIMESTAMP, naive_value TIMESTAMP, "
                    "day_value TIMESTAMP, v INT) "
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

    @classmethod
    def expected_aware_rows(cls, timezone, precision):
        rows = []
        source_rows = _AWARE_ROWS_MS if precision == "ms" else _AWARE_ROWS
        for ts, naive, value in source_rows:
            rows.append(
                (
                    cls.scale_epoch(cls.epoch_us(ts, "UTC"), precision),
                    cls.scale_epoch(cls.epoch_us(naive, timezone), precision),
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
                        "SELECT CAST(ts AS BIGINT), "
                        "CAST(naive_value AS BIGINT), v "
                        f"FROM {database}.v_aware ORDER BY ts"
                    )
                    self.check_rows(self.expected_aware_rows(timezone, precision))

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

        print("MySQL timestamp pushdown ................. [ passed ]")

    # --- main ---

    def test_mysql_timestamp_pushdown(self):
        """Push MySQL virtual-table timestamp conversion to the remote server

        1. Verify TIMESTAMP, DATETIME, and DATE conversion across client timezones.
        2. Verify destination database precision conversion for ms, us, and ns.
        3. Verify both static VTABLE and dynamic VSTABLE remote SQL paths.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-03 Simon Add MySQL timestamp pushdown coverage

        """
        self.do_timestamp_pushdown()
