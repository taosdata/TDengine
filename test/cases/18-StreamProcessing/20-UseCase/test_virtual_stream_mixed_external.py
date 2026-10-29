from new_test_framework.utils import tdLog, tdSql, etool

import datetime
import os
import sys
import time

from new_test_framework.utils import tdStream


_CASE_DIR = os.path.dirname(os.path.abspath(__file__))
_EXT_HELPER_DIR = os.path.abspath(
    os.path.join(
        _CASE_DIR,
        "..",
        "..",
        "05-VirtualTables",
        "01-DDL",
        "03-ExtSource",
    )
)
_FQ_DIR = os.path.abspath(
    os.path.join(
        _CASE_DIR,
        "..",
        "..",
        "09-DataQuerying",
        "19-FederatedQuery",
    )
)
_FS_DIR = os.path.abspath(os.path.join(_CASE_DIR, "..", "99-Others"))

for helper_dir in (_EXT_HELPER_DIR, _FQ_DIR, _FS_DIR):
    if helper_dir not in sys.path:
        sys.path.insert(0, helper_dir)

from ext_source_helpers import (  # noqa: E402
    ExtSrcEnv,
    create_ext_source,
    create_influx_measurement,
    create_mysql_table,
    create_pg_table,
    create_remote_db,
)
from fs_common import ensure_snode  # noqa: E402


class TestVirtualStreamMixedExternal:
    DB = "vs_mixed_ext"
    MYSQL_DB = "vs_mixed_mysql"
    MYSQL_SOURCE = "vs_mixed_mysql_src"
    PG_DB = "vs_mixed_pg"
    PG_SOURCE = "vs_mixed_pg_src"
    INFLUX_DB = "vs_mixed_influx"
    INFLUX_SOURCE = "vs_mixed_influx_src"
    INFLUX_MEASUREMENT = "same_m"
    STREAM = "mixed_stream"
    THREE_EXT_INTERVAL_STREAM = "three_ext_interval_stream"
    THREE_EXT_SLIDING_STREAM = "three_ext_sliding_stream"
    THREE_EXT_SESSION_STREAM = "three_ext_session_stream"
    THREE_EXT_STATE_STREAM = "three_ext_state_stream"
    THREE_EXT_EVENT_STREAM = "three_ext_event_stream"
    SAME_SOURCE_TWO_TABLE_STREAM = "same_source_two_table_stream"
    VSTB = "mixed_vstb"
    THREE_EXT_INTERVAL_VSTB = "three_ext_interval_vstb"
    SAME_SOURCE_TWO_TABLE_VSTB = "same_source_two_table_vstb"
    VTABLE_A = "mixed_a"
    VTABLE_B = "mixed_b"
    THREE_EXT_INTERVAL_VTABLE = "three_ext_interval_vt"
    SAME_SOURCE_TWO_TABLE_VTABLE = "same_source_two_table_vt"
    SINK = "mixed_sink"
    THREE_EXT_INTERVAL_SINK = "three_ext_interval_sink"
    THREE_EXT_SLIDING_SINK = "three_ext_sliding_sink"
    THREE_EXT_SESSION_SINK = "three_ext_session_sink"
    THREE_EXT_STATE_SINK = "three_ext_state_sink"
    THREE_EXT_EVENT_SINK = "three_ext_event_sink"
    SAME_SOURCE_TWO_TABLE_SINK = "same_source_two_table_sink"
    VAR_WIDTH_STREAM = "var_width_stream"
    VAR_WIDTH_SINK = "var_width_sink"
    VAR_WIDTH_VSTB = "var_width_vstb"
    VAR_WIDTH_VTABLE = "var_width_vt"
    QUOTED_PG_STREAM = "quoted_pg_stream"
    QUOTED_PG_SINK = "quoted_pg_sink"
    QUOTED_PG_VSTB = "quoted_pg_vstb"
    QUOTED_PG_VTABLE = "quoted_pg_vt"
    QUOTED_PG_STEP_MS = 1000
    BASE_MS = 1767225600000
    THREE_EXT_INTERVAL_STEP_MS = 1000
    three_ext_interval_base_ms = None

    @classmethod
    def setup_class(cls):
        tdLog.info(f"start to execute {__file__}")
        cls.env = ExtSrcEnv()
        cls.env.ensure_env()
        ensure_snode()
        tdSql.execute("set timezone 'UTC'")

    @classmethod
    def teardown_class(cls):
        try:
            cls._cleanup_fixture()
        finally:
            cls.env.teardown_env()

    @classmethod
    def _cleanup_fixture(cls):
        tdLog.info("cleaning mixed virtual external fixture")
        try:
            for stream in (
                cls.STREAM,
                cls.THREE_EXT_INTERVAL_STREAM,
                cls.THREE_EXT_SLIDING_STREAM,
                cls.THREE_EXT_SESSION_STREAM,
                cls.THREE_EXT_STATE_STREAM,
                cls.THREE_EXT_EVENT_STREAM,
                cls.SAME_SOURCE_TWO_TABLE_STREAM,
                cls.VAR_WIDTH_STREAM,
                cls.QUOTED_PG_STREAM,
            ):
                tdSql.execute(f"DROP STREAM IF EXISTS {cls.DB}.{stream}", queryTimes=30)
            tdSql.execute(f"DROP STREAM IF EXISTS {cls.DB}.influx_col_stream", queryTimes=30)
            tdSql.execute(f"DROP STREAM IF EXISTS {cls.DB}.history_guard", queryTimes=30)
            tdSql.execute(f"DROP STREAM IF EXISTS {cls.DB}.bad_time_guard", queryTimes=30)
            tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB} FORCE")
            tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {cls.MYSQL_SOURCE}")
            tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {cls.PG_SOURCE}")
            tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {cls.INFLUX_SOURCE}")
            tdSql.execute(f"alter all dnodes 'qdebugflag 143'")
        finally:
            for dropper, database in (
                (ExtSrcEnv.mysql_drop_db, cls.MYSQL_DB),
                (ExtSrcEnv.pg_drop_db, cls.PG_DB),
                (ExtSrcEnv.influx_drop_db, cls.INFLUX_DB),
            ):
                try:
                    dropper(database)
                except Exception as error:
                    tdLog.info(
                        f"remote database cleanup skipped: database={database}, error={error}"
                    )

    @classmethod
    def _prepare_fixture(cls):
        cls._cleanup_fixture()
        tdLog.info("creating local, MySQL, and PostgreSQL fixture objects")

        create_remote_db("mysql", cls.MYSQL_DB)
        create_mysql_table(
            cls.MYSQL_DB,
            "same_t",
            "ts DATETIME(3) NOT NULL PRIMARY KEY, value_a INT, value_b INT",
        )
        create_mysql_table(
            cls.MYSQL_DB,
            "missing_ts",
            "id INT NOT NULL PRIMARY KEY, value_a INT",
        )

        create_remote_db("postgresql", cls.PG_DB)
        create_pg_table(
            cls.PG_DB,
            "same_t",
            "ts TIMESTAMP(3) PRIMARY KEY, value_a INTEGER, value_b INTEGER",
        )
        create_pg_table(
            cls.PG_DB,
            "bad_type",
            "ts TIMESTAMP(3) PRIMARY KEY, payload BYTEA",
        )

        warmup_ts = cls._remote_timestamp(cls.BASE_MS - 2000)
        ExtSrcEnv.mysql_exec(
            cls.MYSQL_DB,
            [f"INSERT INTO same_t VALUES ('{warmup_ts}', 1, 1)"],
        )
        ExtSrcEnv.pg_exec(
            cls.PG_DB,
            [f"INSERT INTO same_t VALUES ('{warmup_ts}', 1, 1)"],
        )

        create_ext_source(cls.MYSQL_SOURCE, "mysql", cls.MYSQL_DB)
        create_ext_source(cls.PG_SOURCE, "postgresql", cls.PG_DB)

        tdSql.execute(f"CREATE DATABASE {cls.DB} VGROUPS 1 PRECISION 'ms'")
        tdSql.execute(f"CREATE TABLE {cls.DB}.same_t (ts TIMESTAMP, value_a INT)")
        tdSql.execute(
            f"CREATE STABLE {cls.DB}.{cls.VSTB} ("
            "ts TIMESTAMP, local_value INT, mysql_a INT, mysql_b INT, "
            "pg_a INT, pg_b INT) TAGS (site INT) VIRTUAL 1"
        )
        cls._create_virtual_child(cls.VTABLE_A, 1)
        cls._create_virtual_child(cls.VTABLE_B, 2)
        tdLog.info("mixed virtual external fixture is ready")

    @classmethod
    def _prepare_influx_column_fixture(cls):
        cls._cleanup_fixture()
        tdLog.info("creating local and InfluxDB column-reference fixture objects")

        create_remote_db("influxdb", cls.INFLUX_DB)
        cls._seed_influx_measurement()
        create_ext_source(cls.INFLUX_SOURCE, "influxdb", cls.INFLUX_DB)

        tdSql.execute(f"CREATE DATABASE {cls.DB} VGROUPS 1 PRECISION 'ns'")
        tdSql.execute(f"CREATE TABLE {cls.DB}.same_t (ts TIMESTAMP, value_a INT)")
        tdSql.execute(
            f"CREATE STABLE {cls.DB}.influx_col_vstb "
            "(ts TIMESTAMP, local_value INT, influx_load BIGINT) "
            "TAGS (site NCHAR(16)) VIRTUAL 1"
        )
        tdSql.execute(
            f"CREATE VTABLE {cls.DB}.influx_col_a ("
            f"local_value FROM {cls.DB}.same_t.value_a, "
            "influx_load FROM s_influx.load) "
            f"USING {cls.DB}.influx_col_vstb TAGS('site_a') "
            f"SERIES s_influx AS {cls.INFLUX_SOURCE}.{cls.INFLUX_DB}."
            f"{cls.INFLUX_MEASUREMENT} (host='site_a')"
        )
        tdSql.execute(
            f"CREATE VTABLE {cls.DB}.influx_col_b ("
            f"local_value FROM {cls.DB}.same_t.value_a, "
            "influx_load FROM s_influx.load) "
            f"USING {cls.DB}.influx_col_vstb TAGS('site_b') "
            f"SERIES s_influx AS {cls.INFLUX_SOURCE}.{cls.INFLUX_DB}."
            f"{cls.INFLUX_MEASUREMENT} (host='site_b')"
        )
        tdLog.info("InfluxDB column-reference fixture is ready")

    @classmethod
    def _prepare_same_source_two_table_fixture(cls):
        cls._cleanup_fixture()
        tdLog.info("creating same-source two-external-table fixture objects")

        create_remote_db("mysql", cls.MYSQL_DB)
        create_mysql_table(
            cls.MYSQL_DB,
            "same_t",
            "ts DATETIME(3) NOT NULL PRIMARY KEY, value_a INT",
        )
        create_mysql_table(
            cls.MYSQL_DB,
            "same_t_extra",
            "ts DATETIME(3) NOT NULL PRIMARY KEY, value_x INT",
        )
        warmup_ts = cls._remote_timestamp(cls.BASE_MS - 2000)
        ExtSrcEnv.mysql_exec(
            cls.MYSQL_DB,
            [f"INSERT INTO same_t VALUES ('{warmup_ts}', 1)"],
        )
        ExtSrcEnv.mysql_exec(
            cls.MYSQL_DB,
            [f"INSERT INTO same_t_extra VALUES ('{warmup_ts}', 1)"],
        )
        create_ext_source(cls.MYSQL_SOURCE, "mysql", cls.MYSQL_DB)

        tdSql.execute(f"CREATE DATABASE {cls.DB} VGROUPS 1 PRECISION 'ms'")
        tdSql.execute(f"CREATE TABLE {cls.DB}.same_t (ts TIMESTAMP, value_a INT)")
        tdSql.execute(
            f"CREATE STABLE {cls.DB}.{cls.SAME_SOURCE_TWO_TABLE_VSTB} "
            "(ts TIMESTAMP, tick INT, mysql_a INT, mysql_extra INT) "
            "TAGS (site NCHAR(16)) VIRTUAL 1"
        )
        tdSql.execute(
            f"CREATE VTABLE {cls.DB}.{cls.SAME_SOURCE_TWO_TABLE_VTABLE} ("
            f"tick FROM {cls.DB}.same_t.value_a, "
            f"mysql_a FROM {cls.MYSQL_SOURCE}.{cls.MYSQL_DB}.same_t.value_a, "
            f"mysql_extra FROM {cls.MYSQL_SOURCE}.{cls.MYSQL_DB}.same_t_extra.value_x) "
            f"USING {cls.DB}.{cls.SAME_SOURCE_TWO_TABLE_VSTB} TAGS('site_same')"
        )
        tdLog.info("same-source two-external-table fixture is ready")

    @classmethod
    def _prepare_three_external_interval_fixture(cls, base_ms=None):
        cls._cleanup_fixture()
        tdLog.info("creating three-external-source interval fixture objects")

        cls.three_ext_interval_base_ms = base_ms if base_ms is not None else cls.BASE_MS
        tdLog.info(
            f"three-external interval fixed base timestamp: "
            f"base_ms={cls.three_ext_interval_base_ms}, "
            f"base_ts={cls._timestamp(cls.three_ext_interval_base_ms)}"
        )
        create_remote_db("mysql", cls.MYSQL_DB)
        create_mysql_table(
            cls.MYSQL_DB,
            "interval_t",
            "ts DATETIME(3) NOT NULL PRIMARY KEY, value_a INT",
        )

        create_remote_db("postgresql", cls.PG_DB)
        create_pg_table(
            cls.PG_DB,
            "interval_t",
            "ts TIMESTAMP(3) PRIMARY KEY, value_a INTEGER",
        )

        create_remote_db("influxdb", cls.INFLUX_DB)
        seed_ms = cls.BASE_MS - 2 * cls.THREE_EXT_INTERVAL_STEP_MS
        seed_text = cls._remote_timestamp(seed_ms)
        # InfluxDB stores an absolute instant, so derive the ns epoch from the
        # millisecond value instead of re-parsing a wall clock string.
        seed_ns = seed_ms * 1_000_000
        tdLog.info(
            f"seeding three-external interval measurements: "
            f"seed_ts={seed_text}, seed_ns={seed_ns}"
        )
        seed_influx_lines = [
            f"{cls.INFLUX_MEASUREMENT},host=site_all load=1i {seed_ns}",
        ]
        create_influx_measurement(
            cls.INFLUX_DB,
            seed_influx_lines,
        )
        seed_mysql_row = [f"('{seed_text}', 1)"]
        seed_pg_row = [f"('{seed_text}', 1)"]
        ExtSrcEnv.mysql_exec(
            cls.MYSQL_DB,
            [f"INSERT INTO interval_t VALUES {', '.join(seed_mysql_row)}"],
        )
        ExtSrcEnv.pg_exec(
            cls.PG_DB,
            [f"INSERT INTO interval_t VALUES {', '.join(seed_pg_row)}"],
        )

        create_ext_source(cls.MYSQL_SOURCE, "mysql", cls.MYSQL_DB)
        create_ext_source(cls.PG_SOURCE, "postgresql", cls.PG_DB)
        create_ext_source(cls.INFLUX_SOURCE, "influxdb", cls.INFLUX_DB)

        tdSql.execute(f"CREATE DATABASE {cls.DB} VGROUPS 1 PRECISION 'ns'")
        tdSql.execute(f"CREATE TABLE {cls.DB}.same_t (ts TIMESTAMP, value_a INT)")
        tdSql.execute(
            f"CREATE STABLE {cls.DB}.{cls.THREE_EXT_INTERVAL_VSTB} "
            "(ts TIMESTAMP, tick INT, mysql_v INT, pg_v INT, influx_v BIGINT) "
            "TAGS (site NCHAR(16)) VIRTUAL 1"
        )
        tdSql.execute(
            f"CREATE VTABLE {cls.DB}.{cls.THREE_EXT_INTERVAL_VTABLE} ("
            f"tick FROM {cls.DB}.same_t.value_a, "
            f"mysql_v FROM {cls.MYSQL_SOURCE}.{cls.MYSQL_DB}.interval_t.value_a, "
            f"pg_v FROM {cls.PG_SOURCE}.{cls.PG_DB}.interval_t.value_a, "
            "influx_v FROM s_influx.load) "
            f"USING {cls.DB}.{cls.THREE_EXT_INTERVAL_VSTB} TAGS('site_all') "
            f"SERIES s_influx AS {cls.INFLUX_SOURCE}.{cls.INFLUX_DB}."
            f"{cls.INFLUX_MEASUREMENT} (host='site_all')"
        )
        tdLog.info("three-external-source interval fixture is ready")

    @classmethod
    def _seed_influx_measurement(cls):
        seed_text = cls._timestamp(cls.BASE_MS)
        seed_dt = datetime.datetime.strptime(
            seed_text, "%Y-%m-%d %H:%M:%S.%f"
        ).replace(tzinfo=datetime.timezone.utc)
        seed_ts = int(seed_dt.timestamp() * 1_000_000_000)
        tdLog.info(
            f"seeding InfluxDB measurement rows: measurement={cls.INFLUX_MEASUREMENT}, "
            f"ts={seed_text}, ns={seed_ts}"
        )
        create_influx_measurement(
            cls.INFLUX_DB,
            [
                f"{cls.INFLUX_MEASUREMENT},host=site_a load=101i {seed_ts}",
                f"{cls.INFLUX_MEASUREMENT},host=site_b load=201i {seed_ts}",
            ],
        )

    @classmethod
    def _create_virtual_child(cls, table_name, site):
        tdSql.execute(
            f"CREATE VTABLE {cls.DB}.{table_name} ("
            f"local_value FROM {cls.DB}.same_t.value_a, "
            f"mysql_a FROM {cls.MYSQL_SOURCE}.{cls.MYSQL_DB}.same_t.value_a, "
            f"mysql_b FROM {cls.MYSQL_SOURCE}.{cls.MYSQL_DB}.same_t.value_b, "
            f"pg_a FROM {cls.PG_SOURCE}.{cls.PG_DB}.same_t.value_a, "
            f"pg_b FROM {cls.PG_SOURCE}.{cls.PG_DB}.same_t.value_b) "
            f"USING {cls.DB}.{cls.VSTB} TAGS({site})"
        )

    @classmethod
    def _create_stream(cls):
        tdLog.info("creating mixed virtual external stream")
        tdSql.execute(
            f"CREATE STREAM {cls.DB}.{cls.STREAM} COUNT_WINDOW(1) "
            f"FROM {cls.DB}.{cls.VSTB} PARTITION BY tbname "
            "STREAM_OPTIONS(IGNORE_DISORDER) "
            f"INTO {cls.DB}.{cls.SINK} "
            f"OUTPUT_SUBTABLE(CONCAT('{cls.SINK}_', tbname)) "
            "(first_ts, last_ts, local_sum, mysql_a_sum, mysql_b_sum, "
            "pg_a_sum, pg_b_sum) AS SELECT first(_c0), last_row(_c0), "
            "sum(local_value), sum(mysql_a), sum(mysql_b), "
            "sum(pg_a), sum(pg_b) FROM %%trows"
        )
        tdStream.checkStreamStatus(cls.STREAM)
        tdLog.info("mixed virtual external stream is running with two sources")

    @classmethod
    def _insert_phase(cls, phase, local_values, mysql_values, pg_values, pg_shift_ms=0):
        start_offset = phase * 20000
        local_rows = []
        mysql_rows = []
        pg_rows = []
        for index in range(2):
            base_ts = cls.BASE_MS + start_offset + index * 10000
            local_rows.append(f"('{cls._timestamp(base_ts)}', {local_values[index]})")
            mysql_rows.append(
                f"('{cls._remote_timestamp(base_ts)}', {mysql_values[index][0]}, "
                f"{mysql_values[index][1]})"
            )
            pg_rows.append(
                f"('{cls._remote_timestamp(base_ts + pg_shift_ms)}', {pg_values[index][0]}, "
                f"{pg_values[index][1]})"
            )

        tdLog.info(
            f"inserting phase={phase}, start_offset={start_offset}, pg_shift_ms={pg_shift_ms}"
        )
        ExtSrcEnv.mysql_exec(
            cls.MYSQL_DB,
            [f"INSERT INTO same_t VALUES {', '.join(mysql_rows)}"],
        )
        ExtSrcEnv.pg_exec(
            cls.PG_DB,
            [f"INSERT INTO same_t VALUES {', '.join(pg_rows)}"],
        )
        tdSql.execute(
            f"INSERT INTO {cls.DB}.same_t VALUES {', '.join(local_rows)}"
        )

    @classmethod
    def _insert_late_rows(cls):
        late_ts = cls.BASE_MS + 5000
        tdLog.info(f"inserting ignored late rows at ts={late_ts}")
        ExtSrcEnv.mysql_exec(
            cls.MYSQL_DB,
            [
                "INSERT INTO same_t VALUES "
                f"('{cls._remote_timestamp(late_ts)}', 900, 9000)"
            ],
        )
        ExtSrcEnv.pg_exec(
            cls.PG_DB,
            [
                "INSERT INTO same_t VALUES "
                f"('{cls._remote_timestamp(late_ts)}', 800, 8000)"
            ],
        )
        tdSql.execute(
            f"INSERT INTO {cls.DB}.same_t VALUES ('{cls._timestamp(late_ts)}', 700)"
        )

    @classmethod
    def _three_external_interval_remote_rows(cls):
        mysql_rows = []
        pg_rows = []
        influx_lines = []
        base_ms = cls._three_external_interval_base_ms()
        for index in range(12):
            ts_ms = base_ms + index * cls.THREE_EXT_INTERVAL_STEP_MS
            ts_text = cls._remote_timestamp(ts_ms)
            # InfluxDB timestamps are absolute ns epochs, so no wall-clock
            # rendering is involved here.
            influx_ts = ts_ms * 1_000_000
            mysql_rows.append(f"('{ts_text}', {100 + index})")
            pg_rows.append(f"('{ts_text}', {1000 + index})")
            influx_lines.append(
                f"{cls.INFLUX_MEASUREMENT},host=site_all "
                f"load={10000 + index}i {influx_ts}"
            )
        return {"mysql": mysql_rows, "pg": pg_rows, "influx": influx_lines}

    @classmethod
    def _insert_three_external_interval_remote_rows(cls):
        remote_rows = cls._three_external_interval_remote_rows()
        tdLog.info(
            f"inserting interval external rows: rows={len(remote_rows['mysql'])}"
        )
        ExtSrcEnv.mysql_exec(
            cls.MYSQL_DB,
            [f"INSERT INTO interval_t VALUES {', '.join(remote_rows['mysql'])}"],
        )
        ExtSrcEnv.pg_exec(
            cls.PG_DB,
            [f"INSERT INTO interval_t VALUES {', '.join(remote_rows['pg'])}"],
        )
        create_influx_measurement(cls.INFLUX_DB, remote_rows["influx"])

    @classmethod
    def _insert_three_external_interval_rows(cls):
        cls._insert_three_external_interval_trigger_values(
            list(range(1, 13)), "interval"
        )

    @classmethod
    def _insert_three_external_interval_trigger_values(cls, values, label):
        rows = []
        base_ms = cls._three_external_interval_base_ms()
        for index, item in enumerate(values):
            if isinstance(item, tuple):
                row_index, value = item
            else:
                row_index, value = index, item
            ts_ns = (base_ms + row_index * cls.THREE_EXT_INTERVAL_STEP_MS) * 1_000_000
            rows.append(f"({ts_ns}, {value})")
        tdLog.info(
            f"inserting {label} trigger rows: rows={len(rows)}, "
            f"table={cls.DB}.same_t"
        )
        tdSql.execute(f"INSERT INTO {cls.DB}.same_t VALUES {', '.join(rows)}")

    @classmethod
    def _insert_three_external_interval_warmup_rows(cls):
        seed_ms = cls._three_external_interval_base_ms() - 2 * cls.THREE_EXT_INTERVAL_STEP_MS
        close_ms = seed_ms + cls.THREE_EXT_INTERVAL_STEP_MS
        rows = [
            f"({seed_ms * 1_000_000}, -2)",
            f"({close_ms * 1_000_000}, -1)",
        ]
        tdLog.info(
            f"inserting interval warmup trigger rows: rows={len(rows)}, "
            f"seed_ts={cls._timestamp(seed_ms)}, close_ts={cls._timestamp(close_ms)}"
        )
        tdSql.execute(f"INSERT INTO {cls.DB}.same_t VALUES {', '.join(rows)}")

    @classmethod
    def _wait_three_external_interval_sink(
        cls, expected_rows, timeout=30, lower_bound=None, upper_bound=None, sink=None
    ):
        if sink is None:
            sink = cls.THREE_EXT_INTERVAL_SINK
        if lower_bound is None:
            lower_bound = cls._timestamp(cls._three_external_interval_base_ms())
        if upper_bound is None:
            upper_bound = cls._timestamp(
                cls._three_external_interval_base_ms()
                + 10 * cls.THREE_EXT_INTERVAL_STEP_MS
            )
        sql = (
            "SELECT wstart, row_count, mysql_sum, pg_sum, influx_sum "
            f"FROM {cls.DB}.{sink} "
            f"WHERE wstart >= '{lower_bound}' AND wstart < '{upper_bound}' "
            "ORDER BY wstart"
        )
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                result = tdSql.getResult(sql)
                last = cls._normalize_rows(result)
                if last == expected_rows:
                    tdLog.info(
                        f"three-external trigger sink matched: "
                        f"table={sink}, rows={last}"
                    )
                    return
            except Exception as error:
                tdLog.info(
                    f"three-external trigger sink not ready: "
                    f"table={sink}, error={error}"
                )
            time.sleep(0.5)
        raise TimeoutError(
            f"three-external trigger sink did not match for "
            f"{sink}: "
            f"got={last}, expected={expected_rows}"
        )

    @classmethod
    def _three_external_stream_status(cls, stream_name):
        try:
            result = tdSql.getResult(
                "SELECT status, message FROM information_schema.ins_streams "
                f"WHERE db_name='{cls.DB}' AND stream_name='{stream_name}'"
            )
            return cls._normalize_rows(result)
        except Exception as error:
            return f"status query failed: {error}"

    @classmethod
    def _wait_three_external_interval_bootstrap(cls, stream_name):
        tdLog.info(f"waiting for external reader bootstrap to finish, stream={stream_name}")
        # Keep the original 40s lower bound, but poll the stream status and allow up to 60s on
        # slow CI machines instead of racing the reader's first history-cutoff sample.
        lower_bound = time.time() + 40
        deadline = time.time() + 60
        while time.time() < deadline:
            time.sleep(5)
            try:
                status = cls._three_external_stream_status(stream_name)
                if status:
                    tdLog.debug(f"stream status during bootstrap: {status}")
            except Exception as error:
                tdLog.info(f"bootstrap status poll failed: {error}")
            if time.time() >= lower_bound:
                return
        tdLog.info("bootstrap wait reached its upper bound")

    @classmethod
    def _wait_totals(cls, child_name, expected_rows, timeout=120):
        output_table = f"{cls.SINK}_{child_name}"
        sql = (
            "SELECT first_ts, last_ts, local_sum, mysql_a_sum, mysql_b_sum, "
            f"pg_a_sum, pg_b_sum FROM {cls.DB}.{output_table} "
            "ORDER BY first_ts, last_ts"
        )
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                result = tdSql.getResult(sql)
                last = cls._normalize_rows(result)
                if last == expected_rows:
                    tdLog.info(
                        f"sink rows matched: table={output_table}, rows={last}"
                    )
                    return
            except Exception as error:
                tdLog.info(
                    f"sink rows not ready: table={output_table}, error={error}"
                )
            time.sleep(0.5)
        raise TimeoutError(
            f"sink rows did not match for {output_table}: got={last}, expected={expected_rows}"
        )

    @classmethod
    def _insert_influx_column_rows(cls):
        tdLog.info("inserting local rows for InfluxDB column-reference stream")
        tdSql.execute(
            f"INSERT INTO {cls.DB}.same_t VALUES "
            f"('{cls._timestamp(cls.BASE_MS)}', 701)"
        )

    @classmethod
    def _insert_same_source_two_table_rows(cls):
        tdLog.info("inserting same-source two-external-table rows")
        mysql_rows = []
        mysql_extra_rows = []
        local_rows = []
        for index in range(3):
            remote_ts_text = cls._remote_timestamp(cls.BASE_MS + index * 1000)
            local_ts_text = cls._timestamp(cls.BASE_MS + index * 1000)
            mysql_rows.append(f"('{remote_ts_text}', {100 + index})")
            mysql_extra_rows.append(f"('{remote_ts_text}', {1000 + index})")
            local_rows.append(f"('{local_ts_text}', {10 + index})")

        ExtSrcEnv.mysql_exec(
            cls.MYSQL_DB,
            [f"INSERT INTO same_t VALUES {', '.join(mysql_rows)}"],
        )
        ExtSrcEnv.mysql_exec(
            cls.MYSQL_DB,
            [f"INSERT INTO same_t_extra VALUES {', '.join(mysql_extra_rows)}"],
        )
        tdSql.execute(f"INSERT INTO {cls.DB}.same_t VALUES {', '.join(local_rows)}")

    @classmethod
    def _wait_influx_column_sink(cls, child_name, expected_rows, timeout=30):
        output_table = f"influx_col_sink_{child_name}"
        sql = (
            f"SELECT ts, local_sum, influx_load_sum FROM {cls.DB}.{output_table} "
            "ORDER BY ts"
        )
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                result = tdSql.getResult(sql)
                last = cls._normalize_rows(result)
                if last == expected_rows:
                    tdLog.info(
                        f"InfluxDB column sink matched: table={output_table}, rows={last}"
                    )
                    return
            except Exception as error:
                tdLog.info(
                    f"InfluxDB column sink not ready: table={output_table}, error={error}"
                )
            time.sleep(0.5)
        raise TimeoutError(
            f"InfluxDB column sink did not match for {output_table}: "
            f"got={last}, expected={expected_rows}"
        )

    @classmethod
    def _wait_same_source_two_table_sink(cls, expected_rows, timeout=60):
        sql = (
            "SELECT ts, tick_sum, mysql_a_sum, mysql_extra_sum "
            f"FROM {cls.DB}.{cls.SAME_SOURCE_TWO_TABLE_SINK} ORDER BY ts"
        )
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                result = tdSql.getResult(sql)
                last = cls._normalize_rows(result)
                if last == expected_rows:
                    tdLog.info(f"same-source two-table sink matched: rows={last}")
                    return
            except Exception as error:
                tdLog.info(f"same-source two-table sink not ready: error={error}")
            time.sleep(0.5)
        raise TimeoutError(
            f"same-source two-table sink did not match: got={last}, expected={expected_rows}"
        )

    @classmethod
    def _assert_totals_stable(cls, child_name, expected_rows, duration=3):
        deadline = time.time() + duration
        while time.time() < deadline:
            cls._wait_totals(child_name, expected_rows, timeout=1)
            time.sleep(0.5)

    # Timestamp rendering rules for this case (the container timezone must not
    # change the data written or verified):
    #   * SQL literals sent to TDengine are parsed by the server in the session
    #     timezone, which setup_class pins to UTC ("set timezone 'UTC'").
    #   * Timezone-naive external columns (MySQL DATETIME, PG timestamp) are
    #     decoded by the stream external reader with the *stream* client
    #     timezone, i.e. the session timezone captured by CREATE STREAM, so
    #     their literals must be rendered in UTC as well (see
    #     _remote_timestamp); rendering them in the OS local zone shifts every
    #     external row by the local UTC offset.
    #   * Values returned by tdSql are converted with datetime.fromtimestamp(),
    #     i.e. the OS local timezone, so expected rows must be rendered with
    #     _local_timestamp.
    @classmethod
    def _timestamp(cls, value_ms):
        """Render an epoch (ms) as a UTC wall clock (session/stream timezone)."""
        return datetime.datetime.fromtimestamp(
            value_ms / 1000, datetime.timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    @classmethod
    def _remote_timestamp(cls, value_ms):
        """Render a timestamp literal for a timezone-naive remote column.

        MySQL DATETIME / PG timestamp carry no offset and are decoded with the
        stream client timezone, so they must spell the wall clock of that
        timezone (UTC here) instead of the OS local one.
        """
        return cls._timestamp(value_ms)

    @classmethod
    def _local_timestamp(cls, value_ms):
        """Render an epoch (ms) in the OS local timezone (tdSql output form)."""
        return datetime.datetime.fromtimestamp(value_ms / 1000.0).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3]

    @classmethod
    def _sink_row(cls, offset_ms, local_value, mysql_a, mysql_b, pg_a, pg_b):
        ts = cls._local_timestamp(cls.BASE_MS + offset_ms)
        return (ts, ts, local_value, mysql_a, mysql_b, pg_a, pg_b)

    @classmethod
    def _three_external_interval_sink_row(cls, index):
        ts = cls._local_timestamp(
            cls._three_external_interval_base_ms()
            + index * cls.THREE_EXT_INTERVAL_STEP_MS
        )
        return (ts, 1, 100 + index, 1000 + index, 10000 + index)

    @classmethod
    def _three_external_trigger_sink_row(cls, start_index, count):
        base_ms = (
            cls.three_ext_interval_base_ms
            if cls.three_ext_interval_base_ms is not None
            else cls.BASE_MS
        )
        ts = cls._local_timestamp(
            base_ms + start_index * cls.THREE_EXT_INTERVAL_STEP_MS
        )
        return (
            ts,
            count,
            sum(100 + index for index in range(start_index, start_index + count)),
            sum(1000 + index for index in range(start_index, start_index + count)),
            sum(10000 + index for index in range(start_index, start_index + count)),
        )

    @classmethod
    def _three_external_interval_base_ms(cls):
        if cls.three_ext_interval_base_ms is None:
            raise RuntimeError("three-external interval base timestamp is not set")
        return cls.three_ext_interval_base_ms

    @classmethod
    def _normalize_rows(cls, rows):
        return [tuple(cls._normalize_cell(cell) for cell in row) for row in rows]

    @classmethod
    def _normalize_cell(cls, value):
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        if isinstance(value, int) and not isinstance(value, bool) and abs(value) > 10 ** 15:
            return cls._local_timestamp(value // 1_000_000)
        return value

    @classmethod
    def _verify_creation_failures(cls):
        tdLog.info("verifying external virtual reference creation failures")
        tdSql.error(
            f"CREATE VTABLE {cls.DB}.bad_source (ts TIMESTAMP, "
            "value_a INT FROM no_such_source.no_db.no_table.value_a)"
        )
        tdSql.error(
            f"CREATE VTABLE {cls.DB}.bad_table (ts TIMESTAMP, "
            f"value_a INT FROM {cls.MYSQL_SOURCE}.{cls.MYSQL_DB}.no_table.value_a)"
        )
        tdSql.error(
            f"CREATE VTABLE {cls.DB}.bad_column (ts TIMESTAMP, "
            f"value_a INT FROM {cls.PG_SOURCE}.{cls.PG_DB}.same_t.no_column)"
        )
        tdSql.error(
            f"CREATE VTABLE {cls.DB}.bad_time (ts TIMESTAMP, "
            f"value_a INT FROM {cls.MYSQL_SOURCE}.{cls.MYSQL_DB}.missing_ts.value_a)"
        )
        tdSql.error(
            f"CREATE VTABLE {cls.DB}.bad_type (ts TIMESTAMP, "
            f"value_a INT FROM {cls.PG_SOURCE}.{cls.PG_DB}.bad_type.payload)"
        )
        tdLog.info("external virtual reference creation failures were rejected")

    @classmethod
    def _verify_history_guard(cls):
        tdLog.info("verifying fill-history guard for external virtual references")
        tdSql.execute(
            f"CREATE STREAM {cls.DB}.history_guard COUNT_WINDOW(1) "
            f"FROM {cls.DB}.{cls.VSTB} PARTITION BY tbname "
            "STREAM_OPTIONS(FILL_HISTORY('2025-01-01 00:00:00')) "
            f"INTO {cls.DB}.history_sink AS SELECT first(_c0), "
            "sum(mysql_a) FROM %%trows"
        )
        deadline = time.time() + 60
        last = None
        while time.time() < deadline:
            try:
                tdSql.query(
                    "SELECT status, message FROM information_schema.ins_streams "
                    f"WHERE db_name='{cls.DB}' AND stream_name='history_guard'"
                )
                if tdSql.queryRows == 1:
                    last = (tdSql.getData(0, 0), tdSql.getData(0, 1))
                    if str(last[0]).lower() == "failed":
                        assert "FILL_HISTORY/FILL_HISTORY_FIRST is not supported" in str(
                            last[1]
                        )
                        tdLog.info(
                            f"external virtual history guard reached failed state: message={last[1]}"
                        )
                        return
            except Exception as error:
                tdLog.info(f"history guard status not ready: error={error}")
            time.sleep(1)
        raise AssertionError(
            f"external virtual history guard did not fail, last={last}"
        )

    def do_mixed_external_realtime_lifecycle(self):
        self._prepare_fixture()
        try:
            self._verify_creation_failures()
            self._verify_history_guard()
            self._create_stream()
            self._wait_three_external_interval_bootstrap(self.STREAM)

            self._insert_phase(
                0,
                (1, 2),
                ((10, 100), (20, 200)),
                ((1000, 10000), (2000, 20000)),
                pg_shift_ms=1000,
            )
            initial = [
                self._sink_row(0, 1, 10, 100, None, None),
                self._sink_row(1000, None, None, None, 1000, 10000),
                self._sink_row(10000, 2, 20, 200, None, None),
                self._sink_row(11000, None, None, None, 2000, 20000),
            ]
            self._wait_totals(self.VTABLE_A, initial)
            self._wait_totals(self.VTABLE_B, initial)
            tdLog.info("patching one virtual target from PostgreSQL to MySQL identity")
            tdSql.execute(
                f"ALTER VTABLE {self.DB}.{self.VTABLE_B} ALTER COLUMN pg_a SET "
                f"{self.MYSQL_SOURCE}.{self.MYSQL_DB}.same_t.value_a"
            )
            tdStream.checkStreamStatus(self.STREAM)

            self._insert_phase(
                1,
                (3, 4),
                ((30, 300), (40, 400)),
                ((3000, 30000), (4000, 40000)),
            )
            phase1_a = initial + [
                self._sink_row(20000, 3, 30, 300, 3000, 30000),
                self._sink_row(30000, 4, 40, 400, 4000, 40000),
            ]
            phase1_b = initial + [
                self._sink_row(20000, 3, 30, 300, 30, 30000),
                self._sink_row(30000, 4, 40, 400, 40, 40000),
            ]
            self._wait_totals(self.VTABLE_A, phase1_a)
            self._wait_totals(self.VTABLE_B, phase1_b)

            tdLog.info("patching one virtual target from internal to MySQL identity")
            tdSql.execute(
                f"ALTER VTABLE {self.DB}.{self.VTABLE_A} ALTER COLUMN local_value SET "
                f"{self.MYSQL_SOURCE}.{self.MYSQL_DB}.same_t.value_a"
            )
            tdStream.checkStreamStatus(self.STREAM)

            tdLog.info("forcing checkpoint recovery with stop/start stream")
            tdSql.execute(f"STOP STREAM {self.DB}.{self.STREAM}")
            tdSql.execute(f"START STREAM {self.DB}.{self.STREAM}")
            tdStream.checkStreamStatus(self.STREAM)

            self._insert_phase(
                2,
                (5, 6),
                ((50, 500), (60, 600)),
                ((5000, 50000), (6000, 60000)),
            )
            expected_a = phase1_a + [
                self._sink_row(40000, 50, 50, 500, 5000, 50000),
                self._sink_row(50000, 60, 60, 600, 6000, 60000),
            ]
            expected_b = phase1_b + [
                self._sink_row(40000, 5, 50, 500, 50, 50000),
                self._sink_row(50000, 6, 60, 600, 60, 60000),
            ]
            self._wait_totals(self.VTABLE_A, expected_a)
            self._wait_totals(self.VTABLE_B, expected_b)

            self._insert_late_rows()
            self._assert_totals_stable(self.VTABLE_A, expected_a)
            self._assert_totals_stable(self.VTABLE_B, expected_b)
            tdLog.info("mixed virtual external realtime lifecycle [ passed ]")
            tdLog.success(f"{__file__} successfully executed")
        finally:
            self._cleanup_fixture()

    def do_same_source_two_external_tables(self):
        self._prepare_same_source_two_table_fixture()
        try:
            tdLog.info("creating same-source two-external-table virtual stream")
            tdSql.execute(
                f"CREATE STREAM {self.DB}.{self.SAME_SOURCE_TWO_TABLE_STREAM} "
                "COUNT_WINDOW(1) "
                f"FROM {self.DB}.{self.SAME_SOURCE_TWO_TABLE_VTABLE} "
                "STREAM_OPTIONS(IGNORE_DISORDER) "
                f"INTO {self.DB}.{self.SAME_SOURCE_TWO_TABLE_SINK} "
                "AS SELECT first(_c0) AS ts, sum(tick) AS tick_sum, "
                "sum(mysql_a) AS mysql_a_sum, "
                "sum(mysql_extra) AS mysql_extra_sum FROM %%trows"
            )
            tdStream.checkStreamStatus(self.SAME_SOURCE_TWO_TABLE_STREAM)
            self._wait_three_external_interval_bootstrap(self.SAME_SOURCE_TWO_TABLE_STREAM)
            self._insert_same_source_two_table_rows()
            expected_rows = [
                (
                    self._local_timestamp(self.BASE_MS + index * 1000),
                    10 + index,
                    100 + index,
                    1000 + index,
                )
                for index in range(3)
            ]
            self._wait_same_source_two_table_sink(expected_rows)
            tdLog.info("same-source two-external-table virtual stream [ passed ]")
        finally:
            self._cleanup_fixture()

    @classmethod
    def _prepare_var_width_fixture(cls):
        cls._cleanup_fixture()
        tdLog.info("creating var-width virtual column fixture objects")

        create_remote_db("mysql", cls.MYSQL_DB)
        # Single-byte charset keeps the mapping as VARCHAR (bytes = 32 + var header),
        # while the virtual column declares VARCHAR(64): a legal wider reference.
        create_mysql_table(
            cls.MYSQL_DB,
            "var_t",
            "ts DATETIME(3) NOT NULL PRIMARY KEY, "
            "payload VARCHAR(32) CHARACTER SET latin1",
        )
        warmup_ts = cls._remote_timestamp(cls.BASE_MS - 2000)
        ExtSrcEnv.mysql_exec(
            cls.MYSQL_DB,
            [f"INSERT INTO var_t VALUES ('{warmup_ts}', 'warmup')"],
        )
        create_ext_source(cls.MYSQL_SOURCE, "mysql", cls.MYSQL_DB)

        tdSql.execute(f"CREATE DATABASE {cls.DB} VGROUPS 1 PRECISION 'ms'")
        tdSql.execute(
            f"CREATE STABLE {cls.DB}.{cls.VAR_WIDTH_VSTB} "
            "(ts TIMESTAMP, payload VARCHAR(64)) "
            "TAGS (site NCHAR(16)) VIRTUAL 1"
        )
        tdSql.execute(
            f"CREATE VTABLE {cls.DB}.{cls.VAR_WIDTH_VTABLE} ("
            f"payload FROM {cls.MYSQL_SOURCE}.{cls.MYSQL_DB}.var_t.payload) "
            f"USING {cls.DB}.{cls.VAR_WIDTH_VSTB} TAGS('site_w')"
        )
        tdLog.info("var-width virtual column fixture is ready")

    @classmethod
    def _insert_var_width_rows(cls):
        rows = [
            f"('{cls._remote_timestamp(cls.BASE_MS + index * 1000)}', "
            f"'payload_{index}')"
            for index in range(3)
        ]
        tdLog.info(f"inserting var-width external rows: rows={len(rows)}")
        ExtSrcEnv.mysql_exec(
            cls.MYSQL_DB,
            [f"INSERT INTO var_t VALUES {', '.join(rows)}"],
        )

    @classmethod
    def _wait_var_width_sink(cls, expected_rows, timeout=60):
        sql = (
            f"SELECT ts, payload FROM {cls.DB}.{cls.VAR_WIDTH_SINK} "
            "ORDER BY ts"
        )
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                result = tdSql.getResult(sql)
                last = cls._normalize_rows(result)
                if last == expected_rows:
                    tdLog.info(f"var-width sink matched: rows={last}")
                    return
            except Exception as error:
                tdLog.info(f"var-width sink not ready: error={error}")
            time.sleep(0.5)
        raise TimeoutError(
            f"var-width sink did not match: got={last}, expected={expected_rows}"
        )

    def do_var_width_column_reference(self):
        self._prepare_var_width_fixture()
        try:
            tdLog.info("creating var-width virtual column stream")
            tdSql.execute(
                f"CREATE STREAM {self.DB}.{self.VAR_WIDTH_STREAM} "
                "COUNT_WINDOW(1) "
                f"FROM {self.DB}.{self.VAR_WIDTH_VSTB} PARTITION BY tbname "
                "STREAM_OPTIONS(IGNORE_DISORDER) "
                f"INTO {self.DB}.{self.VAR_WIDTH_SINK} "
                "AS SELECT first(_c0) AS ts, first(payload) AS payload "
                "FROM %%trows"
            )
            tdStream.checkStreamStatus(self.VAR_WIDTH_STREAM)
            self._wait_three_external_interval_bootstrap(self.VAR_WIDTH_STREAM)
            self._insert_var_width_rows()
            expected_rows = [
                (
                    self._local_timestamp(self.BASE_MS + index * 1000),
                    f"payload_{index}",
                )
                for index in range(3)
            ]
            self._wait_var_width_sink(expected_rows)
            tdLog.info("var-width virtual column stream [ passed ]")
        finally:
            self._cleanup_fixture()

    @classmethod
    def _prepare_quoted_pg_fixture(cls):
        cls._cleanup_fixture()
        tdLog.info("creating quoted PostgreSQL identifier stream fixture")
        create_remote_db("postgresql", cls.PG_DB)
        # Quoted identifiers keep their exact case in PostgreSQL; the virtual table reference
        # must render them as quoted identifiers again, or the reader's generated SQL would
        # resolve the unquoted form to lower case and fail to fetch any rows.
        ExtSrcEnv.pg_exec(
            cls.PG_DB,
            [
                'DROP TABLE IF EXISTS public."QuotedCase"',
                'CREATE TABLE public."QuotedCase" '
                '(ts TIMESTAMP(3) PRIMARY KEY, "MixedValue" INTEGER)',
            ],
        )
        create_ext_source(cls.PG_SOURCE, "postgresql", cls.PG_DB)

        tdSql.execute(f"CREATE DATABASE {cls.DB} VGROUPS 1 PRECISION 'ms'")
        tdSql.execute(
            f"CREATE STABLE {cls.DB}.{cls.QUOTED_PG_VSTB} "
            "(ts TIMESTAMP, quoted_v INT) TAGS (site NCHAR(16)) VIRTUAL 1"
        )
        tdSql.execute(
            f"CREATE VTABLE {cls.DB}.{cls.QUOTED_PG_VTABLE} ("
            f"quoted_v FROM {cls.PG_SOURCE}.{cls.PG_DB}.`QuotedCase`.`MixedValue`) "
            f"USING {cls.DB}.{cls.QUOTED_PG_VSTB} TAGS('site_all')"
        )
        tdLog.info("quoted PostgreSQL identifier fixture is ready")

    @classmethod
    def _insert_quoted_pg_rows(cls):
        rows = [
            f"('{cls._remote_timestamp(cls.BASE_MS + index * cls.QUOTED_PG_STEP_MS)}', "
            f"{10 + index})"
            for index in range(3)
        ]
        tdLog.info(f"inserting quoted-PG external rows: rows={len(rows)}")
        ExtSrcEnv.pg_exec(
            cls.PG_DB,
            [f'INSERT INTO public."QuotedCase" VALUES {", ".join(rows)}'],
        )

    @classmethod
    def _wait_quoted_pg_sink(cls, expected_rows, timeout=90):
        sql = (
            f"SELECT first_ts, quoted_v, row_count, quoted_sum "
            f"FROM {cls.DB}.{cls.QUOTED_PG_SINK} ORDER BY first_ts"
        )
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                result = tdSql.getResult(sql)
                last = cls._normalize_rows(result)
                if last == expected_rows:
                    tdLog.info(f"quoted-PG sink matched: rows={last}")
                    return
            except Exception as error:
                tdLog.info(f"quoted-PG sink not ready: error={error}")
            time.sleep(0.5)
        raise TimeoutError(
            f"quoted-PG sink did not match: got={last}, expected={expected_rows}"
        )

    def do_quoted_pg_identifier_stream(self):
        self._prepare_quoted_pg_fixture()
        try:
            tdLog.info("creating quoted PostgreSQL identifier virtual stream")
            tdSql.execute(
                f"CREATE STREAM {self.DB}.{self.QUOTED_PG_STREAM} "
                "COUNT_WINDOW(1) "
                f"FROM {self.DB}.{self.QUOTED_PG_VSTB} PARTITION BY tbname "
                "STREAM_OPTIONS(IGNORE_DISORDER) "
                f"INTO {self.DB}.{self.QUOTED_PG_SINK} "
                "(first_ts, quoted_v, row_count, quoted_sum) AS SELECT "
                "first(_c0), first(quoted_v), count(*), sum(quoted_v) FROM %%trows"
            )
            tdStream.checkStreamStatus(self.QUOTED_PG_STREAM)
            self._wait_three_external_interval_bootstrap(self.QUOTED_PG_STREAM)
            self._insert_quoted_pg_rows()
            expected_rows = [
                (
                    self._local_timestamp(self.BASE_MS + index * self.QUOTED_PG_STEP_MS),
                    10 + index,
                    1,
                    10 + index,
                )
                for index in range(3)
            ]
            self._wait_quoted_pg_sink(expected_rows)
            tdLog.info("quoted PostgreSQL identifier virtual stream [ passed ]")
        finally:
            self._cleanup_fixture()

    def do_three_external_interval_stream(self):
        self._prepare_three_external_interval_fixture()
        try:
            self._create_three_external_trigger_stream(
                self.THREE_EXT_INTERVAL_STREAM,
                self.THREE_EXT_INTERVAL_SINK,
                "INTERVAL(1s) SLIDING(1s)",
                "_twstart",
            )
            self._insert_three_external_interval_warmup_rows()
            self._wait_three_external_interval_bootstrap(self.THREE_EXT_INTERVAL_STREAM)
            self._insert_three_external_interval_remote_rows()
            self._insert_three_external_interval_rows()
            expected_rows = [
                self._three_external_interval_sink_row(index) for index in range(10)
            ]
            self._wait_three_external_interval_sink(expected_rows)
            tdLog.info("three-external-source interval virtual stream [ passed ]")
        finally:
            self._cleanup_fixture()

    def _create_three_external_trigger_stream(
        self,
        stream_name,
        sink_name,
        trigger_clause,
        wstart_expr,
        stream_options="IGNORE_NODATA_TRIGGER|IGNORE_DISORDER",
        partition_clause="",
        source_name=None,
    ):
        if source_name is None:
            source_name = self.THREE_EXT_INTERVAL_VSTB
        tdLog.info(
            f"creating three-external-source trigger virtual stream: "
            f"stream={stream_name}, sink={sink_name}, trigger={trigger_clause}"
        )
        tdSql.execute(
            f"CREATE STREAM {self.DB}.{stream_name} "
            f"{trigger_clause} "
            f"FROM {self.DB}.{source_name} "
            f"{partition_clause} "
            f"STREAM_OPTIONS({stream_options}) "
            f"INTO {self.DB}.{sink_name} "
            f"AS SELECT {wstart_expr} AS wstart, count(*) AS row_count, "
            "sum(tick) AS tick_sum, sum(mysql_v) AS mysql_sum, "
            "sum(pg_v) AS pg_sum, sum(influx_v) AS influx_sum FROM %%trows"
        )
        tdStream.checkStreamStatus(stream_name)

    def _run_three_external_trigger_stream(
        self,
        stream_name,
        sink_name,
        trigger_clause,
        wstart_expr,
        trigger_values,
        expected_rows,
        label,
        stream_options="IGNORE_NODATA_TRIGGER|IGNORE_DISORDER",
        partition_clause="",
        timeout=30,
        base_ms=None,
        source_name=None,
    ):
        self._prepare_three_external_interval_fixture(base_ms=base_ms)
        try:
            self._create_three_external_trigger_stream(
                stream_name,
                sink_name,
                trigger_clause,
                wstart_expr,
                stream_options,
                partition_clause,
                source_name,
            )
            self._insert_three_external_interval_warmup_rows()
            self._wait_three_external_interval_bootstrap(stream_name)
            self._insert_three_external_interval_remote_rows()
            self._insert_three_external_interval_trigger_values(trigger_values, label)
            try:
                self._wait_three_external_interval_sink(
                    expected_rows, sink=sink_name, timeout=timeout
                )
            except Exception as error:
                status = self._three_external_stream_status(stream_name)
                raise AssertionError(
                    f"three-external-source {label} stream did not produce "
                    f"expected rows: status={status}, error={error}"
                )
            tdLog.info(
                f"three-external-source {label} virtual stream [ passed ]"
            )
        finally:
            self._cleanup_fixture()

    def do_three_external_sliding_stream(self):
        expected_rows = [
            self._three_external_trigger_sink_row(index, 1) for index in range(10)
        ]
        self._run_three_external_trigger_stream(
            self.THREE_EXT_SLIDING_STREAM,
            self.THREE_EXT_SLIDING_SINK,
            "SLIDING(1s)",
            "_tcurrent_ts",
            list(range(1, 13)),
            expected_rows,
            "sliding",
        )

    def do_three_external_session_stream(self):
        expected_rows = [
            self._three_external_trigger_sink_row(index, 1) for index in range(10)
        ]
        self._run_three_external_trigger_stream(
            self.THREE_EXT_SESSION_STREAM,
            self.THREE_EXT_SESSION_SINK,
            "SESSION(ts, 500a)",
            "_twstart",
            list(range(1, 13)),
            expected_rows,
            "session",
            "IGNORE_DISORDER",
            "",
            60,
            None,
            self.THREE_EXT_INTERVAL_VTABLE,
        )

    def do_three_external_state_stream(self):
        expected_rows = [
            self._three_external_trigger_sink_row(0, 2),
            self._three_external_trigger_sink_row(2, 3),
            self._three_external_trigger_sink_row(5, 2),
        ]
        self._run_three_external_trigger_stream(
            self.THREE_EXT_STATE_STREAM,
            self.THREE_EXT_STATE_SINK,
            "STATE_WINDOW(tick)",
            "_twstart",
            [1, 1, 2, 2, 2, 3, 3, 4],
            expected_rows,
            "state_window",
            "IGNORE_DISORDER",
            "PARTITION BY tbname",
        )

    def do_three_external_event_stream(self):
        expected_rows = [self._three_external_trigger_sink_row(0, 5)]
        self._run_three_external_trigger_stream(
            self.THREE_EXT_EVENT_STREAM,
            self.THREE_EXT_EVENT_SINK,
            "EVENT_WINDOW(START WITH tick > 0 END WITH tick > 4)",
            "_twstart",
            [1, 2, 3, 4, 5, -1, -1],
            expected_rows,
            "event_window",
            "IGNORE_DISORDER",
            "PARTITION BY tbname",
        )

    def do_influx_column_reference_stream(self):
        self._prepare_influx_column_fixture()
        try:
            tdLog.info("verifying InfluxDB external column references")
            # tdSql.query(
            #     f"SELECT site, influx_load FROM {self.DB}.influx_col_vstb ORDER BY site"
            # )
            # tdSql.checkRows(2)
            # tdSql.checkData(0, 0, "site_a")
            # tdSql.checkData(0, 1, 101)
            # tdSql.checkData(1, 0, "site_b")
            # tdSql.checkData(1, 1, 201)

            tdLog.info("creating InfluxDB column-reference virtual stream")
            tdSql.execute(
                f"CREATE STREAM {self.DB}.influx_col_stream COUNT_WINDOW(1) "
                f"FROM {self.DB}.influx_col_vstb PARTITION BY tbname "
                f"INTO {self.DB}.influx_col_sink "
                "OUTPUT_SUBTABLE(CONCAT('influx_col_sink_', tbname)) "
                "AS SELECT first(_c0) as ts, sum(local_value) as local_sum, sum(influx_load) as influx_load_sum FROM %%trows"
            )
            tdStream.checkStreamStatus("influx_col_stream")
            self._wait_three_external_interval_bootstrap("influx_col_stream")

            self._insert_influx_column_rows()
            self._wait_influx_column_sink(
                "influx_col_a",
                [(self._local_timestamp(self.BASE_MS), 701, 101)],
            )
            self._wait_influx_column_sink(
                "influx_col_b",
                [(self._local_timestamp(self.BASE_MS), 701, 201)],
            )
            tdLog.info("InfluxDB column-reference virtual stream [ passed ]")
        finally:
            self._cleanup_fixture()

    def test_virtual_stream_three_external_interval(self):
        """Stream: interval trigger with MySQL, PostgreSQL, and InfluxDB virtual columns

        1. Create one virtual table whose data columns come from MySQL, PostgreSQL,
           and InfluxDB at the same timestamps.
        2. Create an INTERVAL stream over the virtual super table.
        3. Insert twelve trigger rows and verify the first ten closed interval
           windows in the result table.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-08-17 Wang Mingming Add three-source interval coverage
        """
        self.do_three_external_interval_stream()

    def test_virtual_stream_three_external_sliding(self):
        """Stream: sliding trigger with MySQL, PostgreSQL, and InfluxDB virtual columns

        1. Create one virtual table whose data columns come from MySQL, PostgreSQL,
           and InfluxDB at the same timestamps.
        2. Create a SLIDING stream over the virtual super table.
        3. Insert trigger rows and verify closed sliding windows in the result table.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-08-18 Wang Mingming Add three-source sliding coverage
        """
        self.do_three_external_sliding_stream()

    def test_virtual_stream_three_external_session(self):
        """Stream: session trigger with MySQL, PostgreSQL, and InfluxDB virtual columns

        1. Create one virtual table whose data columns come from MySQL, PostgreSQL,
           and InfluxDB at the same timestamps.
        2. Create a SESSION stream over the virtual super table.
        3. Insert gapped trigger rows and verify closed session windows.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-08-18 Wang Mingming Add three-source session coverage
        """
        self.do_three_external_session_stream()

    def test_virtual_stream_three_external_state_window(self):
        """Stream: state-window trigger with MySQL, PostgreSQL, and InfluxDB virtual columns

        1. Create one virtual table whose data columns come from MySQL, PostgreSQL,
           and InfluxDB at the same timestamps.
        2. Create a STATE_WINDOW stream over the virtual super table.
        3. Insert trigger rows with repeated states and verify closed state windows.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-08-18 Wang Mingming Add three-source state-window coverage
        """
        self.do_three_external_state_stream()

    def test_virtual_stream_three_external_event_window(self):
        """Stream: event-window trigger with MySQL, PostgreSQL, and InfluxDB virtual columns

        1. Create one virtual table whose data columns come from MySQL, PostgreSQL,
           and InfluxDB at the same timestamps.
        2. Create an EVENT_WINDOW stream over the virtual super table.
        3. Insert start/end trigger rows and verify the closed event window.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-08-18 Wang Mingming Add three-source event-window coverage
        """
        self.do_three_external_event_stream()

    def test_virtual_stream_same_source_two_external_tables(self):
        """Stream: same external source with two referenced external tables

        1. Create one MySQL external source with two external tables.
        2. Create one virtual table whose columns reference both external tables.
        3. Create a COUNT_WINDOW stream and verify both external table columns
           are bound, pulled, and merged into the result table.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-08-18 Wang Mingming Add same-source two-table coverage
        """
        self.do_same_source_two_external_tables()

    def test_virtual_stream_mixed_external(self):
        """Stream: mixed local, MySQL, PostgreSQL, and InfluxDB virtual references

        1. Reject invalid external source, table, column, timestamp, and type references.
        2. Merge local and two external readers into two virtual target groups.
        3. Resolve virtual table data columns from two InfluxDB tag-filtered series.
        4. Patch one target to a different external source identity while running.
        5. Patch one target from an internal column to an external source identity.
        6. Recover patched reader bindings after a stop/start checkpoint cycle.
        7. Ignore late rows after realtime reader watermarks have advanced.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-08-05 Wang Mingming Add mixed virtual external coverage
            - 2026-08-13 Wang Mingming Add InfluxDB column-reference coverage
        """
        self.do_mixed_external_realtime_lifecycle()
        self.do_influx_column_reference_stream()

    def test_virtual_stream_var_width_column(self):
        """Stream: virtual column wider than the external varchar mapping

        1. Create a MySQL table whose payload column is VARCHAR(32) with a
           single-byte charset (mapped as VARCHAR, bytes=34).
        2. Create a virtual table declaring that column as VARCHAR(64).
        3. Create a COUNT_WINDOW stream over the pure-external virtual table
           and verify the merged payload rows land in the sink intact.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-10-09 Wang Mingming Add var-width column merge coverage
        """
        self.do_var_width_column_reference()

    def test_virtual_stream_quoted_pg_identifier(self):
        """Stream: quoted PostgreSQL table and column identifiers survive reader SQL rendering

        1. Create a PostgreSQL table named "QuotedCase" with a column "MixedValue".
        2. Reference both through backtick-quoted names in a virtual table.
        3. Create a COUNT_WINDOW stream and verify the quoted remote column
           participates in the trigger rows and in the %%trows aggregation.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-10-15 Wang Mingming Add quoted PG identifier stream coverage
        """
        self.do_quoted_pg_identifier_stream()
