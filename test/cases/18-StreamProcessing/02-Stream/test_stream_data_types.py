import time
from new_test_framework.utils import tdLog, tdSql
from new_test_framework.utils.stmt2 import tdStmt2


class TestStreamDatatypes:

    ROW_COUNT = 1000
    BASE_TS = 1700000000000  # 2023-11-14 22:13:20.000 UTC

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_all_datatypes(self):
        """Stream all datatypes correctness

        Verify all supported data types pass through stream processing correctly,
        including VARCHAR/NCHAR/VARBINARY which require varstr header wrapping in setColData.

        Test design:
            1. Insert ROW_COUNT rows into a super table, exactly 1 row per second (1000 ms apart).
            2. Use interval(1s) stream: each 1-second window contains exactly one source row.
            3. last() of a single-row window equals the original row value — no aggregation distortion.
            4. Stream produces ROW_COUNT-1 output rows: the final window is unclosed (no subsequent
               data arrives to trigger it), so it never appears in the stream output.
            5. Compare each stream output row against the corresponding source row, column by column,
               for all 15 data types.

        The key insight: because each 1s window holds exactly one data point, last(col) == col.
        Any data corruption, type conversion bug, or missing varstr header in setColData is
        directly detected by the exact-match comparison between source table and stream output.

        Covered types: bool, tinyint, smallint, int, bigint, tinyint unsigned, smallint unsigned,
        int unsigned, bigint unsigned, float, double, varchar, nchar, varbinary

        Data insertion uses the STMT2 (parameter-binding) API because the bug is STMT2-specific:
        STMT2 submits data with SUBMIT_REQ_COLUMN_DATA_FORMAT, which takes the column-data branch
        in vnodeStream.c and calls setColData() on each SColData block.  Plain SQL submits data
        in row format (SRow) and takes a completely different branch that never calls setColData(),
        so plain SQL cannot trigger this bug.  Using STMT2 is required to reproduce the crash.

        Since: v3.4.1.10

        Labels: common,ci

        Jira: https://project.feishu.cn/taosdata_td/defect/detail/6996748560

        History:
            - 2026-5-23 Alex Duan Created

        """
        self.prepare_env()
        self.insert_data()
        self.verify_data()

    def prepare_env(self):
        tdLog.info("prepare environment")
        sqls = [
            "drop database if exists db_dtype;",
            "create database db_dtype vgroups 1 precision 'ms';",
            "use db_dtype;",
            "create snode on dnode 1;",
            # super table with all supported data types
            """create table stb (
                ts timestamp,
                c_bool bool,
                c_tinyint tinyint,
                c_smallint smallint,
                c_int int,
                c_bigint bigint,
                c_utinyint tinyint unsigned,
                c_usmallint smallint unsigned,
                c_uint int unsigned,
                c_ubigint bigint unsigned,
                c_float float,
                c_double double,
                c_varchar varchar(128),
                c_nchar nchar(128),
                c_varbinary varbinary(128)
            ) tags (t_id int);""",
            "create table t1 using stb tags(1);",
            # interval(1s) sliding(1s): tumbling 1-second windows — one row per window,
            # so last(col) == the original column value.
            # _twstart+0s casts the window-start to timestamp for the output ts column.
            # The WHERE clause constrains each per-window subquery to its own time range.
            """create stream stream_dtype_last
                interval(1s) sliding(1s) from stb
                into stream_out_last as
                select _twstart+0s as ts,
                    last(c_bool)      as r_bool,
                    last(c_tinyint)   as r_tinyint,
                    last(c_smallint)  as r_smallint,
                    last(c_int)       as r_int,
                    last(c_bigint)    as r_bigint,
                    last(c_utinyint)  as r_utinyint,
                    last(c_usmallint) as r_usmallint,
                    last(c_uint)      as r_uint,
                    last(c_ubigint)   as r_ubigint,
                    last(c_float)     as r_float,
                    last(c_double)    as r_double,
                    last(c_varchar)   as r_varchar,
                    last(c_nchar)     as r_nchar,
                    last(c_varbinary) as r_varbinary
                from %%trows""",
        ]
        tdSql.executes(sqls)

        # wait for streams to be running
        self._wait_stream_ready("stream_dtype_last")
        tdLog.info("stream is running")

    def insert_data(self):
        """Insert ROW_COUNT rows via STMT2 at 1-second intervals with varied values.

        Using STMT2 (parameter-binding API) is required to reproduce the original crash:
        STMT2 submits data with SUBMIT_REQ_COLUMN_DATA_FORMAT; the stream engine processes it
        through the column-data branch in vnodeStream.c, calling setColData() which had the
        missing varstr header bug.  Plain SQL submits row-format data (SRow) and takes a
        different branch that never calls setColData() — so plain SQL cannot trigger this bug.
        """
        tdLog.info(f"inserting {self.ROW_COUNT} rows via STMT2 (1 row per second)")

        sql = "INSERT INTO db_dtype.t1 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        batch_params = []
        for i in range(self.ROW_COUNT):
            ts          = self.BASE_TS + i * 1000
            c_bool      = bool(i % 2 == 0)            # True / False
            c_tinyint   = (i % 127) + 1               # 1-127
            c_smallint  = (i % 32767) + 1             # 1-32767
            c_int       = i + 1                       # 1-1000
            c_bigint    = (i + 1) * 1000000           # 1000000-1000000000
            c_utinyint  = i % 256                     # 0-255
            c_usmallint = i % 65536                   # 0-65535
            c_uint      = i                           # 0-999
            c_ubigint   = i * 1000                    # 0-999000
            # i + 0.5 / i + 0.25 are exact binary fractions — no float-precision loss
            c_float     = float(i) + 0.5
            c_double    = float(i) + 0.25
            c_varchar   = f"varchar_{i}"
            c_nchar     = f"中文_{i}"
            # 2-byte varbinary: bytes equivalent of the SQL literal \x{i:04X}
            c_varbinary = bytes.fromhex(f"{i:04X}")
            batch_params.append([
                ts, c_bool, c_tinyint, c_smallint, c_int, c_bigint,
                c_utinyint, c_usmallint, c_uint, c_ubigint,
                c_float, c_double, c_varchar, c_nchar, c_varbinary,
            ])

        tdStmt2.execute_batch(sql, batch_params, check_affected=True, expected_rows=self.ROW_COUNT)
        tdLog.info(f"inserted {self.ROW_COUNT} rows via STMT2")

    def verify_data(self):
        """Compare source rows[0..N-2] with stream output rows[0..N-2] column by column.

        The last source row (index N-1) sits in an unclosed window and is intentionally
        absent from the stream output, so we only compare the first N-1 rows.
        """
        expected_rows = self.ROW_COUNT - 1  # last window is unclosed, never triggers
        tdLog.info(
            f"waiting for {expected_rows} rows in stream_out_last "
            f"(source has {self.ROW_COUNT}, last window unclosed)"
        )
        self._wait_row_count("db_dtype.stream_out_last", expected_rows, timeout=120)

        # Fetch first expected_rows source rows ordered by ts
        source = self._fetch_all(
            f"select ts,c_bool,c_tinyint,c_smallint,c_int,c_bigint,"
            f"c_utinyint,c_usmallint,c_uint,c_ubigint,"
            f"c_float,c_double,c_varchar,c_nchar,c_varbinary "
            f"from db_dtype.t1 order by ts limit {expected_rows};",
            ncols=15,
        )

        # Fetch all stream output rows ordered by ts
        stream = self._fetch_all(
            f"select ts,r_bool,r_tinyint,r_smallint,r_int,r_bigint,"
            f"r_utinyint,r_usmallint,r_uint,r_ubigint,"
            f"r_float,r_double,r_varchar,r_nchar,r_varbinary "
            f"from db_dtype.stream_out_last order by ts;",
            ncols=15,
        )

        if len(source) != expected_rows:
            tdLog.exit(f"source row count mismatch: got {len(source)}, expected {expected_rows}")
        if len(stream) != expected_rows:
            tdLog.exit(f"stream row count mismatch: got {len(stream)}, expected {expected_rows}")

        col_names = [
            "ts",         "c_bool",     "c_tinyint",   "c_smallint",  "c_int",
            "c_bigint",   "c_utinyint", "c_usmallint",  "c_uint",      "c_ubigint",
            "c_float",    "c_double",   "c_varchar",    "c_nchar",     "c_varbinary",
        ]
        float_col_indices = {10, 11}  # c_float, c_double

        for row_idx in range(expected_rows):
            src = source[row_idx]
            stm = stream[row_idx]
            for col_idx, col_name in enumerate(col_names):
                sv, dv = src[col_idx], stm[col_idx]
                if col_idx in float_col_indices:
                    ok = self._float_eq(sv, dv)
                else:
                    ok = (sv == dv)
                if not ok:
                    tdLog.exit(
                        f"row {row_idx} col '{col_name}' mismatch: "
                        f"source={sv!r}  stream={dv!r}"
                    )

        tdLog.info(
            f"verification passed: {expected_rows} rows x {len(col_names)} columns "
            f"all match between source table and stream output"
        )

    # ---- helpers ----

    def _fetch_all(self, sql, ncols):
        """Execute query and return all rows as a list of tuples"""
        tdSql.query(sql)
        return [tuple(tdSql.getData(i, j) for j in range(ncols))
                for i in range(tdSql.getRows())]

    def _float_eq(self, a, b, rel_tol=1e-5):
        """Approximate float equality with relative tolerance"""
        if a is None and b is None:
            return True
        if a is None or b is None:
            return False
        if a == b:
            return True
        denom = max(abs(a), abs(b), 1e-300)
        return abs(a - b) / denom < rel_tol

    def _wait_stream_ready(self, stream_name, timeout=30):
        """Wait for a stream to reach Running status"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            tdSql.query(
                f"select status from information_schema.ins_streams "
                f"where stream_name='{stream_name}';"
            )
            if tdSql.getRows() > 0 and tdSql.getData(0, 0) == "Running":
                return
            time.sleep(1)
        tdLog.exit(f"stream '{stream_name}' not running after {timeout}s")

    def _wait_row_count(self, table, min_count, timeout=120):
        """Wait until a table contains at least min_count rows"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                tdSql.query(f"select count(*) from {table};")
                cnt = tdSql.getData(0, 0)
                if tdSql.getRows() > 0 and cnt is not None and cnt >= min_count:
                    return
            except Exception:
                pass
            time.sleep(1)
        tdLog.exit(f"table '{table}' did not reach {min_count} rows within {timeout}s")
