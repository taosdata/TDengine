"""FS §9 — End-to-end use cases for stream + federated query.

Real CREATE STREAM grammar (from removed reference tests + FS §9):
    CREATE STREAM <name> INTERVAL(...) SLIDING(...)
      FROM   <ext_src>.<remote_db_or_schema>.<table>   -- ext-driven
        OR   <local_db>.<local_table>                  -- local-driven
      [PARTITION BY tbname|tag]
      INTO   <local_db>.<sink>
      AS SELECT _twstart AS ts, ... FROM %%trows [JOIN <ext>.<...>.<tbl>] ...;

Three-segment external reference uses the *remote* db / schema created by
``_with_std_sources`` (``{prefix}_{m,p,i}db``); PG middle segment is the
schema (``public``).

Cases:
  FS-UC-001  Periodic summary: ext-driven INTERVAL stream over external metric.
  FS-UC-002  Local fact joined with external dim table (PG-only, FS §9.2).
  FS-UC-003  Single stream referencing two distinct EXTERNAL SOURCEs.
  FS-UC-004  InfluxDB measurement -> TSDB subtable split (FS §9.3).
"""

import time
import sys

from new_test_framework.utils import tdLog, tdSql, tdStream

sys.path.insert(0, "cases/09-DataQuerying/19-FederatedQuery")
from federated_query_common import (  # noqa: E402
    ExtSrcEnv,
    FederatedQueryTestMixin,
)

sys.path.insert(0, "cases/18-StreamProcessing/federated")
from test_fs_common import (  # noqa: E402
    ensure_snode,
    wait_stream_window_closed,
    FS_BASE_MS,
    ms_to_dt,
    dt_str,
    verify_sink_rows,
    FsSharedFixtureMixin,
    get_stream_ext_meta,
)


def _mid_segment(prefix: str, src_name: str) -> str:
    """For src '{prefix}_{m|p|i}', return remote db (mysql/influx) or schema (pg)."""
    suffix = src_name[-1]
    if suffix == "p":
        return "public"
    return f"{prefix}_{suffix}db"


class TestFsUseCases(FsSharedFixtureMixin, FederatedQueryTestMixin):
    """FS §9 — Use cases (FS-UC-001..004)."""

    DB = "fs_uc"

    # ------------------------------------------------------------------
    # Shared test data for FS-UC-001
    # Fixed base timestamp: 2025-03-03 00:00:00 UTC (epoch ms).
    # Each row is in its own 1-minute window (INTERVAL 1m SLIDING 1m).
    # batch1: windows 08:00/08:01/08:02   batch2: windows 08:03/08:04
    # ------------------------------------------------------------------
    _BASE_MS = FS_BASE_MS

    # (ts_ms, val, score, name, flag)
    _BATCH1 = [
        (_BASE_MS,          6, 6.5, 'zeta',  0),
        (_BASE_MS + 60_000, 7, 7.5, 'eta',   1),
        (_BASE_MS + 120_000, 8, 8.5, 'theta', 0),
    ]
    _BATCH1_MORE = [
        (_BASE_MS,          6, 6.5, 'zeta',  0),
        (_BASE_MS + 60_000, 7, 7.5, 'eta',   1),
        (_BASE_MS + 80_000, 8, 7.6, 'hello',   1),
        (_BASE_MS + 120_000, 9, 8.5, 'theta', 0),
        (_BASE_MS + 140_000, 10, 8.6, 'too', 0),
    ]
    _BATCH2 = [
        (_BASE_MS + 180_000, 9,  9.5,  'iota',  0),
        (_BASE_MS + 240_000, 10, 10.5, 'kappa', 1),
    ]

    # ------------------------------------------------------------------
    # The uc001b InfluxDB PARTITION BY family (one measurement with 2 tags
    # = 4 live series, stale pre-batch + post-batch) uses the shared fixture
    # from FsSharedFixtureMixin (_UC_SERIES / _prep_partition_influx / ...).
    # ------------------------------------------------------------------

    @classmethod
    def setup_class(cls):
        cls.env = ExtSrcEnv()
        cls.env.ensure_env()
        ensure_snode()
        tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB}")
        tdSql.execute(f"CREATE DATABASE {cls.DB} PRECISION 'ms'")
        tdSql.execute(f"alter dnode 1 'debugflag 135'")
        tdSql.execute(f"alter dnode 1 'qdebugflag 143'")

    @classmethod
    def teardown_class(cls):
        try:
            tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB}")
        finally:
            cls.env.teardown_env()

    # ------------------------------------------------------------------
    # Shared helpers for FS-UC-001 (_ms_to_dt / _dt_str / _insert_rows come
    # from FsSharedFixtureMixin)
    # ------------------------------------------------------------------

    _ms_to_dt = staticmethod(ms_to_dt)
    _dt_str = staticmethod(dt_str)

    def _verify(self, stream: str, sink: str, expected: list, label: str):
        verify_sink_rows(self.DB, stream, sink, expected, label)

    def _create_uc001_stream(self, stream: str, sink: str, src_name: str, mid: str,
                             mode: str = "trows"):
        """Create the FS-UC-001 INTERVAL stream and wait until Running.

        _twstart precision differs by source type:
          MySQL / PG  : microseconds  -> divide by 1_000 to get ms timestamp
          InfluxDB    : nanoseconds   -> divide by 1_000_000 to get ms timestamp

        `mode` selects the calc SELECT's FROM clause:
          "trows"                : FROM %%trows (the default, trigger rows themselves)
          "no_trows"              : FROM the ext table directly, no window filter
          "no_trows_with_wstart"  : FROM the ext table, WHERE ts >= _twstart AND ts < _twend
          "no_trows_with_range"   : FROM the ext table, WHERE ts within a fixed
                                     [_BASE_MS, _BASE_MS + 120_000] literal range
        """
        # src_name ends with '_m' (MySQL), '_p' (PG), or '_i' (InfluxDB)
        twstart_divisor = 1_000_000 if src_name.endswith("_i") else 1_000
        ts_col = "time" if src_name.endswith("_i") else "ts"
        src_table = f"{src_name}.{mid}.src_t"
        from_clauses = {
            "trows": "%%trows",
            "no_trows": src_table,
            "no_trows_with_wstart": (
                f"{src_table} where {ts_col} >=_twstart and {ts_col} < _twend"
            ),
            "no_trows_with_range": (
                f"{src_table} where {ts_col} >= {self._BASE_MS * twstart_divisor} "
                f"and {ts_col} <= {(self._BASE_MS + 120_000) * twstart_divisor}"
            ),
        }

        tdSql.execute(f"USE {self.DB}")
        tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
        tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
        tdSql.execute(
            f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
            f"FROM {src_table} "
            f"INTO {self.DB}.{sink} AS "
            f"SELECT cast(_twstart/{twstart_divisor} as timestamp) AS ts, "
            f"COUNT(*) AS cnt, AVG(val) AS avg_val "
            f"FROM {from_clauses[mode]}"
        )
        tdLog.info(f"[{src_name}] stream created; waiting for Running status...")
        tdStream.checkStreamStatus(stream)

    # ------------------------------------------------------------------
    # Shared helpers for the uc001b InfluxDB PARTITION BY test family
    # (_build_influx_lines / _prep_partition_influx / _write_partition_post_batch
    # / _teardown_partition_influx come from FsSharedFixtureMixin)
    # ------------------------------------------------------------------

    def _assert_distinct_tbnames(self, sink_stb: str, expected_tbnames: list):
        tb_res = tdSql.getResult(
            f"SELECT DISTINCT tbname FROM {self.DB}.{sink_stb} order by tbname"
        )
        actual_tbnames = sorted(row[0] for row in tb_res)
        tdSql.checkEqual(actual_tbnames, expected_tbnames)

    def _assert_windows_per_subtable(self, columns: str, expected_by_subtable: dict,
                                     window_starts_ms: list):
        """Verify `columns` (a SELECT column-list string, ts excluded) row-by-row
        for every auto-created OUTPUT_SUBTABLE sink in `expected_by_subtable`,
        across all `window_starts_ms` -- each subtable's `tail` tuple is expected
        to repeat identically for every closed window."""
        for sub_tbname, tail in expected_by_subtable.items():
            res = tdSql.getResult(
                f"SELECT ts, {columns} FROM {self.DB}.`{sub_tbname}` ORDER BY ts"
            )
            expected = [(self._ms_to_dt(ts_ms), *tail) for ts_ms in window_starts_ms]
            tdSql.checkEqual(res, expected)

    def test_uc_001b_influx_stable_no_partition_by(self):
        """InfluxDB multi-subtable coverage for the OR-batched fetch path.

        One measurement (src_t) carries 3 device tags -> 3 subtables/series.
        PARTITION BY tbname makes the influx reader discover the tag column,
        register one uid per device and fetch them through the OR-compound query
        built by buildInfluxBatchSql / consumed by fetchDataBatchInflux
        (WHERE (device='d1' AND ts>..) OR (device='d2' AND ts>..) OR ...).

        Each device carries a distinct val (1/2/3) so the per-subtable AVG(val)
        identifies which series was fetched: seeing all of 1.0/2.0/3.0 proves the
        OR batch pulled every subtable (a broken OR would drop some devices)."""
        src = "uc001sp_n"
        i_db = "uc001sp_ndb"
        stream = "s_uc00nsp"
        sink_stb = "sink_n_uc001sp_stb"

        self._prep_partition_influx(src, i_db)
        try:
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink_stb}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{i_db}.src_t "
                f"INTO {self.DB}.{sink_stb} AS "
                f"SELECT cast(_twstart/1000000 as timestamp) AS ts, "
                f"COUNT(*) AS cnt, AVG(val) AS avg_val "
                f"FROM %%trows"
            )
            tdStream.checkStreamStatus(stream)

            # Write the verification batch only after the stream is Running.
            self._write_partition_post_batch(i_db)

            # Only the post-stream batch should be visible: no PARTITION BY, so
            # all 4 series collapse into a single group -> 4 closed windows
            # (last window stays open), each aggregating all 4 series ->
            # cnt=4 and avg_val = (1+3+2+4)/4 = 2.5.
            wait_stream_window_closed(stream, self.DB, sink_stb,
                                      expected_rows=4, timeout=120)

            window_starts_ms = [self._BASE_MS + k * 60_000 for k in range(4)]
            res = tdSql.getResult(
                f"SELECT ts, cnt, avg_val FROM {self.DB}.{sink_stb} ORDER BY ts"
            )
            expected = [
                (self._ms_to_dt(ts_ms), 4, 2.5) for ts_ms in window_starts_ms
            ]
            tdSql.checkEqual(res, expected)

            tdLog.info("[uc001sp] verified no-PARTITION-BY single group: "
                       "4 subtables collapsed into 1 group")
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
        finally:
            self._teardown_partition_influx(src, i_db)

    def test_uc_001b_influx_subset_partition_by_tbname(self):
        """InfluxDB PARTITION BY tbname (pure, no other tags) -> one group per uid.

        The measurement src_t carries TWO tags, host and region, with 4 live
        series (host in {a,b} x region in {x,y}) -> 4 sub-tables/uids in the
        reader. PARTITION BY tbname means groupId = uid for every series, so 4
        distinct groups/subtables are expected (no collapsing), each keeping its
        own single-series aggregate value.

        The test also prewrites an older batch before CREATE STREAM and only
        verifies the batch written after stream creation. This proves that the
        stream does not backfill pre-existing Influx rows.
        """
        src = "uc001sp_tbname"
        i_db = "uc001sp_tdb"
        stream = "s_uc00tsp"
        sink_stb = "sink_t_uc001sp_stb"

        self._prep_partition_influx(src, i_db)
        try:
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{i_db}.src_t PARTITION BY tbname "
                f"INTO {self.DB}.{sink_stb} "
                f"OUTPUT_SUBTABLE(CONCAT('h_', tbname)) AS "
                f"SELECT cast(_twstart/1000000 as timestamp) AS ts, "
                f"COUNT(*) AS cnt, AVG(val) AS avg_val, %%tbname "
                f"FROM %%trows"
            )
            tdStream.checkStreamStatus(stream)

            # Write the verification batch only after the stream is Running.
            self._write_partition_post_batch(i_db)

            # 4 groups (one per series) x 4 closed windows = 16 sink rows (last
            # window stays open).
            wait_stream_window_closed(stream, self.DB, sink_stb,
                                      expected_rows=16, timeout=120)

            # First confirm exactly the 4 expected subtables were auto-created
            # with the OUTPUT_SUBTABLE-computed name (proves the resolved bare
            # tbname reference is both present and correctly formed, not just
            # that some 4 groups exist).
            self._assert_distinct_tbnames(
                sink_stb, ['h_src_t_a_x_tname', 'h_src_t_a_y_tname', 'h_src_t_b_x_tname', 'h_src_t_b_y_tname']
            )

            # Then verify every row of each subtable individually: 4 closed
            # windows (08:00..08:03), in order, with the exact per-series
            # aggregate value and %%tbname -- not just an unordered bag.
            window_starts_ms = [self._BASE_MS + k * 60_000 for k in range(4)]
            expected_by_subtable = {
                "h_src_t_a_x_tname": (1, 1.0, "src_t_a_x_tname"),
                "h_src_t_a_y_tname": (1, 3.0, "src_t_a_y_tname"),
                "h_src_t_b_x_tname": (1, 2.0, "src_t_b_x_tname"),
                "h_src_t_b_y_tname": (1, 4.0, "src_t_b_y_tname"),
            }
            self._assert_windows_per_subtable(
                "cnt, avg_val, `%%tbname`", expected_by_subtable, window_starts_ms
            )

            tdLog.info("[uc001sp] verified pure PARTITION BY tbname: "
                       "4 uids kept as 4 distinct groups, "
                       "row-by-row match on all 4 subtables")
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
        finally:
            self._teardown_partition_influx(src, i_db)
    
    def test_uc_001b_influx_subset_partition_by_tbname2(self):
        """InfluxDB PARTITION BY tbname (pure, no other tags) -> one group per uid.

        The measurement src_t carries TWO tags, host and region, with 4 live
        series (host in {a,b} x region in {x,y}) -> 4 sub-tables/uids in the
        reader. PARTITION BY tbname means groupId = uid for every series, so 4
        distinct groups/subtables are expected (no collapsing), each keeping its
        own single-series aggregate value.

        The test also prewrites an older batch before CREATE STREAM and only
        verifies the batch written after stream creation. This proves that the
        stream does not backfill pre-existing Influx rows.
        """
        src = "uc001sp_tbname"
        i_db = "uc001sp_tdb"
        stream = "s_uc00tsp"
        sink_stb = "sink_t_uc001sp_stb"

        self._prep_partition_influx(src, i_db)
        try:
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{i_db}.src_t PARTITION BY tbname "
                f"INTO {self.DB}.{sink_stb} "
                f"OUTPUT_SUBTABLE(CONCAT('h_', tbname)) AS "
                f"SELECT cast(_twstart/1000000 as timestamp) AS ts, "
                f"COUNT(*) AS cnt, AVG(val) AS avg_val, %%1 "
                f"FROM %%trows"
            )
            tdStream.checkStreamStatus(stream)

            # Write the verification batch only after the stream is Running.
            self._write_partition_post_batch(i_db)

            # 4 groups (one per series) x 4 closed windows = 16 sink rows (last
            # window stays open).
            wait_stream_window_closed(stream, self.DB, sink_stb,
                                      expected_rows=16, timeout=120)

            # First confirm exactly the 4 expected subtables were auto-created
            # with the OUTPUT_SUBTABLE-computed name (proves the resolved bare
            # tbname reference is both present and correctly formed, not just
            # that some 4 groups exist).
            self._assert_distinct_tbnames(
                sink_stb, ['h_src_t_a_x_tname', 'h_src_t_a_y_tname', 'h_src_t_b_x_tname', 'h_src_t_b_y_tname']
            )

            # Then verify every row of each subtable individually: 4 closed
            # windows (08:00..08:03), in order, with the exact per-series
            # aggregate value and %%tbname -- not just an unordered bag.
            window_starts_ms = [self._BASE_MS + k * 60_000 for k in range(4)]
            expected_by_subtable = {
                "h_src_t_a_x_tname": (1, 1.0, "src_t_a_x_tname"),
                "h_src_t_a_y_tname": (1, 3.0, "src_t_a_y_tname"),
                "h_src_t_b_x_tname": (1, 2.0, "src_t_b_x_tname"),
                "h_src_t_b_y_tname": (1, 4.0, "src_t_b_y_tname"),
            }
            self._assert_windows_per_subtable(
                "cnt, avg_val, `%%1`", expected_by_subtable, window_starts_ms
            )
            tdLog.info("[uc001sp] verified pure PARTITION BY tbname: "
                       "4 uids kept as 4 distinct groups, "
                       "row-by-row match on all 4 subtables")
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
        finally:
            self._teardown_partition_influx(src, i_db)

    def test_uc_001b_influx_subset_partition_by_tbname_tbname(self):
        """InfluxDB PARTITION BY tbname (pure, no other tags) -> one group per uid.

        The measurement src_t carries TWO tags, host and region, with 4 live
        series (host in {a,b} x region in {x,y}) -> 4 sub-tables/uids in the
        reader. PARTITION BY tbname means groupId = uid for every series, so 4
        distinct groups/subtables are expected (no collapsing), each keeping its
        own single-series aggregate value.

        The test also prewrites an older batch before CREATE STREAM and only
        verifies the batch written after stream creation. This proves that the
        stream does not backfill pre-existing Influx rows.
        """
        src = "uc001sp_tbname"
        i_db = "uc001sp_tdb"
        stream = "s_uc00tsp"
        sink_stb = "sink_t_uc001sp_stb"

        self._prep_partition_influx(src, i_db)
        try:
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{i_db}.src_t PARTITION BY tbname,`tbname` "
                f"INTO {self.DB}.{sink_stb} "
                f"OUTPUT_SUBTABLE(CONCAT('h_', tbname)) AS "
                f"SELECT cast(_twstart/1000000 as timestamp) AS ts, "
                f"COUNT(*) AS cnt, AVG(val) AS avg_val, %%1,%%tbname,cast(%%2 as varchar(64)) "
                f"FROM %%trows"
            )
            tdStream.checkStreamStatus(stream)

            # Write the verification batch only after the stream is Running.
            self._write_partition_post_batch(i_db)

            # 4 groups (one per series) x 4 closed windows = 16 sink rows (last
            # window stays open).
            wait_stream_window_closed(stream, self.DB, sink_stb,
                                      expected_rows=16, timeout=120)

            # First confirm exactly the 4 expected subtables were auto-created
            # with the OUTPUT_SUBTABLE-computed name (proves the resolved bare
            # tbname reference is both present and correctly formed, not just
            # that some 4 groups exist).
            self._assert_distinct_tbnames(
                sink_stb, ['h_src_t_a_x_tname', 'h_src_t_a_y_tname', 'h_src_t_b_x_tname', 'h_src_t_b_y_tname']
            )

            # Then verify every row of each subtable individually: 4 closed
            # windows (08:00..08:03), in order, with the exact per-series
            # aggregate value and %%tbname -- not just an unordered bag.
            window_starts_ms = [self._BASE_MS + k * 60_000 for k in range(4)]
            expected_by_subtable = {
                "h_src_t_a_x_tname": (1, 1.0, "src_t_a_x_tname", "src_t_a_x_tname", "tname"),
                "h_src_t_a_y_tname": (1, 3.0, "src_t_a_y_tname", "src_t_a_y_tname", "tname"),
                "h_src_t_b_x_tname": (1, 2.0, "src_t_b_x_tname", "src_t_b_x_tname", "tname"),
                "h_src_t_b_y_tname": (1, 4.0, "src_t_b_y_tname", "src_t_b_y_tname", "tname"),
            }
            self._assert_windows_per_subtable(
                "cnt, avg_val, `%%1`, `%%tbname`, `cast(%%2 as varchar(64))`", expected_by_subtable, window_starts_ms
            )
            tdLog.info("[uc001sp] verified pure PARTITION BY tbname: "
                       "4 uids kept as 4 distinct groups, "
                       "row-by-row match on all 4 subtables")
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
        finally:
            self._teardown_partition_influx(src, i_db)

    def test_uc_001b_influx_partition_host_tbname_region_mixed(self):
        """InfluxDB PARTITION BY mixing tbname with explicit tag columns
        (PARTITION BY host, tbname, region) -> tbname forces finest-granularity
        grouping (same cardinality as PARTITION BY tbname alone: one group per
        unique host/region combination, NOT collapsed by host), while host and
        region remain individually referenceable both by name and by
        position (%%n), and tbname's own position resolves to the sub-table's
        synthesized identity rather than NULL.

        The measurement src_t carries the same 4 live series as
        test_uc_001b_influx_subset_partition_by_tbname (host in {a,b} x region
        in {x,y}). PARTITION BY host, tbname, region puts host at position 1,
        tbname at position 2, region at position 3.

        OUTPUT_SUBTABLE(CONCAT('hm_', tbname)) uses the bare tbname reference
        (allowed here because tbname is present in PARTITION BY, mixed or
        not -- see the isExtInfluxSource fix in
        parTranslater.c rewriteTagSubtableExpr). The calc SELECT list
        additionally projects %%1/%%2/%%3/%%tbname as t1/t2/t3/tname to
        directly verify positional resolution:
          - t1 (%%1, position 1) must equal this series' own host value.
          - t3 (%%3, position 3) must equal this series' own region value,
            not host's value again and not some other tag's.
          - t2 (%%2, position 2 -- tbname's own slot, not a real tag column)
            must equal the sub-table's own synthesized tbname (same string as
            tname / %%tbname), not NULL and not host's/region's value:
            buildExtSpecs marks this slot with a "tbname" sentinel and
            handleGroupColValuePull fills it accordingly instead of treating
            it like any other unresolvable (e.g. scalar-function) partition
            key slot.
        This is the direct regression check for the parser (buildExtSpecs /
        rewriteTagSubtableExpr) and reader (streamReaderExt.c
        handleGroupColValuePull) ordering-alignment fix: before that fix,
        mixing tbname into PARTITION BY made the reader either drop
        host/region entirely (treating it as bare "PARTITION BY tbname") or,
        had a fix only touched one side, misalign %%1/%%2/%%3 with whatever
        tag happened to sort first internally instead of each position's own
        value.

        Expected: 4 groups (one per host/region combination, matching pure
        "PARTITION BY tbname" cardinality), each with its own recognizable
        "hm_<measurement>_<host>_<region>" subtable name and single-series
        (not averaged across regions) aggregate value.
        """
        src = "uc001mx_m"
        i_db = "uc001mx_mdb"
        stream = "s_uc001mx_m"
        sink_stb = "sink_m_uc001mx_stb"

        self._prep_partition_influx(src, i_db)
        try:
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{i_db}.src_t PARTITION BY host, tbname, region "
                f"INTO {self.DB}.{sink_stb} "
                f"OUTPUT_SUBTABLE(CONCAT('hm_', tbname)) AS "
                f"SELECT cast(_twstart/1000000 as timestamp) AS ts, "
                f"COUNT(*) AS cnt, AVG(val) AS avg_val, "
                # host/region are InfluxDB tag columns; their ext-catalog type
                # defaults to TSDB_MAX_BINARY_LEN (unbounded-length string,
                # parserExtSource.c buildTableMetaFromExtMeta) when parsed
                # directly as an output column, which alone blows past
                # TSDB_MAX_BYTES_PER_ROW. CAST to a bounded VARCHAR so %%1/%%2/
                # %%3 can be projected as real sink-table columns.
                f"CAST(%%1 AS VARCHAR(32)) as t1, CAST(%%2 AS VARCHAR(32)) as t2, "
                f"CAST(%%3 AS VARCHAR(32)) as t3, %%tbname as tname "
                f"FROM %%trows"
            )
            tdStream.checkStreamStatus(stream)

            # Write the verification batch only after the stream is Running.
            self._write_partition_post_batch(i_db)

            # 4 groups (one per host/region combination) x 4 closed windows =
            # 16 sink rows (last window stays open). If grouping were wrongly
            # collapsed by host alone (tbname's presence not honored), this
            # would instead be 2 groups x 4 = 8 rows.
            wait_stream_window_closed(stream, self.DB, sink_stb,
                                      expected_rows=16, timeout=120)

            # Confirm exactly the 4 expected subtables were auto-created with
            # the OUTPUT_SUBTABLE-computed name (CONCAT('hm_', tbname), where
            # tbname is the reader's synthesized "<measurement>_<tagVal>..."
            # identity -- e.g. "src_t_a_x" -- built from ALL of this uid's tag
            # values, not just the ones in PARTITION BY). This proves bare
            # tbname resolves in OUTPUT_SUBTABLE even though it is mixed with
            # explicit tag columns in PARTITION BY.
            self._assert_distinct_tbnames(sink_stb, [
                'hm_src_t_a_x_tname', 'hm_src_t_a_y_tname', 'hm_src_t_b_x_tname', 'hm_src_t_b_y_tname',
            ])

            # Then verify every row of each subtable individually: 4 closed
            # windows, in order, with the exact per-series aggregate value,
            # AND the %%1/%%2/%%3/%%tbname projected columns -- not just an
            # unordered bag. This is the direct regression check for the
            # ordering-alignment fix: t1 must be THIS series' own host value
            # (position 1), t3 must be THIS series' own region value
            # (position 3). t2 is position 2 -- tbname's OWN slot in
            # PARTITION BY host, tbname, region, not a real tag column --
            # so it must resolve to the sub-table's own synthesized tbname
            # value (same as %%tbname / tname), not to host's or region's
            # value and not to NULL: handleGroupColValuePull recognizes this
            # specific slot (buildExtSpecs marks it with the "tbname"
            # sentinel) and fills it with the same string used for %%tbname.
            window_starts_ms = [self._BASE_MS + k * 60_000 for k in range(4)]
            expected_by_subtable = {
                "hm_src_t_a_x_tname": (1, 1.0, "a", "src_t_a_x_tname", "x", "src_t_a_x_tname"),
                "hm_src_t_a_y_tname": (1, 3.0, "a", "src_t_a_y_tname", "y", "src_t_a_y_tname"),
                "hm_src_t_b_x_tname": (1, 2.0, "b", "src_t_b_x_tname", "x", "src_t_b_x_tname"),
                "hm_src_t_b_y_tname": (1, 4.0, "b", "src_t_b_y_tname", "y", "src_t_b_y_tname"),
            }
            self._assert_windows_per_subtable(
                "cnt, avg_val, t1, t2, t3, tname", expected_by_subtable, window_starts_ms
            )

            tdLog.info("[uc001mx] verified PARTITION BY host, tbname, region: "
                       "4 groups (not collapsed by host), bare tbname in "
                       "OUTPUT_SUBTABLE resolved, %%1/%%3 (host/region) each "
                       "resolved to their own tag value, %%2 (tbname's own "
                       "slot) resolved to the sub-table's own tbname")
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
        finally:
            self._teardown_partition_influx(src, i_db)

    def test_uc_001b_influx_subset_partition(self):
        """InfluxDB PARTITION BY a tag *subset* -> many uids share one groupId.

        The measurement src_t carries TWO tags, host and region, with 4 live
        series (host in {a,b} x region in {x,y}) -> 4 sub-tables/uids in the
        reader (uid = hash(full tagset host|region)).

        The stream partitions by host ONLY.  streamReaderExt.c must therefore
        derive groupId = hash(host) alone, so the two region-series of a host
        (e.g. a/x and a/y) collapse into a single group even though they are
        distinct uids.  This is the discriminating check for the uid vs groupId
        split: correct behavior yields 2 groups; a regression that keyed groupId
        off the full tagset (uid == groupId) would yield 4.

        Values are chosen so AVG(val) per host proves the collapse:
          host a: region x val=1, region y val=3  -> AVG = 2.0
          host b: region x val=2, region y val=4  -> AVG = 3.0
        A per-full-tag (broken) grouping would instead expose the raw
        1/2/3/4 values across 4 groups.

        The test also prewrites an older batch before CREATE STREAM and only
        verifies the batch written after stream creation. This proves that the
        stream does not backfill pre-existing Influx rows.
        """
        src = "uc001sp_s"
        i_db = "uc001sp_sdb"
        stream = "s_uc001sp_subset"
        sink_stb = "sink_s_uc001sp_stb"

        self._prep_partition_influx(src, i_db)
        try:
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{i_db}.src_t PARTITION BY host,`tbname` "
                f"INTO {self.DB}.{sink_stb} "
                f"OUTPUT_SUBTABLE(CONCAT('h_', cast(host as varchar(32)), '_', cast(`tbname` as varchar(32)))) AS "
                f"SELECT cast(_twstart/1000000 as timestamp) AS ts, "
                f"COUNT(*) AS cnt, AVG(val) AS avg_val, cast(%%2 as varchar(32)) "
                f"FROM %%trows"
            )
            tdStream.checkStreamStatus(stream)

            # Write the verification batch only after the stream is Running.
            self._write_partition_post_batch(i_db)
            # Only the post-stream batch should be visible: 2 host groups x 4
            # closed windows = 8 sink rows (last window stays open). Each host
            # window aggregates BOTH region series -> cnt=2 and avg_val = 2.0
            # (host a) / 3.0 (host b).
            wait_stream_window_closed(stream, self.DB, sink_stb,
                                      expected_rows=8, timeout=120)

            # First confirm exactly the two expected subtables were auto-created
            # with the OUTPUT_SUBTABLE-computed name (proves the resolved "host"
            # group-col value is both present and case-correct, not just that some
            # 2 groups exist).
            self._assert_distinct_tbnames(sink_stb, ["h_a_tname", "h_b_tname"])

            # Then verify every row of each subtable individually: 4 closed
            # windows (08:00..08:03), in order, with the exact per-host
            # aggregate values -- not just an unordered (cnt, avg_val) bag.
            window_starts_ms = [self._BASE_MS + k * 60_000 for k in range(4)]
            expected_by_subtable = {
                "h_a_tname": (2, 2.0, "tname"),  # region x val=1, region y val=3 -> AVG = 2.0
                "h_b_tname": (2, 3.0, "tname"),  # region x val=2, region y val=4 -> AVG = 3.0
            }
            self._assert_windows_per_subtable(
                "cnt, avg_val, `cast(%%2 as varchar(32))`", expected_by_subtable, window_starts_ms
            )

            tdLog.info("[uc001sp] verified subset PARTITION BY host: "
                       "4 uids collapsed into 2 groups, "
                       "row-by-row match on h_a/h_b subtables")
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
        finally:
            self._teardown_partition_influx(src, i_db)
            
    def test_uc_001b_influx_subset_partition_scalar(self):
        """InfluxDB PARTITION BY a tag *subset* -> many uids share one groupId.

        The measurement src_t carries TWO tags, host and region, with 4 live
        series (host in {a,b} x region in {x,y}) -> 4 sub-tables/uids in the
        reader (uid = hash(full tagset host|region)).

        The stream partitions by host ONLY.  streamReaderExt.c must therefore
        derive groupId = hash(host) alone, so the two region-series of a host
        (e.g. a/x and a/y) collapse into a single group even though they are
        distinct uids.  This is the discriminating check for the uid vs groupId
        split: correct behavior yields 2 groups; a regression that keyed groupId
        off the full tagset (uid == groupId) would yield 4.

        Values are chosen so AVG(val) per host proves the collapse:
          host a: region x val=1, region y val=3  -> AVG = 2.0
          host b: region x val=2, region y val=4  -> AVG = 3.0
        A per-full-tag (broken) grouping would instead expose the raw
        1/2/3/4 values across 4 groups.

        The test also prewrites an older batch before CREATE STREAM and only
        verifies the batch written after stream creation. This proves that the
        stream does not backfill pre-existing Influx rows.
        """
        src = "uc001sp_s"
        i_db = "uc001sp_sdb"
        stream = "s_uc001sp_subset"
        sink_stb = "sink_s_uc001sp_stb"

        self._prep_partition_influx(src, i_db)
        try:
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{i_db}.src_t PARTITION BY upper(host),`tbname` "
                f"INTO {self.DB}.{sink_stb} "
                f"OUTPUT_SUBTABLE(CONCAT('h_', cast(upper(host) as varchar(32)), '_', cast(`tbname` as varchar(32)))) AS "
                f"SELECT cast(_twstart/1000000 as timestamp) AS ts, "
                f"COUNT(*) AS cnt, AVG(val) AS avg_val, cast(%%1 as varchar(32)), cast(%%2 as varchar(32)) "
                f"FROM %%trows"
            )
            tdStream.checkStreamStatus(stream)

            # Write the verification batch only after the stream is Running.
            self._write_partition_post_batch(i_db)
            # Only the post-stream batch should be visible: 2 host groups x 4
            # closed windows = 8 sink rows (last window stays open). Each host
            # window aggregates BOTH region series -> cnt=2 and avg_val = 2.0
            # (host a) / 3.0 (host b).
            wait_stream_window_closed(stream, self.DB, sink_stb,
                                      expected_rows=8, timeout=120)

            # First confirm exactly the two expected subtables were auto-created
            # with the OUTPUT_SUBTABLE-computed name (proves the resolved "host"
            # group-col value is both present and case-correct, not just that some
            # 2 groups exist).
            self._assert_distinct_tbnames(sink_stb, ["h_A_tname", "h_B_tname"])

            # Then verify every row of each subtable individually: 4 closed
            # windows (08:00..08:03), in order, with the exact per-host
            # aggregate values -- not just an unordered (cnt, avg_val) bag.
            window_starts_ms = [self._BASE_MS + k * 60_000 for k in range(4)]
            expected_by_subtable = {
                "h_A_tname": (2, 2.0, "A", "tname"),  # region x val=1, region y val=3 -> AVG = 2.0
                "h_B_tname": (2, 3.0, "B", "tname"),  # region x val=2, region y val=4 -> AVG = 3.0
            }
            self._assert_windows_per_subtable(
                "cnt, avg_val, `cast(%%1 as varchar(32))`, `cast(%%2 as varchar(32))`", expected_by_subtable, window_starts_ms
            )

            tdLog.info("[uc001sp] verified subset PARTITION BY host: "
                       "4 uids collapsed into 2 groups, "
                       "row-by-row match on h_a/h_b subtables")
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
        finally:
            self._teardown_partition_influx(src, i_db)

    def do_uc_001b_influx_multi_tag_scalar_partition(self):
        src = "uc001sp_mc"
        i_db = "uc001sp_mcdb"
        stream = "s_uc001sp_multi"
        sink_stb = "sink_mc_uc001sp_stb"

        self._prep_partition_influx(src, i_db)
        try:
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{i_db}.src_t "
                f"PARTITION BY CONCAT(host, region), LENGTH(CONCAT(host, region)) "
                f"INTO {self.DB}.{sink_stb} "
                f"OUTPUT_SUBTABLE(CONCAT('mc_', "
                f"CAST(CONCAT(host, region) AS VARCHAR(32)))) AS "
                f"SELECT cast(_twstart/1000000 as timestamp) AS ts, "
                f"COUNT(*) AS cnt, AVG(val) AS avg_val, "
                f"CAST(%%1 AS VARCHAR(32)) AS partition_key, "
                f"%%2 AS partition_length "
                f"FROM %%trows"
            )
            tdStream.checkStreamStatus(stream)

            self._write_partition_post_batch(i_db)

            # Four host-region tuples must remain four independent groups.
            wait_stream_window_closed(stream, self.DB, sink_stb,
                                      expected_rows=16, timeout=120)
            self._assert_distinct_tbnames(
                sink_stb, ["mc_ax", "mc_ay", "mc_bx", "mc_by"]
            )

            window_starts_ms = [self._BASE_MS + k * 60_000 for k in range(4)]
            expected_by_subtable = {
                "mc_ax": (1, 1.0, "ax", 8),
                "mc_ay": (1, 3.0, "ay", 8),
                "mc_bx": (1, 2.0, "bx", 8),
                "mc_by": (1, 4.0, "by", 8),
            }
            self._assert_windows_per_subtable(
                "cnt, avg_val, partition_key, partition_length",
                expected_by_subtable, window_starts_ms
            )

            tdLog.info("[uc001sp] verified multi-tag scalar PARTITION BY: "
                       "typed expressions produced four independent groups")
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
        finally:
            self._teardown_partition_influx(src, i_db)

    def test_uc_001b_influx_multi_tag_scalar_partition(self):
        """Verify multi-tag scalar PARTITION BY preserves the full tuple.

        1. Partition four InfluxDB series by a multi-tag CONCAT expression
        2. Verify string and integer partition results preserve their types
        3. Verify four groups, four sink subtables, and positional values

        Catalog:
            - Stream

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-16 Wang Mingming Add multi-tag scalar partition regression coverage

        """
        self.do_uc_001b_influx_multi_tag_scalar_partition()

    def do_uc_001b_influx_same_tag_multi_expr_partition(self):
        src = "uc001sp_dup"
        i_db = "uc001sp_dupdb"
        stream = "s_uc001sp_dup"
        sink_stb = "sink_dup_uc001sp_stb"

        self._prep_partition_influx(src, i_db)
        try:
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{i_db}.src_t "
                f"PARTITION BY UPPER(host), CONCAT(host, '_raw') "
                f"INTO {self.DB}.{sink_stb} "
                f"OUTPUT_SUBTABLE(CONCAT('dup_', "
                f"CAST(UPPER(host) AS VARCHAR(32)))) AS "
                f"SELECT cast(_twstart/1000000 as timestamp) AS ts, "
                f"COUNT(*) AS cnt, AVG(val) AS avg_val, "
                f"CAST(%%1 AS VARCHAR(32)) AS upper_host, "
                f"CAST(%%2 AS VARCHAR(32)) AS raw_host "
                f"FROM %%trows"
            )
            tdStream.checkStreamStatus(stream)

            self._write_partition_post_batch(i_db)

            # Both expressions must read the same immutable raw host value.
            wait_stream_window_closed(stream, self.DB, sink_stb,
                                      expected_rows=8, timeout=120)
            self._assert_distinct_tbnames(sink_stb, ["dup_A", "dup_B"])

            window_starts_ms = [self._BASE_MS + k * 60_000 for k in range(4)]
            expected_by_subtable = {
                "dup_A": (2, 2.0, "A", "a_raw"),
                "dup_B": (2, 3.0, "B", "b_raw"),
            }
            self._assert_windows_per_subtable(
                "cnt, avg_val, upper_host, raw_host",
                expected_by_subtable, window_starts_ms
            )

            tdLog.info("[uc001sp] verified same-tag partition expressions: "
                       "each slot evaluated from the immutable raw host value")
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
            print("same-tag multi-expression partition ........ [ passed ]")
        finally:
            self._teardown_partition_influx(src, i_db)

    def test_uc_001b_influx_same_tag_multi_expr_partition(self):
        """Verify repeated tag references remain independent across expressions.

        1. Partition by UPPER(host) and CONCAT(host, '_raw')
        2. Verify the second expression reads the original lowercase host
        3. Verify both positional slots are non-NULL and correctly ordered

        Catalog:
            - Stream

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-16 Wang Mingming Add repeated-tag expression regression coverage

        """
        self.do_uc_001b_influx_same_tag_multi_expr_partition()
    def _wait_ext_error(self, stream: str, timeout: int = 30):
        """Poll stream metadata until its message reports a runtime error."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            meta = get_stream_ext_meta(stream)
            message = meta.get("message") or ""
            if "Influxdb tag key is too long or convert error" in message:
                tdLog.debug(f"Stream {stream} reported runtime error: {message}")
                return meta
            time.sleep(1)
        raise AssertionError(
            tdLog.exit(f"Stream {stream} never reported a runtime error within {timeout}s")
        )
    
    def do_uc_001b_influx_long_tag_value_error(self):
        src = "uc001sp_long_tag"
        i_db = "uc001sp_long_tag_db"
        stream = "s_uc001sp_long_tag"
        sink_stb = "sink_long_tag_uc001sp_stb"
        measurement = "long_tag_t"
        long_tag_key = "k" * 64
        long_tag_value = "v" * 257

        self._cleanup_src(src)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            # Materialize the measurement schema before CREATE STREAM.
            ExtSrcEnv.influx_write_cfg(
                self._influx_cfg(), i_db,
                [
                    f"{measurement},{long_tag_key}={long_tag_value} val=10i "
                    f"{(self._BASE_MS - 60_000) * 1_000_000}"
                ],
            )
            self._mk_influx_real(src, database=i_db)

            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{i_db}.{measurement} "
                f"PARTITION BY `{long_tag_key}` "
                f"INTO {self.DB}.{sink_stb} AS "
                f"SELECT cast(_twstart/1000000 as timestamp) AS ts, "
                f"COUNT(*) AS cnt, CAST(%%1 AS VARCHAR(256)) AS tag_value "
                f"FROM %%trows"
            )
            meta = self._wait_ext_error(stream, timeout=60)
            tdLog.info(f"do_uc_001b_influx_long_tag_key meta = {meta}")
            
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass

    def do_uc_001b_influx_long_tag_key_error(self):
        src = "uc001sp_long_tag"
        i_db = "uc001sp_long_tag_db"
        stream = "s_uc001sp_long_tag"
        sink_stb = "sink_long_tag_uc001sp_stb"
        measurement = "long_tag_t"
        long_tag_key = "k" * 66
        long_tag_value = "v" * 257

        self._cleanup_src(src)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        try:
            # Materialize the measurement schema before CREATE STREAM.
            ExtSrcEnv.influx_write_cfg(
                self._influx_cfg(), i_db,
                [
                    f"{measurement},{long_tag_key}={long_tag_value} val=10i "
                    f"{(self._BASE_MS - 60_000) * 1_000_000}"
                ],
            )
            self._mk_influx_real(src, database=i_db)

            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{i_db}.{measurement} "
                f"PARTITION BY tbname "
                f"INTO {self.DB}.{sink_stb} AS "
                f"SELECT cast(_twstart/1000000 as timestamp) AS ts, "
                f"COUNT(*) AS cnt, CAST(%%1 AS VARCHAR(256)) AS tag_value "
                f"FROM %%trows"
            )
            meta = self._wait_ext_error(stream, timeout=60)
            tdLog.info(f"do_uc_001b_influx_long_tag_key meta = {meta}")
            
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP STABLE IF EXISTS {self.DB}.{sink_stb}")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
            except Exception:
                pass

    
    def test_uc_001b_influx_long_tag_truncation(self):
        """Verify overlong InfluxDB tag keys and values are truncated.

        1. Create an InfluxDB series with a 65-character tag key
        2. Write a 257-character value for that tag
        3. Partition by the truncated 64-character key
        4. Verify the stream exposes exactly the first 256 value characters

        Catalog:
            - Stream

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-22 Wang Mingming Add InfluxDB tag truncation regression coverage

        """
        self.do_uc_001b_influx_long_tag_value_error()
        self.do_uc_001b_influx_long_tag_key_error()

    # ------------------------------------------------------------------
    # FS-UC-001 — Periodic summary, ext-driven (single-batch verify)
    #
    # Insert all 5 rows in two batches without an explicit delay.
    # The stream must produce 4 closed windows (08:00..08:03);
    # window 08:04 stays open because no later row arrives.
    # ------------------------------------------------------------------
    def test_uc_001_periodic_summary(self):
        """Ext-driven INTERVAL stream: two batches inserted back-to-back.

        Verifies that the stream correctly closes windows 08:00..08:03
        even when batch1 and batch2 arrive before the trigger has a chance
        to process batch1 independently.
        """
        prefix = "uc001"

        EXPECTED = [
            (self._ms_to_dt(self._BASE_MS),             1, 6.0),
            (self._ms_to_dt(self._BASE_MS + 60_000),    1, 7.0),
            (self._ms_to_dt(self._BASE_MS + 120_000),   1, 8.0),
            (self._ms_to_dt(self._BASE_MS + 180_000),   1, 9.0),
        ]

        def body(src_name: str):
            mid = _mid_segment(prefix, src_name)
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            suffix = src_name[-1]
            db_or_bucket = f"{prefix}_{suffix}db"

            self._create_uc001_stream(stream, sink, src_name, mid)
            self._insert_rows(suffix, db_or_bucket, self._BATCH1, src_name)
            self._insert_rows(suffix, db_or_bucket, self._BATCH2, src_name)
            self._verify(stream, sink, EXPECTED, src_name)

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body, skip_mysql=False,
                               skip_pg=False, skip_influx=False)

    # ------------------------------------------------------------------
    # FS-UC-001 — Periodic summary, ext-driven (two-phase verify)
    #
    # Insert batch1 first, verify that only the two fully-closed windows
    # (08:00, 08:01) appear in the sink.  Then insert batch2 and verify
    # that the remaining two windows (08:02, 08:03) are also produced.
    # This specifically exercises the cross-batch window-close path where
    # batch2's arrival causes 08:02's window to close.
    # ------------------------------------------------------------------
    def test_uc_001_periodic_summary_twice(self):
        """Ext-driven INTERVAL stream: verify incremental window closing.

        Phase 1: batch1 (08:00..08:02) → windows 08:00 and 08:01 close.
        Phase 2: batch2 (08:03..08:04) → windows 08:02 and 08:03 close.
        """
        prefix = "uc001"

        # After batch1: windows 08:00 and 08:01 are closed by 08:02's arrival.
        EXPECTED1 = [
            (self._ms_to_dt(self._BASE_MS),          1, 6.0),
            (self._ms_to_dt(self._BASE_MS + 60_000), 1, 7.0),
        ]

        # After batch2: 08:02 closed by 08:03, 08:03 closed by 08:04.
        EXPECTED2 = [
            (self._ms_to_dt(self._BASE_MS),             1, 6.0),
            (self._ms_to_dt(self._BASE_MS + 60_000),    1, 7.0),
            (self._ms_to_dt(self._BASE_MS + 120_000),   1, 8.0),
            (self._ms_to_dt(self._BASE_MS + 180_000),   1, 9.0),
        ]

        def body(src_name: str):
            mid = _mid_segment(prefix, src_name)
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            suffix = src_name[-1]
            db_or_bucket = f"{prefix}_{suffix}db"

            self._create_uc001_stream(stream, sink, src_name, mid)

            # Phase 1: batch1 closes windows 08:00 and 08:01.
            self._insert_rows(suffix, db_or_bucket, self._BATCH1, src_name)
            self._verify(stream, sink, EXPECTED1, src_name)

            # Phase 2: batch2 closes windows 08:02 and 08:03.
            self._insert_rows(suffix, db_or_bucket, self._BATCH2, src_name)
            self._verify(stream, sink, EXPECTED2, src_name)

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body, skip_mysql=False,
                               skip_pg=False, skip_influx=False)

    # ------------------------------------------------------------------
    # FS-UC-001 — Periodic summary, ext-driven (single-batch verify)
    #
    # Insert all 5 rows in two batches without an explicit delay.
    # The stream must produce 4 closed windows (08:00..08:03);
    # window 08:04 stays open because no later row arrives.
    # ------------------------------------------------------------------
    def test_uc_001_periodic_summary_no_trows(self):
        """Ext-driven INTERVAL stream: two batches inserted back-to-back.

        Verifies that the stream correctly closes windows 08:00..08:03
        even when batch1 and batch2 arrive before the trigger has a chance
        to process batch1 independently.
        """
        prefix = "uc001"

        EXPECTED_NO_TROWS = [
            (self._ms_to_dt(self._BASE_MS),             8, 4.5),
            (self._ms_to_dt(self._BASE_MS + 60_000),    8, 4.5),
        ]

        def body(src_name: str):
            mid = _mid_segment(prefix, src_name)
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            suffix = src_name[-1]
            db_or_bucket = f"{prefix}_{suffix}db"

            self._create_uc001_stream(stream, sink, src_name, mid, mode="no_trows")
            self._insert_rows(suffix, db_or_bucket, self._BATCH1, src_name)
            self._verify(stream, sink, EXPECTED_NO_TROWS, src_name)

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body, skip_mysql=False,
                               skip_pg=False, skip_influx=False)

    # ------------------------------------------------------------------
    # FS-UC-001 — Periodic summary, ext-driven (single-batch verify)
    #
    # Insert all 5 rows in two batches without an explicit delay.
    # The stream must produce 4 closed windows (08:00..08:03);
    # window 08:04 stays open because no later row arrives.
    # ------------------------------------------------------------------
    def test_uc_001_periodic_summary_no_trows_with_wstart(self):
        """Ext-driven INTERVAL stream: two batches inserted back-to-back.

        Verifies that the stream correctly closes windows 08:00..08:03
        even when batch1 and batch2 arrive before the trigger has a chance
        to process batch1 independently.
        """
        prefix = "uc001"

        EXPECTED = [
            (self._ms_to_dt(self._BASE_MS),             1, 6),
            (self._ms_to_dt(self._BASE_MS + 60_000),    2, 7.5),
        ]

        def body(src_name: str):
            mid = _mid_segment(prefix, src_name)
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            suffix = src_name[-1]
            db_or_bucket = f"{prefix}_{suffix}db"

            self._create_uc001_stream(stream, sink, src_name, mid,
                                      mode="no_trows_with_wstart")
            self._insert_rows(suffix, db_or_bucket, self._BATCH1_MORE, src_name)
            self._verify(stream, sink, EXPECTED, src_name)

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body, skip_mysql=False,
                               skip_pg=False, skip_influx=False)

    # ------------------------------------------------------------------
    # FS-UC-001 — Periodic summary, ext-driven (single-batch verify)
    #
    # Insert all 5 rows in two batches without an explicit delay.
    # The stream must produce 4 closed windows (08:00..08:03);
    # window 08:04 stays open because no later row arrives.
    # ------------------------------------------------------------------
    def test_uc_001_periodic_summary_no_trows_with_range(self):
        """Ext-driven INTERVAL stream: two batches inserted back-to-back.

        Verifies that the stream correctly closes windows 08:00..08:03
        even when batch1 and batch2 arrive before the trigger has a chance
        to process batch1 independently.
        """
        prefix = "uc001"

        EXPECTED = [
            (self._ms_to_dt(self._BASE_MS),             4, 7.5),
            (self._ms_to_dt(self._BASE_MS + 60_000),    4, 7.5),
        ]

        def body(src_name: str):
            mid = _mid_segment(prefix, src_name)
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            suffix = src_name[-1]
            db_or_bucket = f"{prefix}_{suffix}db"

            self._create_uc001_stream(stream, sink, src_name, mid,
                                      mode="no_trows_with_range")
            self._insert_rows(suffix, db_or_bucket, self._BATCH1_MORE, src_name)
            self._verify(stream, sink, EXPECTED, src_name)

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        self._with_std_sources(prefix, body, skip_mysql=False,
                               skip_pg=False, skip_influx=False)

    # ------------------------------------------------------------------
    # FS-UC-001 — Explicit namespace without a source-level default
    # ------------------------------------------------------------------
    def test_uc_001_explicit_database_without_source_default(self):
        """Trigger and calc readers use the database named in the stream SQL."""
        src = "uc001_nodefault_m"
        remote_db = "uc001_nodefault_mdb"
        stream = "s_uc001_nodefault"
        sink = "sink_uc001_nodefault"
        remote_db_nodata = "uc001_nodefault_mdb_nodata"

        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), remote_db)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), remote_db_nodata)

        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), remote_db, [
                "DROP TABLE IF EXISTS `src_t`",
                "CREATE TABLE `src_t` ("
                "  ts DATETIME(3) PRIMARY KEY, val INT, score DOUBLE,"
                "  name VARCHAR(32), flag TINYINT(1))",
            ])
            self._mk_mysql_real(src, database=remote_db_nodata)

            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src}.{remote_db}.src_t "
                f"INTO {self.DB}.{sink} AS "
                f"SELECT cast(_twstart/1000 as timestamp) AS ts, COUNT(*) AS cnt "
                f"FROM {src}.{remote_db}.src_t"
            )
            tdStream.checkStreamStatus(stream)

            self._insert_rows('m', remote_db, self._BATCH2, src)
            wait_stream_window_closed(stream, self.DB, sink,
                                      expected_rows=1, timeout=120)
            tdSql.query(f"SELECT cnt FROM {self.DB}.{sink}")
            tdSql.checkData(0, 0, len(self._BATCH2))
            tdSql.query(
                f"SELECT external_sources FROM information_schema.ins_streams "
            )
            tdSql.checkData(0, 0, 1, show=True)

            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
        finally:
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), remote_db)
            except Exception:
                pass

    def test_uc_002_local_trigger_external_calc(self):
        src = "uc002_calc_m"
        remote_db = "uc002_calc_mdb"
        local_trigger = "trigger_uc002"
        stream = "s_uc002_local_ext"
        sink = "sink_uc002_local_ext"
        external_rows = [
            (self._BASE_MS, 10, 1.0, "first", 1),
            (self._BASE_MS + 60_000, 20, 2.0, "second", 0),
            (self._BASE_MS + 120_000, 30, 3.0, "third", 1),
        ]

        self._cleanup_src(src)
        ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), remote_db)
        try:
            ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), remote_db, [
                "DROP TABLE IF EXISTS `src_t`",
                "CREATE TABLE `src_t` ("
                "  ts DATETIME(3) PRIMARY KEY, val INT, score DOUBLE,"
                "  name VARCHAR(32), flag TINYINT(1))",
            ])
            self._insert_rows("m", remote_db, external_rows, src)
            self._mk_mysql_real(src, database=remote_db)

            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{local_trigger}")
            tdSql.execute(
                f"CREATE TABLE {self.DB}.{local_trigger} "
                f"(ts TIMESTAMP, marker INT)"
            )
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {self.DB}.{local_trigger} "
                f"INTO {self.DB}.{sink} AS "
                f"SELECT _twstart AS ts, COUNT(*) AS ext_cnt, "
                f"SUM(val) AS ext_sum, AVG(val) AS ext_avg "
                f"FROM {src}.{remote_db}.src_t"
            )
            tdStream.checkStreamStatus(stream)
            tdLog.info(
                f"[{stream}] running with local trigger {self.DB}.{local_trigger} "
                f"and external calculation table {src}.{remote_db}.src_t"
            )

            for offset, marker in enumerate((1001, 1002, 1003, 1004)):
                trigger_ms = self._BASE_MS + offset * 60_000
                tdSql.execute(
                    f"INSERT INTO {self.DB}.{local_trigger} VALUES "
                    f"('{self._dt_str(trigger_ms)}', {marker})"
                )
            tdLog.info(f"[{stream}] inserted four local trigger rows")

            expected = [
                (self._ms_to_dt(self._BASE_MS + offset * 60_000), 3, 60, 20.0)
                for offset in range(3)
            ]
            wait_stream_window_closed(
                stream, self.DB, sink, expected_rows=len(expected), timeout=120
            )
            actual = tdSql.getResult(
                f"SELECT ts, ext_cnt, ext_sum, ext_avg "
                f"FROM {self.DB}.{sink} ORDER BY ts"
            )
            tdSql.checkEqual(actual, expected)
            tdSql.query(
                f"SELECT external_sources FROM information_schema.ins_streams "
            )
            tdSql.checkData(0, 0, 1, show=True)
            tdLog.info(
                f"[{stream}] verified {len(expected)} external aggregate rows"
            )
        finally:
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{local_trigger}")
            self._cleanup_src(src)
            try:
                ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), remote_db)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # FS-UC-003 — Two ext sources in one stream
    # ------------------------------------------------------------------
    def test_uc_003_two_sources_one_stream(self):
        """Single stream whose calc query JOINs two distinct EXTERNAL SOURCEs.

        The trigger reads source A; the calc SELECT joins source A with source B
        (two real external tables, NOT %%trows — %%trows may not appear in a join
        condition, rejected by the parser).  This exercises per-calc-scan ext-spec
        binding: each of the two calc readers must bind to its own source by name
        (src_a -> extSpecs[src_a], src_b -> extSpecs[src_b])."""
        prefix_a, prefix_b = "uc003a", "uc003b"
        src_a, src_b = f"{prefix_a}_m", f"{prefix_b}_m"
        db_a, db_b = f"{prefix_a}_mdb", f"{prefix_b}_mdb"
        self._cleanup_src(src_a, src_b)
        try:
            # Two MySQL sources with identical schema; data is inserted after the
            # stream is Running (see below) so the trigger drives the windows.
            for prefix, db in ((prefix_a, db_a), (prefix_b, db_b)):
                ExtSrcEnv.mysql_create_db_cfg(self._mysql_cfg(), db)
                ExtSrcEnv.mysql_exec_cfg(self._mysql_cfg(), db, [
                    "DROP TABLE IF EXISTS `src_t`",
                    "CREATE TABLE `src_t` ("
                    "  ts DATETIME(3) PRIMARY KEY, val INT, score DOUBLE,"
                    "  name VARCHAR(32), flag TINYINT(1))",
                ])
                self._mk_mysql_real(f"{prefix}_m", database=db)

            stream = "s_uc003"
            sink = "sink_uc003"
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
            # calc JOINs two real external tables (source A + source B). Using
            # %%trows in the join condition is rejected by the parser, so the calc
            # reads the ext table of source A directly and joins source B.
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src_a}.{db_a}.src_t "
                f"INTO {self.DB}.{sink} AS "
                f"SELECT cast(_twstart/1000 as timestamp) AS ts, "
                f"SUM(a.val + b.val) AS s "
                f"FROM {src_a}.{db_a}.src_t a "
                f"JOIN {src_b}.{db_b}.src_t b ON a.ts = b.ts"
            )
            tdStream.checkStreamStatus(stream)

            # 5 rows per source aligned on ts so the calc JOIN matches row-by-row.
            # source A val = 1..5, source B val = 10,20,..,50.  With a direct ext
            # scan (no %%trows), each triggered window aggregates the whole joined
            # table, so every closed window's s = SUM(a.val+b.val) over all rows
            #   = (1+2+3+4+5) + (10+20+30+40+50) = 15 + 150 = 165.
            # The value 165 proves BOTH sources are joined (source A alone -> 15,
            # source B alone -> 150), i.e. each calc reader bound to the right src.
            rows_a = [(self._BASE_MS + k * 60_000, k + 1,        1.0, 'a', 0) for k in range(5)]
            rows_b = [(self._BASE_MS + k * 60_000, (k + 1) * 10, 1.0, 'b', 0) for k in range(5)]
            self._insert_rows('m', db_a, rows_a, src_a)
            self._insert_rows('m', db_b, rows_b, src_b)

            # 5 trigger rows -> the first 4 windows close (the last stays open with
            # no later trigger row), each holding the full joined-table sum 165.
            EXPECTED = [
                (self._ms_to_dt(self._BASE_MS),            165.0),
                (self._ms_to_dt(self._BASE_MS + 60_000),   165.0),
                (self._ms_to_dt(self._BASE_MS + 120_000),  165.0),
                (self._ms_to_dt(self._BASE_MS + 180_000),  165.0),
            ]
            wait_stream_window_closed(stream, self.DB, sink,
                                      expected_rows=len(EXPECTED), timeout=120)
            res = tdSql.getResult(f"SELECT ts, s FROM {self.DB}.{sink} ORDER BY ts")
            tdSql.checkEqual(res, EXPECTED)
            tdSql.query(
                f"SELECT external_sources FROM information_schema.ins_streams "
            )
            tdSql.checkData(0, 0, 2, show=True)
            tdLog.info(f"[uc003] verified {len(EXPECTED)} joined sink rows OK")

            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
        finally:
            self._cleanup_src(src_a, src_b)
            for db in (db_a, db_b):
                try:
                    ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), db)
                except Exception:
                    pass
