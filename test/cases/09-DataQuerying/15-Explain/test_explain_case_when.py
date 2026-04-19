from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdCom


class TestExplainCaseWhen:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_explain_case_when_scan_range(self):
        """explain verbose true with CASE WHEN subquery

        1. EXPLAIN VERBOSE TRUE on a query that uses CASE WHEN inside a subquery
           with the outer WHERE filtering on the computed CASE column should not crash.
        2. The scan time range reported by EXPLAIN VERBOSE TRUE must match the
           explicit ts filters in the inner WHERE clause, not the full history range.

        Catalog:
            - Query:Explain

        Since: v3.3.6.0

        Labels: common,ci

        Jira: TD-34178

        History:
            - 2026-04-13 Wei Pan Created to cover two related bugs:
              (a) EXPLAIN VERBOSE TRUE crashes with "unknown node = CaseWhen"
              (b) Server scans wrong time range when outer WHERE filters on a CASE WHEN alias

        """

        # ------------------------------------------------------------------
        # Setup
        # ------------------------------------------------------------------
        tdSql.execute("DROP DATABASE IF EXISTS test_case_when_explain")
        tdSql.execute("CREATE DATABASE test_case_when_explain PRECISION 'ms'")
        tdSql.execute("USE test_case_when_explain")
        tdSql.execute(
            "CREATE STABLE device_data "
            "(ts TIMESTAMP, val DOUBLE, zone_id NCHAR(32)) "
            "TAGS (device_sn NCHAR(64))"
        )
        tdSql.execute(
            "CREATE TABLE device_001 USING device_data TAGS ('DEV001')"
        )
        # Insert rows: one before, two inside, one after the test window
        tdSql.execute("INSERT INTO device_001 VALUES ('2020-12-31 23:59:59.000', 0.0, 'UTC')")
        tdSql.execute("INSERT INTO device_001 VALUES ('2021-01-01 06:00:00.000', 1.0, 'UTC')")
        tdSql.execute("INSERT INTO device_001 VALUES ('2021-01-01 18:00:00.000', 2.0, 'UTC')")
        tdSql.execute("INSERT INTO device_001 VALUES ('2021-01-02 00:00:01.000', 3.0, 'UTC')")

        # ------------------------------------------------------------------
        # The query under test – DO NOT change this SQL; it is the exact
        # pattern reported in TD-34178.
        # ------------------------------------------------------------------
        # Window: [2021-01-01 00:00:00.000, 2021-01-02 00:00:00.000)
        # Expected time range is derived from the server's timezone to avoid
        # hardcoding UTC offsets that differ across environments.
        inner_ts_start = "2021-01-01 00:00:00.000"
        inner_ts_end   = "2021-01-02 00:00:00.000"

        # Query the server to get the actual epoch-ms for these timestamp strings
        tdSql.query(
            f"SELECT to_unixtimestamp('{inner_ts_start}'), to_unixtimestamp('{inner_ts_end}') "
            f"FROM device_001 LIMIT 1"
        )
        expected_skey = tdSql.queryResult[0][0]
        expected_ekey = tdSql.queryResult[0][1] - 1  # exclusive → inclusive

        test_sql = f"""
            SELECT
                time_period,
                first(val)     AS first_val,
                last(val)      AS last_val,
                first(zone_id) AS zone_id
            FROM (
                SELECT
                    CASE
                        WHEN ts >= '{inner_ts_start}' AND ts < '{inner_ts_end}'
                            THEN 'PERIOD_20210101'
                        ELSE NULL
                    END AS time_period,
                    val,
                    zone_id,
                    ts
                FROM device_001
                WHERE ts >= '{inner_ts_start}'
                  AND ts < '{inner_ts_end}'
            ) t
            WHERE time_period IS NOT NULL
            GROUP BY time_period
            ORDER BY time_period
        """

        # ------------------------------------------------------------------
        # (a) Verify EXPLAIN VERBOSE TRUE does not raise an error
        # ------------------------------------------------------------------
        tdLog.info("step1: EXPLAIN VERBOSE TRUE must not crash on CASE WHEN subquery")
        tdSql.query(f"EXPLAIN VERBOSE TRUE {test_sql}")

        # ------------------------------------------------------------------
        # (b) Verify the scan time range matches the inner WHERE conditions
        # ------------------------------------------------------------------
        tdLog.info("step2: check scan Time Range in EXPLAIN output")
        time_range_line = f"Time Range: [{expected_skey}, {expected_ekey}]"
        found = False
        for row in tdSql.queryResult:
            # Each row is a one-column tuple containing the explain text
            if time_range_line in str(row[0]):
                found = True
                break
        if not found:
            tdLog.info(f"EXPLAIN output rows:")
            for row in tdSql.queryResult:
                tdLog.info(f"  {row[0]}")
        assert found, (
            f"Expected '{time_range_line}' in EXPLAIN VERBOSE TRUE output, "
            f"but it was not found. The server may be scanning the wrong time range."
        )

        # ------------------------------------------------------------------
        # (c) EXPLAIN ANALYZE VERBOSE TRUE must also not crash
        # ------------------------------------------------------------------
        tdLog.info("step3: EXPLAIN ANALYZE VERBOSE TRUE must not crash")
        tdSql.query(f"EXPLAIN ANALYZE VERBOSE TRUE {test_sql}")

        # ------------------------------------------------------------------
        # (d) Functional correctness: only the two rows inside the window
        #     should be returned.
        # ------------------------------------------------------------------
        tdLog.info("step4: query correctness – only rows inside window returned")
        tdSql.query(test_sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "PERIOD_20210101")
        tdSql.checkData(0, 1, 1.0)   # first(val)
        tdSql.checkData(0, 2, 2.0)   # last(val)