import json
import time

from new_test_framework.utils import tdSql


class TestIntervalSparsePerf:
    """Manual performance coverage for sparse interval windows; not registered in CI."""

    def _query_rows(self, sql):
        tdSql.query(sql)
        return list(tdSql.queryResult)

    def _measure(self, queries, expected):
        for sql in queries.values():
            for _ in range(8):
                self._query_rows(sql)

        result = {}
        for name, sql in queries.items():
            samples = []
            for _ in range(20):
                started = time.perf_counter_ns()
                assert self._query_rows(sql) == expected
                samples.append((time.perf_counter_ns() - started) / 1_000_000)
            samples.sort()
            result[name] = {
                "runs": len(samples),
                "min_ms": round(samples[0], 3),
                "median_ms": round(samples[len(samples) // 2], 3),
                "mean_ms": round(sum(samples) / len(samples), 3),
                "p95_ms": round(samples[int(len(samples) * 0.95) - 1], 3),
            }
        return result

    def test_sparse_interval_perf(self):
        """Interval: sparse interval performance comparison.

        Compare normal, FAST_INTERVAL, and EX_INTERVAL result sets for sparse
        interval windows before collecting manual performance samples.

        Since: v3.4.2.1

        Labels: interval,manual

        Jira: None

        History:
            - 2026-09-01 wpan Added test metadata
        """
        db = "interval_sparse_perf"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 2")
        tdSql.execute(f"create stable {db}.st(ts timestamp, f float, d double) tags(t int)")
        table_count = 64
        active_windows = 512
        start = 1704067200000
        window_step_ms = 195000
        for table in range(table_count):
            tdSql.execute(f"create table {db}.ct{table} using {db}.st tags({table})")
            for begin in range(0, active_windows, 128):
                values = " ".join(
                    f"({start + index * window_step_ms}, {table + index + 1}.0, {table + index + 10}.0)"
                    for index in range(begin, min(begin + 128, active_windows))
                )
                tdSql.execute(f"insert into {db}.ct{table} values {values}")

        query = (
            f"select _wstart, count(*), sum(f), avg(d) from {db}.st "
            "where ts >= '2024-01-01 00:00:00' and ts < '2024-01-02 03:46:38' interval(1s)"
        )
        queries = {
            "normal": query,
            "fast": query.replace("select", "select /*+ FAST_INTERVAL() */", 1),
            "ex": query.replace("select", "select /*+ EX_INTERVAL() */", 1),
        }
        expected = self._query_rows(queries["normal"])
        for sql in queries.values():
            assert self._query_rows(sql) == expected
        print("INTERVAL_SPARSE_PERF=" + json.dumps(self._measure(queries, expected), sort_keys=True))

        batch_query = query.replace("select _wstart, ", "select ", 1)
        batch_queries = {
            "normal": batch_query,
            "fast": batch_query.replace("select", "select /*+ FAST_INTERVAL() */", 1),
            "ex": batch_query.replace("select", "select /*+ EX_INTERVAL() */", 1),
        }
        batch_expected = self._query_rows(batch_queries["normal"])
        for sql in batch_queries.values():
            assert self._query_rows(sql) == batch_expected
        print("INTERVAL_SPARSE_BATCH_PERF=" + json.dumps(self._measure(batch_queries, batch_expected), sort_keys=True))
