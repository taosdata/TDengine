import os

from new_test_framework.utils import tdLog, tdSql


class TestJoinHashMemory:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join_hash_memory_safety(self):
        """Exercise Hash Join correctness with a CI-sized data set under sanitizers.

        The CI wrapper is responsible for ASAN/LSAN failure detection. This case
        keeps the workload deterministic and verifies both plan selection and
        exact results for unique, repeated, composite, and full-join keys.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: TD-29941

        History:
            - 2026-07-15 Added Hash Join memory profile test plan runner.
            - 2026-09-17 Converted to a CI memory-safety and correctness stress test.
        """

        rows = max(1, int(os.environ.get("HJ_MEM_ROWS", "100000")))
        duplicate_mod = max(1, int(os.environ.get("HJ_MEM_DUP_MOD", "1000")))

        tdSql.prepare(dbname="hj_mem", precision="ms")
        self.prepare_tables(rows, duplicate_mod)

        cases = [
            (
                "unique integer key inner join",
                "SELECT /*+ JOIN_ALGO(HASH) */ COUNT(*) "
                "FROM l_mem l JOIN r_mem r ON l.unique_k = r.unique_k",
                rows,
            ),
            (
                "repeated integer key inner join",
                "SELECT /*+ JOIN_ALGO(HASH) */ COUNT(*) "
                "FROM l_mem l JOIN r_mem r ON l.repeated_k = r.repeated_k",
                self.expected_repeated_count(rows, duplicate_mod),
            ),
            (
                "composite key inner join",
                "SELECT /*+ JOIN_ALGO(HASH) */ COUNT(*) FROM l_mem l JOIN r_mem r "
                "ON l.repeated_k = r.repeated_k AND l.k2 = r.k2",
                rows,
            ),
            (
                "repeated integer key full join",
                "SELECT /*+ JOIN_ALGO(HASH) */ COUNT(*) FROM l_mem l FULL JOIN r_mem r "
                "ON l.repeated_k = r.repeated_k",
                self.expected_repeated_count(rows, duplicate_mod),
            ),
        ]

        for name, sql, expected in cases:
            self.assert_hash_plan(sql)
            tdSql.query(sql)
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, expected)
            tdLog.info(f"{name}: verified {expected} rows")

    def prepare_tables(self, rows, duplicate_mod):
        tdSql.execute(
            "CREATE TABLE l_mem("
            "ts TIMESTAMP, unique_k INT, repeated_k INT, k2 BINARY(32), "
            "v DOUBLE, payload BINARY(256))"
        )
        tdSql.execute(
            "CREATE TABLE r_mem("
            "ts TIMESTAMP, unique_k INT, repeated_k INT, k2 BINARY(32), "
            "v DOUBLE, payload BINARY(256))"
        )
        self.insert_rows("l_mem", rows, duplicate_mod, 1704067200000, 1.0)
        self.insert_rows("r_mem", rows, duplicate_mod, 1704067200000, 2.0)

    def insert_rows(self, table, rows, duplicate_mod, base_ts, base_value, batch_size=500):
        for start in range(0, rows, batch_size):
            values = []
            end = min(start + batch_size, rows)
            for i in range(start, end):
                repeated_key = i % duplicate_mod
                key2 = f"k2_{i:08d}"
                payload = f"payload_{table}_{i:08d}"
                values.append(
                    f"({base_ts + i}, {i}, {repeated_key}, '{key2}', "
                    f"{base_value + i}, '{payload}')"
                )
            tdSql.execute(f"INSERT INTO {table} VALUES" + "".join(values))

    def assert_hash_plan(self, sql):
        tdSql.cursor.execute("EXPLAIN " + sql)
        rows = tdSql.cursor.fetchall()
        text = "\n".join("|".join(str(v) for v in row) for row in rows)
        if "Hash" not in text:
            tdLog.exit(f"query did not use Hash Join plan: {text}")

    def expected_repeated_count(self, rows, duplicate_mod):
        effective_mod = min(rows, duplicate_mod)
        base = rows // effective_mod
        remainder = rows % effective_mod
        return remainder * (base + 1) ** 2 + (effective_mod - remainder) * base**2
