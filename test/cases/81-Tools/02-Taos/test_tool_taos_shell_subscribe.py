# -*- coding: utf-8 -*-

import subprocess
import time

from new_test_framework.utils import tdLog, etool


class TestTaosShellSubscribe:
    """Integration tests for taos CLI subscribe command.

    All operations use taos -s to run against the system taosd directly,
    since the subscribe feature is purely client-side.
    """

    DB_NAME = "test_sub_db"
    STABLE_NAME = "meters"
    TOPIC_NAME = "test_sub_topic"

    @classmethod
    def _run_taos_cls(cls, sql, timeout=10):
        """Execute SQL via taos -s (class-level)."""
        taos_bin = etool.binFile("taos")
        cmd = f'{taos_bin} -s "{sql}"'
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=timeout
            )
            return (result.stdout + result.stderr).splitlines()
        except subprocess.TimeoutExpired:
            return []

    def _run_taos(self, sql, timeout=10):
        """Execute SQL via taos -s and return output lines."""
        return self._run_taos_cls(sql, timeout)

    def _subscribe(self, args, timeout=10):
        """Run subscribe command via taos -s and return output lines."""
        taos_bin = etool.binFile("taos")
        cmd = f'{taos_bin} -s "subscribe {args};"'
        tdLog.info(cmd)
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=timeout
            )
            output = result.stdout + result.stderr
        except subprocess.TimeoutExpired as e:
            stdout = e.stdout or b""
            stderr = e.stderr or b""
            if isinstance(stdout, bytes):
                stdout = stdout.decode("utf-8", errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", errors="replace")
            output = stdout + stderr
        return output.splitlines()

    def _exec_sql(self, sql):
        """Execute a SQL statement via taos -s (fire and forget)."""
        self._run_taos(sql)

    def _drop_group(self, group):
        """Drop a single consumer group."""
        self._run_taos(f"DROP CONSUMER GROUP FORCE `{group}` ON {self.TOPIC_NAME}")

    def _insert_rows(self, table="d0", count=5, start_ts=1700000000000):
        """Insert rows into specified child table (batched)."""
        values = ", ".join(
            f"({start_ts + i}, {10.0 + i}, {220 + i}, {0.5 + i * 0.1})"
            for i in range(count)
        )
        self._exec_sql(f"INSERT INTO {self.DB_NAME}.{table} VALUES {values}")

    def _create_topic(self, select_sql=None):
        """Create topic."""
        if select_sql is None:
            select_sql = f"SELECT ts, current, voltage, phase FROM {self.DB_NAME}.{self.STABLE_NAME}"
        self._exec_sql(f"CREATE TOPIC IF NOT EXISTS {self.TOPIC_NAME} AS {select_sql}")

    @classmethod
    def setup_class(cls):
        """One-time setup: create database, tables, and default topic."""
        ALL_GROUPS = [
            "grp_earliest", "grp_limit", "grp_offset", "grp_a", "grp_b",
            "grp_timeout", "grp_clientid", "grp_content", "grp_header",
            "grp_tbname", "grp_multi", "grp_unk", "grp1"
        ]
        for g in ALL_GROUPS:
            cls._run_taos_cls(f"DROP CONSUMER GROUP FORCE `{g}` ON {cls.TOPIC_NAME}")
        cls._run_taos_cls(f"DROP TOPIC IF EXISTS {cls.TOPIC_NAME}")
        cls._run_taos_cls(f"DROP DATABASE IF EXISTS {cls.DB_NAME}")
        cls._run_taos_cls(f"CREATE DATABASE {cls.DB_NAME} PRECISION 'ms'")
        cls._run_taos_cls(
            f"CREATE STABLE {cls.DB_NAME}.{cls.STABLE_NAME} "
            f"(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
            f"TAGS (groupid INT, location BINARY(24))"
        )
        cls._run_taos_cls(f"CREATE TABLE {cls.DB_NAME}.d0 USING {cls.DB_NAME}.{cls.STABLE_NAME} TAGS(1, 'Beijing')")
        cls._run_taos_cls(f"CREATE TABLE {cls.DB_NAME}.d1 USING {cls.DB_NAME}.{cls.STABLE_NAME} TAGS(2, 'Shanghai')")

    @classmethod
    def teardown_class(cls):
        """One-time teardown: drop everything."""
        ALL_GROUPS = [
            "grp_earliest", "grp_limit", "grp_offset", "grp_a", "grp_b",
            "grp_timeout", "grp_clientid", "grp_content", "grp_header",
            "grp_tbname", "grp_multi", "grp_unk", "grp1"
        ]
        for g in ALL_GROUPS:
            cls._run_taos_cls(f"DROP CONSUMER GROUP FORCE `{g}` ON {cls.TOPIC_NAME}")
        cls._run_taos_cls(f"DROP TOPIC IF EXISTS {cls.TOPIC_NAME}")
        cls._run_taos_cls(f"DROP DATABASE IF EXISTS {cls.DB_NAME}")

    def setup_method(self):
        """Per-test: drop topic and truncate tables for clean WAL state."""
        self._run_taos(f"DROP TOPIC IF EXISTS {self.TOPIC_NAME}")
        # Recreate tables to ensure clean vgroup state for TMQ
        self._run_taos(f"DROP TABLE IF EXISTS {self.DB_NAME}.d0")
        self._run_taos(f"DROP TABLE IF EXISTS {self.DB_NAME}.d1")
        self._run_taos(f"CREATE TABLE {self.DB_NAME}.d0 USING {self.DB_NAME}.{self.STABLE_NAME} TAGS(1, 'Beijing')")
        self._run_taos(f"CREATE TABLE {self.DB_NAME}.d1 USING {self.DB_NAME}.{self.STABLE_NAME} TAGS(2, 'Shanghai')")

    def _check_output(self, rlist, expected):
        """Check that expected string exists in output."""
        for line in rlist:
            if expected in line:
                tdLog.info(f'found "{expected}" in: {line}')
                return
        for i, line in enumerate(rlist):
            print(f"  {i}: {line}")
        tdLog.exit(f'not found "{expected}" in output')

    # -----------------------------------------------------------------------
    # Tests
    # -----------------------------------------------------------------------

    def test_subscribe_help(self):
        """subscribe -h shows usage help.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Initial subscribe help test

        """
        tdLog.debug(f"start to execute {__file__}")
        rlist = self._subscribe("-h")
        self._check_output(rlist, "Usage: subscribe <topic> -g <group_id> [options];")
        self._check_output(rlist, "-o <offset>")
        self._check_output(rlist, "Press Ctrl+C to stop")

    def test_subscribe_no_topic(self):
        """subscribe with no arguments shows usage help.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Initial test

        """
        tdLog.debug(f"start to execute {__file__}")
        rlist = self._subscribe("")
        self._check_output(rlist, "Usage: subscribe <topic> -g <group_id> [options];")

    def test_subscribe_no_group(self):
        """subscribe without -g shows error.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Initial test

        """
        tdLog.debug(f"start to execute {__file__}")
        self._create_topic()
        rlist = self._subscribe(f"{self.TOPIC_NAME}")
        self._check_output(rlist, "group_id is required")

    def test_subscribe_unknown_option(self):
        """subscribe with unknown option shows warning.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Initial test

        """
        tdLog.debug(f"start to execute {__file__}")
        self._insert_rows("d0", 1, 1700000000000)
        self._create_topic()
        time.sleep(0.5)
        rlist = self._subscribe(f"{self.TOPIC_NAME} -g grp_unk -z foo -o earliest -n 1")
        self._check_output(rlist, "Warning: unknown option")

    def test_subscribe_nonexist_topic(self):
        """subscribe to non-existent topic shows error.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Initial test

        """
        tdLog.debug(f"start to execute {__file__}")
        rlist = self._subscribe("no_such_topic_xyz -g grp1 -n 1")
        found = any("not exist" in line.lower() or "error" in line.lower() for line in rlist)
        if not found:
            for i, line in enumerate(rlist):
                print(f"  {i}: {line}")
            tdLog.exit("Expected error for non-existent topic")

    def test_subscribe_earliest(self):
        """subscribe with -o earliest receives pre-existing data.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Initial test

        """
        tdLog.debug(f"start to execute {__file__}")
        self._insert_rows("d0", 5, 1700000000000)
        self._create_topic()
        time.sleep(0.5)
        rlist = self._subscribe(f"{self.TOPIC_NAME} -g grp_earliest -o earliest -n 5")
        self._check_output(rlist, "Total rows received: 5")

    def test_subscribe_row_limit(self):
        """subscribe -n limits the number of rows received.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Initial test

        """
        tdLog.debug(f"start to execute {__file__}")
        self._insert_rows("d0", 20, 1700000000000)
        self._create_topic()
        time.sleep(0.5)
        rlist = self._subscribe(f"{self.TOPIC_NAME} -g grp_limit -o earliest -n 8")
        self._check_output(rlist, "Total rows received: 8")

    def test_subscribe_offset_persistence(self):
        """Same consumer group resumes from committed offset.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Initial test

        """
        tdLog.debug(f"start to execute {__file__}")
        self._insert_rows("d0", 10, 1700000000000)
        self._create_topic()
        time.sleep(0.5)

        # first consume: 5 rows
        rlist = self._subscribe(f"{self.TOPIC_NAME} -g grp_offset -o earliest -n 5")
        self._check_output(rlist, "Total rows received: 5")

        # wait for auto-commit to persist offset
        time.sleep(2)

        # insert new data
        self._insert_rows("d0", 3, 1700000100000)
        time.sleep(0.5)

        # second consume with same group: gets new + remaining data, not from beginning
        rlist = self._subscribe(f"{self.TOPIC_NAME} -g grp_offset -n 3")
        self._check_output(rlist, "Total rows received: 3")

    def test_subscribe_group_independence(self):
        """Different consumer groups maintain independent offsets.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Initial test

        """
        tdLog.debug(f"start to execute {__file__}")
        self._insert_rows("d0", 5, 1700000000000)
        self._create_topic()
        time.sleep(0.5)

        rlist = self._subscribe(f"{self.TOPIC_NAME} -g grp_a -o earliest -n 5")
        self._check_output(rlist, "Total rows received: 5")

        rlist = self._subscribe(f"{self.TOPIC_NAME} -g grp_b -o earliest -n 5")
        self._check_output(rlist, "Total rows received: 5")

    def test_subscribe_custom_timeout(self):
        """subscribe with -t custom timeout works normally.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Initial test

        """
        tdLog.debug(f"start to execute {__file__}")
        self._insert_rows("d0", 3, 1700000000000)
        self._create_topic()
        time.sleep(0.5)
        rlist = self._subscribe(f"{self.TOPIC_NAME} -g grp_timeout -o earliest -n 3 -t 500")
        self._check_output(rlist, "Total rows received: 3")

    def test_subscribe_data_content(self):
        """subscribe output contains actual column values.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Initial test

        """
        tdLog.debug(f"start to execute {__file__}")
        self._exec_sql(f"INSERT INTO {self.DB_NAME}.d0 VALUES(1700000000000, 12.5, 220, 0.31)")
        self._create_topic()
        time.sleep(0.5)
        rlist = self._subscribe(f"{self.TOPIC_NAME} -g grp_content -o earliest -n 1")
        self._check_output(rlist, "12.5")
        self._check_output(rlist, "220")

    def test_subscribe_header_display(self):
        """subscribe output includes column header.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Initial test

        """
        tdLog.debug(f"start to execute {__file__}")
        self._insert_rows("d0", 1, 1700000000000)
        self._create_topic()
        time.sleep(0.5)
        rlist = self._subscribe(f"{self.TOPIC_NAME} -g grp_header -o earliest -n 1")
        self._check_output(rlist, "ts")
        self._check_output(rlist, "current")
        self._check_output(rlist, "voltage")
        self._check_output(rlist, "phase")

    def test_subscribe_with_tbname(self):
        """subscribe topic that includes tbname column shows table names.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Initial test

        """
        tdLog.debug(f"start to execute {__file__}")
        self._insert_rows("d0", 2, 1700000000000)
        self._insert_rows("d1", 2, 1700000002000)
        self._exec_sql(
            f"CREATE TOPIC {self.TOPIC_NAME} AS "
            f"SELECT tbname, ts, current FROM {self.DB_NAME}.{self.STABLE_NAME}"
        )
        time.sleep(0.5)
        rlist = self._subscribe(f"{self.TOPIC_NAME} -g grp_tbname -o earliest -n 4")
        self._check_output(rlist, "tbname")
        found_d0 = any("d0" in line for line in rlist)
        found_d1 = any("d1" in line for line in rlist)
        if not found_d0:
            tdLog.exit("Expected 'd0' in subscribe output but not found")
        if not found_d1:
            tdLog.exit("Expected 'd1' in subscribe output but not found")

    def test_subscribe_multi_table(self):
        """subscribe receives data from multiple child tables.

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-25 Initial test

        """
        tdLog.debug(f"start to execute {__file__}")
        self._insert_rows("d0", 3, 1700000000000)
        self._insert_rows("d1", 3, 1700000001000)
        self._create_topic()
        time.sleep(0.5)
        rlist = self._subscribe(f"{self.TOPIC_NAME} -g grp_multi -o earliest -n 6")
        self._check_output(rlist, "Total rows received: 6")
